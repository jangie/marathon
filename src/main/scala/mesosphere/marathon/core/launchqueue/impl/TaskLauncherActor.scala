package mesosphere.marathon.core.launchqueue.impl

import akka.actor._
import akka.event.LoggingReceive
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.flow.OfferReviver
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.launcher.{ InstanceOp, InstanceOpFactory }
import mesosphere.marathon.core.launchqueue.LaunchQueue.QueuedInstanceInfo
import mesosphere.marathon.core.launchqueue.LaunchQueueConfig
import mesosphere.marathon.core.launchqueue.impl.TaskLauncherActor.RecheckIfBackOffUntilReached
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.{ InstanceOpWithSource, MatchedInstanceOps }
import mesosphere.marathon.core.matcher.base.util.InstanceOpSourceDelegate.InstanceOpNotification
import mesosphere.marathon.core.matcher.base.util.{ ActorOfferMatcher, InstanceOpSourceDelegate }
import mesosphere.marathon.core.matcher.manager.OfferMatcherManager
import mesosphere.marathon.core.task.bus.TaskChangeObservables.TaskChanged
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.core.task.TaskStateChange
import mesosphere.marathon.state.{ RunSpec, Timestamp }
import org.apache.mesos.{ Protos => Mesos }

import scala.collection.immutable
import scala.concurrent.duration._

private[launchqueue] object TaskLauncherActor {
  // scalastyle:off parameter.number
  def props(
    config: LaunchQueueConfig,
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    instanceTracker: InstanceTracker,
    rateLimiterActor: ActorRef)(
    runSpec: RunSpec,
    initialCount: Int): Props = {
    Props(new TaskLauncherActor(
      config,
      offerMatcherManager,
      clock, taskOpFactory,
      maybeOfferReviver,
      instanceTracker, rateLimiterActor,
      runSpec, initialCount))
  }
  // scalastyle:on parameter.number

  sealed trait Requests

  /**
    * Increase the task count of the receiver.
    * The actor responds with a [[QueuedInstanceInfo]] message.
    */
  case class AddTasks(spec: RunSpec, count: Int) extends Requests
  /**
    * Get the current count.
    * The actor responds with a [[QueuedInstanceInfo]] message.
    */
  case object GetCount extends Requests

  /**
    * Results in rechecking whether we may launch tasks.
    */
  private case object RecheckIfBackOffUntilReached extends Requests

  case object Stop extends Requests

  private val TASK_OP_REJECTED_TIMEOUT_REASON: String =
    "AppTaskLauncherActor: no accept received within timeout. " +
      "You can reconfigure the timeout with --task_operation_notification_timeout."
}

/**
  * Allows processing offers for starting tasks for the given app.
  */
// scalastyle:off parameter.number
// TODO (pods): rename everything related to tasks -> instances
private class TaskLauncherActor(
    config: LaunchQueueConfig,
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    taskTracker: InstanceTracker,
    rateLimiterActor: ActorRef,

    private[this] var runSpec: RunSpec,
    private[this] var instancesToLaunch: Int) extends Actor with ActorLogging with Stash {
  // scalastyle:on parameter.number

  private[this] var inFlightInstanceOperations = Map.empty[Instance.Id, Cancellable]

  private[this] var recheckBackOff: Option[Cancellable] = None
  private[this] var backOffUntil: Option[Timestamp] = None

  /** tasks that are in flight and those in the tracker */
  private[this] var instanceMap: Map[Instance.Id, Instance] = _

  /** Decorator to use this actor as a [[mesosphere.marathon.core.matcher.base.OfferMatcher#TaskOpSource]] */
  private[this] val myselfAsLaunchSource = InstanceOpSourceDelegate(self)

  override def preStart(): Unit = {
    super.preStart()

    // TODO (pods): need version for RunSpec
    log.info(
      "Started taskLaunchActor for {} version {} with initial count {}",
      runSpec.id, runSpec.version, instancesToLaunch)

    instanceMap = taskTracker.instancesBySpecSync.instancesMap(runSpec.id).instancekMap
    rateLimiterActor ! RateLimiterActor.GetDelay(runSpec)
  }

  override def postStop(): Unit = {
    OfferMatcherRegistration.unregister()
    recheckBackOff.foreach(_.cancel())

    if (inFlightInstanceOperations.nonEmpty) {
      log.warning("Actor shutdown but still some tasks in flight: {}", inFlightInstanceOperations.keys.mkString(", "))
      inFlightInstanceOperations.values.foreach(_.cancel())
    }

    super.postStop()

    log.info("Stopped appTaskLaunchActor for {} version {}", runSpec.id, runSpec.version)
  }

  override def receive: Receive = waitForInitialDelay

  private[this] def waitForInitialDelay: Receive = LoggingReceive.withLabel("waitingForInitialDelay") {
    case RateLimiterActor.DelayUpdate(spec, delayUntil) if spec == runSpec =>
      stash()
      unstashAll()
      context.become(active)
    case msg @ RateLimiterActor.DelayUpdate(spec, delayUntil) if spec != runSpec =>
      log.warning("Received delay update for other app: {}", msg)
    case message: Any => stash()
  }

  private[this] def active: Receive = LoggingReceive.withLabel("active") {
    Seq(
      receiveStop,
      receiveDelayUpdate,
      receiveTaskLaunchNotification,
      receiveTaskUpdate,
      receiveGetCurrentCount,
      receiveAddCount,
      receiveProcessOffers,
      receiveUnknown
    ).reduce(_.orElse[Any, Unit](_))
  }

  private[this] def stopping: Receive = LoggingReceive.withLabel("stopping") {
    Seq(
      receiveStop,
      receiveWaitingForInFlight,
      receiveUnknown
    ).reduce(_.orElse[Any, Unit](_))
  }

  private[this] def receiveWaitingForInFlight: Receive = {
    case notification: InstanceOpNotification =>
      receiveTaskLaunchNotification(notification)
      waitForInFlightIfNecessary()

    case TaskLauncherActor.Stop => // ignore, already stopping

    case "waitingForInFlight" => sender() ! "waitingForInFlight" // for testing
  }

  private[this] def receiveUnknown: Receive = {
    case msg: Any =>
      // fail fast and do not let the sender time out
      sender() ! Status.Failure(new IllegalStateException(s"Unhandled message: $msg"))
  }

  private[this] def receiveStop: Receive = {
    case TaskLauncherActor.Stop =>
      if (inFlightInstanceOperations.nonEmpty) {
        // try to stop gracefully but also schedule timeout
        import context.dispatcher
        log.info("schedule timeout for stopping in " + config.taskOpNotificationTimeout().milliseconds)
        context.system.scheduler.scheduleOnce(config.taskOpNotificationTimeout().milliseconds, self, PoisonPill)
      }
      waitForInFlightIfNecessary()
  }

  private[this] def waitForInFlightIfNecessary(): Unit = {
    if (inFlightInstanceOperations.isEmpty) {
      context.stop(self)
    } else {
      val taskIds = inFlightInstanceOperations.keys.take(3).mkString(", ")
      log.info(
        s"Stopping but still waiting for ${inFlightInstanceOperations.size} in-flight messages, " +
          s"first three task ids: $taskIds"
      )
      context.become(stopping)
    }
  }

  /**
    * Receive rate limiter updates.
    */
  private[this] def receiveDelayUpdate: Receive = {
    case RateLimiterActor.DelayUpdate(spec, delayUntil) if spec == runSpec =>

      if (backOffUntil != Some(delayUntil)) {

        backOffUntil = Some(delayUntil)

        recheckBackOff.foreach(_.cancel())
        recheckBackOff = None

        val now: Timestamp = clock.now()
        if (backOffUntil.exists(_ > now)) {
          import context.dispatcher
          recheckBackOff = Some(
            context.system.scheduler.scheduleOnce(now until delayUntil, self, RecheckIfBackOffUntilReached)
          )
        }

        OfferMatcherRegistration.manageOfferMatcherStatus()
      }

      log.debug("After delay update {}", status)

    case msg @ RateLimiterActor.DelayUpdate(spec, delayUntil) if spec != runSpec =>
      log.warning("Received delay update for other app: {}", msg)

    case RecheckIfBackOffUntilReached => OfferMatcherRegistration.manageOfferMatcherStatus()
  }

  private[this] def receiveTaskLaunchNotification: Receive = {
    case InstanceOpSourceDelegate.InstanceOpRejected(op, reason) if inFlight(op) =>
      removeTask(op.instanceId)
      log.info(
        "Task op '{}' for {} was REJECTED, reason '{}', rescheduling. {}",
        op.getClass.getSimpleName, op.instanceId, reason, status)

      op match {
        // only increment for launch ops, not for reservations:
        case _: InstanceOp.LaunchTask => instancesToLaunch += 1
        case _ => ()
      }

      OfferMatcherRegistration.manageOfferMatcherStatus()

    case InstanceOpSourceDelegate.InstanceOpRejected(op, TaskLauncherActor.TASK_OP_REJECTED_TIMEOUT_REASON) =>
      // This is a message that we scheduled in this actor.
      // When we receive a launch confirmation or rejection, we cancel this timer but
      // there is still a race and we might send ourselves the message nevertheless, so we just
      // ignore it here.
      log.debug("Ignoring task launch rejected for '{}' as the task is not in flight anymore", op.instanceId)

    case InstanceOpSourceDelegate.InstanceOpRejected(op, reason) =>
      log.warning("Unexpected task op '{}' rejected for {}.", op.getClass.getSimpleName, op.instanceId)

    case InstanceOpSourceDelegate.InstanceOpAccepted(op) =>
      inFlightInstanceOperations -= op.instanceId
      log.info("Task op '{}' for {} was accepted. {}", op.getClass.getSimpleName, op.instanceId, status)
  }

  private[this] def receiveTaskUpdate: Receive = {
    case TaskChanged(stateOp, stateChange) =>
      stateChange match {
        case TaskStateChange.Update(newState, _) =>
          log.info("receiveTaskUpdate: updating status of {}", newState.taskId)
          instanceMap += Instance.Id(newState.taskId) -> Instance(newState)

        case TaskStateChange.Expunge(task) =>
          log.info("receiveTaskUpdate: {} finished", task.taskId)
          removeTask(Instance.Id(task.taskId))
          // A) If the app has constraints, we need to reconsider offers that
          // we already rejected. E.g. when a host:unique constraint prevented
          // us to launch tasks on a particular node before, we need to reconsider offers
          // of that node after a task on that node has died.
          //
          // B) If a reservation timed out, already rejected offers might become eligible for creating new reservations.
          // TODO (pods): we don't want to handle something like isResident here
          if (runSpec.constraints.nonEmpty || (runSpec.residency.isDefined && shouldLaunchTasks)) {
            maybeOfferReviver.foreach(_.reviveOffers())
          }

        case _ =>
          log.info("receiveTaskUpdate: ignoring stateChange {}", stateChange)
      }
      replyWithQueuedInstanceCount()
  }

  private[this] def removeTask(taskId: Instance.Id): Unit = {
    inFlightInstanceOperations.get(taskId).foreach(_.cancel())
    inFlightInstanceOperations -= taskId
    instanceMap -= taskId
  }

  private[this] def receiveGetCurrentCount: Receive = {
    case TaskLauncherActor.GetCount =>
      replyWithQueuedInstanceCount()
  }

  private[this] def receiveAddCount: Receive = {
    case TaskLauncherActor.AddTasks(newRunSpec, addCount) =>
      // TODO (pods): add isUpgrade/needsRestart/isOnlyScaleChange to base trait?
      val configChange = runSpec.isUpgrade(newRunSpec)
      if (configChange || runSpec.needsRestart(newRunSpec) || runSpec.isOnlyScaleChange(newRunSpec)) {
        runSpec = newRunSpec
        instancesToLaunch = addCount

        if (configChange) {
          log.info(
            "getting new app definition config for '{}', version {} with {} initial tasks",
            runSpec.id, runSpec.version, addCount
          )

          suspendMatchingUntilWeGetBackoffDelayUpdate()

        } else {
          log.info(
            "scaling change for '{}', version {} with {} initial tasks",
            runSpec.id, runSpec.version, addCount
          )
        }
      } else {
        instancesToLaunch += addCount
      }

      OfferMatcherRegistration.manageOfferMatcherStatus()

      replyWithQueuedInstanceCount()
  }

  private[this] def suspendMatchingUntilWeGetBackoffDelayUpdate(): Unit = {
    // signal no interest in new offers until we get the back off delay.
    // this makes sure that we see unused offers again that we rejected for the old configuration.
    OfferMatcherRegistration.unregister()

    // get new back off delay, don't do anything until we get that.
    backOffUntil = None
    rateLimiterActor ! RateLimiterActor.GetDelay(runSpec)
    context.become(waitForInitialDelay)
  }

  private[this] def replyWithQueuedInstanceCount(): Unit = {
    val tasksLaunched = instanceMap.values.count(_.isLaunched)
    val taskLaunchesInFlight = inFlightInstanceOperations.keys
      .count(taskId => instanceMap.get(taskId).exists(_.isLaunched))
    sender() ! QueuedInstanceInfo(
      runSpec,
      inProgress = instancesToLaunch > 0 || inFlightInstanceOperations.nonEmpty,
      instancesLeftToLaunch = instancesToLaunch,
      finalInstanceCount = instancesToLaunch + taskLaunchesInFlight + tasksLaunched,
      unreachableInstances = instanceMap.values.count(value => value.isUnreachable),
      backOffUntil.getOrElse(clock.now())
    )
  }

  private[this] def receiveProcessOffers: Receive = {
    case ActorOfferMatcher.MatchOffer(deadline, offer) if clock.now() >= deadline || !shouldLaunchTasks =>
      val deadlineReached = clock.now() >= deadline
      log.debug("ignoring offer, offer deadline {}reached. {}", if (deadlineReached) "" else "NOT ", status)
      sender ! MatchedInstanceOps(offer.getId)

    case ActorOfferMatcher.MatchOffer(deadline, offer) =>
      val matchRequest = InstanceOpFactory.Request(runSpec, offer, instanceMap, instancesToLaunch)
      val taskOp: Option[InstanceOp] = taskOpFactory.buildTaskOp(matchRequest)
      taskOp match {
        case Some(op) => handleTaskOp(op, offer)
        case None => sender() ! MatchedInstanceOps(offer.getId)
      }
  }

  private[this] def handleTaskOp(taskOp: InstanceOp, offer: Mesos.Offer): Unit = {
    def updateActorState(): Unit = {
      val taskId = taskOp.instanceId
      taskOp match {
        // only decrement for launched tasks, not for reservations:
        case _: InstanceOp.LaunchTask => instancesToLaunch -= 1
        case _ => ()
      }

      // We will receive the updated task once it's been persisted. Before that,
      // we can only store the possible state, as we don't have the updated task
      // yet.
      taskOp.stateOp.possibleNewState.foreach { newState =>
        instanceMap += taskId -> newState
        scheduleTaskOpTimeout(taskOp)
      }

      OfferMatcherRegistration.manageOfferMatcherStatus()
    }

    log.info(
      "Request {} for task '{}', version '{}'. {}",
      taskOp.getClass.getSimpleName, taskOp.instanceId.idString, runSpec.version, status)

    updateActorState()
    sender() ! MatchedInstanceOps(offer.getId, immutable.Seq(InstanceOpWithSource(myselfAsLaunchSource, taskOp)))
  }

  private[this] def scheduleTaskOpTimeout(taskOp: InstanceOp): Unit = {
    val reject = InstanceOpSourceDelegate.InstanceOpRejected(
      taskOp, TaskLauncherActor.TASK_OP_REJECTED_TIMEOUT_REASON
    )
    val cancellable = scheduleTaskOperationTimeout(context, reject)
    inFlightInstanceOperations += taskOp.instanceId -> cancellable
  }

  private[this] def inFlight(task: InstanceOp): Boolean = inFlightInstanceOperations.contains(task.instanceId)

  protected def scheduleTaskOperationTimeout(
    context: ActorContext,
    message: InstanceOpSourceDelegate.InstanceOpRejected): Cancellable =
    {
      import context.dispatcher
      context.system.scheduler.scheduleOnce(config.taskOpNotificationTimeout().milliseconds, self, message)
    }

  private[this] def backoffActive: Boolean = backOffUntil.forall(_ > clock.now())
  private[this] def shouldLaunchTasks: Boolean = instancesToLaunch > 0 && !backoffActive

  private[this] def status: String = {
    val backoffStr = backOffUntil match {
      case Some(until) if until > clock.now() => s"currently waiting for backoff($until)"
      case _ => "not backing off"
    }

    val inFlight = inFlightInstanceOperations.size
    val tasksLaunchedOrRunning = instanceMap.values.count(_.isLaunched) - inFlight
    val instanceCountDelta = instanceMap.size + instancesToLaunch - runSpec.instances
    val matchInstanceStr = if (instanceCountDelta == 0) "" else s"instance count delta $instanceCountDelta."
    s"$instancesToLaunch instancesToLaunch, $inFlight in flight, " +
      s"$tasksLaunchedOrRunning confirmed. $matchInstanceStr $backoffStr"
  }

  /** Manage registering this actor as offer matcher. Only register it if instancesToLaunch > 0. */
  private[this] object OfferMatcherRegistration {
    private[this] val myselfAsOfferMatcher: OfferMatcher = {
      //set the precedence only, if this app is resident
      // TODO (pods): how shall we handle residency?
      new ActorOfferMatcher(clock, self, runSpec.residency.map(_ => runSpec.id))
    }
    private[this] var registeredAsMatcher = false

    /** Register/unregister as necessary */
    def manageOfferMatcherStatus(): Unit = {
      val shouldBeRegistered = shouldLaunchTasks

      if (shouldBeRegistered && !registeredAsMatcher) {
        log.debug("Registering for {}, {}.", runSpec.id, runSpec.version)
        offerMatcherManager.addSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = true
      } else if (!shouldBeRegistered && registeredAsMatcher) {
        if (instancesToLaunch > 0) {
          log.info("Backing off due to task failures. Stop receiving offers for {}, {}", runSpec.id, runSpec.version)
        } else {
          log.info("No tasks left to launch. Stop receiving offers for {}, {}", runSpec.id, runSpec.version)
        }
        offerMatcherManager.removeSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = false
      }
    }

    def unregister(): Unit = {
      if (registeredAsMatcher) {
        log.info("Deregister as matcher.")
        offerMatcherManager.removeSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = false
      }
    }
  }
}
