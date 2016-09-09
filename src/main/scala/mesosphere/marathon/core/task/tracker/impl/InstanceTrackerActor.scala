package mesosphere.marathon.core.task.tracker.impl

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import akka.event.LoggingReceive
import com.twitter.util.NonFatal
import mesosphere.marathon.core.appinfo.TaskCounts
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.bus.TaskChangeObservables.TaskChanged
import mesosphere.marathon.core.task.tracker.impl.InstanceTrackerActor.ForwardTaskOp
import mesosphere.marathon.core.task.{ InstanceStateOp, TaskStateChange }
import mesosphere.marathon.core.task.tracker.{ InstanceTracker, InstanceTrackerUpdateStepProcessor }
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.metrics.Metrics.AtomicIntGauge
import mesosphere.marathon.state.{ PathId, Timestamp }
import org.slf4j.LoggerFactory

object InstanceTrackerActor {
  def props(
    metrics: ActorMetrics,
    taskLoader: InstancesLoader,
    updateStepProcessor: InstanceTrackerUpdateStepProcessor,
    taskUpdaterProps: ActorRef => Props): Props = {
    Props(new InstanceTrackerActor(metrics, taskLoader, updateStepProcessor, taskUpdaterProps))
  }

  /** Query the current [[InstanceTracker.SpecInstances]] from the [[InstanceTrackerActor]]. */
  private[impl] case object List

  private[impl] case class Get(taskId: Instance.Id)

  /** Forward an update operation to the child [[InstanceUpdateActor]]. */
  private[impl] case class ForwardTaskOp(deadline: Timestamp, taskId: Instance.Id, taskStateOp: InstanceStateOp)

  /** Describes where and what to send after an update event has been processed by the [[InstanceTrackerActor]]. */
  private[impl] case class Ack(initiator: ActorRef, stateChange: TaskStateChange) {
    def sendAck(): Unit = {
      val msg = stateChange match {
        case TaskStateChange.Failure(cause) => Status.Failure(cause)
        case _ => stateChange
      }
      initiator ! msg
    }
  }

  /** Inform the [[InstanceTrackerActor]] of a task state change (after persistence). */
  private[impl] case class StateChanged(taskChanged: TaskChanged, ack: Ack)

  private[tracker] class ActorMetrics(metrics: Metrics) {
    val stagedCount = metrics.gauge("service.mesosphere.marathon.task.staged.count", new AtomicIntGauge)
    val runningCount = metrics.gauge("service.mesosphere.marathon.task.running.count", new AtomicIntGauge)

    def resetMetrics(): Unit = {
      stagedCount.setValue(0)
      runningCount.setValue(0)
    }
  }
}

/**
  * Holds the current in-memory version of all task state. It gets informed of task state changes
  * after they have been persisted.
  *
  * It also spawns the [[InstanceUpdateActor]] as a child and forwards update operations to it.
  */
private class InstanceTrackerActor(
    metrics: InstanceTrackerActor.ActorMetrics,
    taskLoader: InstancesLoader,
    updateStepProcessor: InstanceTrackerUpdateStepProcessor,
    taskUpdaterProps: ActorRef => Props) extends Actor with Stash {

  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] val updaterRef = context.actorOf(taskUpdaterProps(self), "updater")

  override val supervisorStrategy = OneForOneStrategy() { case _: Exception => Escalate }

  override def preStart(): Unit = {
    super.preStart()

    log.info(s"${getClass.getSimpleName} is starting. Task loading initiated.")
    metrics.resetMetrics()

    import akka.pattern.pipe
    import context.dispatcher
    taskLoader.loadTasks().pipeTo(self)
  }

  override def postStop(): Unit = {
    metrics.resetMetrics()

    super.postStop()
  }

  override def receive: Receive = initializing

  private[this] def initializing: Receive = LoggingReceive.withLabel("initializing") {
    case appTasks: InstanceTracker.InstancesBySpec =>
      log.info("Task loading complete.")

      unstashAll()
      context.become(withTasks(
        appTasks,
        TaskCounts(appTasks.allInstances.flatMap(_.tasks), healthStatuses = Map.empty)))

    case Status.Failure(cause) =>
      // escalate this failure
      throw new IllegalStateException("while loading tasks", cause)

    case stashMe: AnyRef =>
      stash()
  }

  private[this] def withTasks(appTasks: InstanceTracker.InstancesBySpec, counts: TaskCounts): Receive = {

    def becomeWithUpdatedApp(appId: PathId)(instanceId: Instance.Id, newInstance: Option[Instance]): Unit = {
      val updatedAppTasks = newInstance match {
        case None => appTasks.updateApp(appId)(_.withoutInstance(instanceId))
        case Some(instance) => appTasks.updateApp(appId)(_.withInstance(instance))
      }

      val updatedCounts = {
        val oldTask = appTasks.instance(instanceId)
        // we do ignore health counts
        val oldTaskCount = TaskCounts(oldTask.map(_.tasks).getOrElse(Seq()), healthStatuses = Map.empty)
        val newTaskCount = TaskCounts(newInstance.map(_.tasks).getOrElse(Seq()), healthStatuses = Map.empty)
        counts + newTaskCount - oldTaskCount
      }

      context.become(withTasks(updatedAppTasks, updatedCounts))
    }

    // this is run on any state change
    metrics.stagedCount.setValue(counts.tasksStaged)
    metrics.runningCount.setValue(counts.tasksRunning)

    LoggingReceive.withLabel("withTasks") {
      case InstanceTrackerActor.List =>
        sender() ! appTasks

      case InstanceTrackerActor.Get(taskId) =>
        sender() ! appTasks.instance(taskId)

      case ForwardTaskOp(deadline, taskId, taskStateOp) =>
        val op = InstanceOpProcessor.Operation(deadline, sender(), taskId, taskStateOp)
        updaterRef.forward(InstanceUpdateActor.ProcessInstanceOp(op))

      case msg @ InstanceTrackerActor.StateChanged(change, ack) =>
        change.stateChange match {
          case TaskStateChange.Update(task, _) =>
            becomeWithUpdatedApp(task.runSpecId)(Instance.Id(task.taskId), newInstance = Some(Instance(task)))

          case TaskStateChange.Expunge(task) =>
            becomeWithUpdatedApp(task.runSpecId)(Instance.Id(task.taskId), newInstance = None)

          case _: TaskStateChange.NoChange |
            _: TaskStateChange.Failure =>
          // ignore, no state change
        }

        val originalSender = sender()

        import context.dispatcher
        updateStepProcessor.process(change).recover {
          case NonFatal(cause) =>
            // since we currently only use ContinueOnErrorSteps, we can simply ignore failures here
            //
            log.warn("updateStepProcessor.process failed: {}", cause)
        }.foreach { _ =>
          ack.sendAck()
          originalSender ! (())
        }
    }
  }
}
