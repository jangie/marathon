package mesosphere.marathon.core.task.tracker

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.state.PathId
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

/**
  * The TaskTracker exposes the latest known state for every task.
  *
  * It is an read-only interface. For modification, see
  * * [[TaskStateOpProcessor]] for create, update, delete operations
  *
  * FIXME: To allow introducing the new asynchronous [[InstanceTracker]] without needing to
  * refactor a lot of code at once, synchronous methods are still available but should be
  * avoided in new code.
  */
trait InstanceTracker {

  def specInstancesLaunchedSync(pathId: PathId): Iterable[Instance]
  def specInstancesSync(pathId: PathId): Iterable[Instance]
  def specInstances(pathId: PathId)(implicit ec: ExecutionContext): Future[Iterable[Instance]]

  def instance(instanceId: Instance.Id): Future[Option[Instance]]

  def instancesBySpecSync: InstanceTracker.InstancesBySpec
  def instancesBySpec()(implicit ec: ExecutionContext): Future[InstanceTracker.InstancesBySpec]

  def countLaunchedSpecInstancesSync(appId: PathId): Int
  def countLaunchedSpecInstancesSync(appId: PathId, filter: Instance => Boolean): Int
  def countSpecInstancesSync(appId: PathId): Int
  def countSpecInstancesSync(appId: PathId, filter: Instance => Boolean): Int
  def countSpecInstances(appId: PathId)(implicit ec: ExecutionContext): Future[Int]

  def hasSpecInstancesSync(appId: PathId): Boolean
  def hasSpecInstances(appId: PathId)(implicit ec: ExecutionContext): Future[Boolean]
}

object InstanceTracker {
  /**
    * Contains all tasks grouped by app ID.
    */
  case class InstancesBySpec private (instancesMap: Map[PathId, InstanceTracker.SpecInstances]) {
    import InstancesBySpec._

    def allSpecIdsWithInstances: Set[PathId] = instancesMap.keySet

    def hasSpecInstances(appId: PathId): Boolean = instancesMap.contains(appId)

    def specInstances(pathId: PathId): Iterable[Instance] = {
      instancesMap.get(pathId).map(_.instances).getOrElse(Iterable.empty)
    }

    def instance(instanceId: Instance.Id): Option[Instance] = for {
      app <- instancesMap.get(instanceId.runSpecId)
      task <- app.instancekMap.get(instanceId)
    } yield task

    def allInstances: Iterable[Instance] = instancesMap.values.view.flatMap(_.instances)

    private[tracker] def updateApp(appId: PathId)(
      update: InstanceTracker.SpecInstances => InstanceTracker.SpecInstances): InstancesBySpec = {
      val updated = update(instancesMap(appId))
      if (updated.isEmpty) {
        log.info(s"Removed app [$appId] from tracker")
        copy(instancesMap = instancesMap - appId)
      } else {
        log.debug(s"Updated app [$appId], currently ${updated.instancekMap.size} tasks in total.")
        copy(instancesMap = instancesMap + (appId -> updated))
      }
    }
  }

  object InstancesBySpec {
    private val log = LoggerFactory.getLogger(getClass)

    def of(appTasks: collection.immutable.Map[PathId, InstanceTracker.SpecInstances]): InstancesBySpec = {
      new InstancesBySpec(appTasks.withDefault(appId => InstanceTracker.SpecInstances(appId)))
    }

    def of(apps: InstanceTracker.SpecInstances*): InstancesBySpec = of(Map(apps.map(app => app.specId -> app): _*))

    def forInstances(tasks: Instance*): InstancesBySpec = of(
      tasks.groupBy(_.runSpecId).map { case (appId, appTasks) => appId -> SpecInstances.forInstances(appId, appTasks) }
    )

    def empty: InstancesBySpec = of(collection.immutable.Map.empty[PathId, InstanceTracker.SpecInstances])
  }
  /**
    * Contains only the tasks of the app with the given app ID.
    *
    * @param specId   The id of the app.
    * @param instancekMap The tasks of this app by task ID. FIXME: change keys to Task.TaskID
    */
  case class SpecInstances(specId: PathId, instancekMap: Map[Instance.Id, Instance] = Map.empty) {

    def isEmpty: Boolean = instancekMap.isEmpty
    def contains(taskId: Instance.Id): Boolean = instancekMap.contains(taskId)
    def instances: Iterable[Instance] = instancekMap.values

    private[tracker] def withInstance(instance: Instance): SpecInstances =
      copy(instancekMap = instancekMap + (instance.instanceId -> instance))

    private[tracker] def withoutInstance(instanceId: Instance.Id): SpecInstances =
      copy(instancekMap = instancekMap - instanceId)
  }

  object SpecInstances {
    def forInstances(pathId: PathId, instances: Iterable[Instance]): SpecInstances =
      SpecInstances(pathId, instances.map(instance => instance.instanceId -> instance).toMap)
  }
}
