package mesosphere.marathon.core.instance

import mesosphere.marathon.core.instance.Instance.InstanceState
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.{ PathId, Timestamp }
import org.apache._

case class Instance(instanceId: Instance.Id, agentInfo: Instance.AgentInfo, state: InstanceState, tasks: Seq[Task]) {

  def runSpecId: PathId = instanceId.runSpecId

  def isLaunched: Boolean = tasks.forall(task => task.launched.isDefined)
}

object Instance {

  def instancesById(tasks: Iterable[Instance]): Map[Instance.Id, Instance] =
    tasks.iterator.map(task => task.instanceId -> task).toMap

  // TODO ju FIXME
  def apply(task: Task): Instance = new Instance(Id(task.taskId), task.agentInfo,
    InstanceState(task.status.taskStatus, task.status.startedAt.getOrElse(task.status.stagedAt)), Seq(task))

  case class InstanceState(status: InstanceStatus, since: Timestamp)

  case class Id(idString: String) extends Ordered[Id] {
    lazy val runSpecId: PathId = Id.runSpecId(idString)

    override def toString: String = s"instance [$idString]"

    override def compare(that: Instance.Id): Int =
      if (this.getClass == that.getClass)
        idString.compare(that.idString)
      else this.compareTo(that)
  }

  object Id {
    private val InstanceIdRegex = """^(.+)[\._]([^_\.]+)$""".r

    def apply(executorId: mesos.Protos.ExecutorID): Id = new Id(executorId.getValue)

    def apply(taskId: Task.Id): Id = new Id(taskId.idString) // TODO ju FIXME

    def runSpecId(taskId: String): PathId = { // TODO ju really ???
      taskId match {
        case InstanceIdRegex(runSpecId, uuid) => PathId.fromSafePath(runSpecId)
      }
    }
  }

  /**
    * Info relating to the host on which the Instance has been launched.
    */
  case class AgentInfo(
    host: String,
    agentId: Option[String],
    attributes: Iterable[mesos.Protos.Attribute])

  implicit class InstanceStatusComparison(val instance: Instance) extends AnyVal {
    def isReserved: Boolean = instance.state.status == InstanceStatus.Reserved
    def isCreated: Boolean = instance.state.status == InstanceStatus.Created
    def isError: Boolean = instance.state.status == InstanceStatus.Error
    def isFailed: Boolean = instance.state.status == InstanceStatus.Failed
    def isFinished: Boolean = instance.state.status == InstanceStatus.Finished
    def isKilled: Boolean = instance.state.status == InstanceStatus.Killed
    def isKilling: Boolean = instance.state.status == InstanceStatus.Killing
    def isRunning: Boolean = instance.state.status == InstanceStatus.Running
    def isStaging: Boolean = instance.state.status == InstanceStatus.Staging
    def isStarting: Boolean = instance.state.status == InstanceStatus.Starting
    def isUnreachable: Boolean = instance.state.status == InstanceStatus.Unreachable
    def isGone: Boolean = instance.state.status == InstanceStatus.Gone
    def isUnknown: Boolean = instance.state.status == InstanceStatus.Unknown
    def isDropped: Boolean = instance.state.status == InstanceStatus.Dropped
  }

}
