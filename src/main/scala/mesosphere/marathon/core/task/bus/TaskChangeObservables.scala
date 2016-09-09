package mesosphere.marathon.core.task.bus

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.bus.TaskChangeObservables.TaskChanged
import mesosphere.marathon.core.task.{ TaskStateChange, InstanceStateOp }
import mesosphere.marathon.state.PathId
import rx.lang.scala.Observable

object TaskChangeObservables {

  // TODO(PODS): remove class, replaced by InstanceChange
  case class TaskChanged(stateOp: InstanceStateOp, stateChange: TaskStateChange) {
    def instanceId: Instance.Id = stateOp.instanceId
    def runSpecId: PathId = stateOp.instanceId.runSpecId
  }
}

/**
  * Allows subscription to TaskChanged updates.
  */
trait TaskChangeObservables {
  def forAll: Observable[TaskChanged]
  def forRunSpecId(appId: PathId): Observable[TaskChanged]
}

