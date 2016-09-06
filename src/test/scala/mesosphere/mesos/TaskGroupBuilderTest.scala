package mesosphere.mesos

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.raml
import mesosphere.marathon.state.EnvVarString
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.{ MarathonSpec, MarathonTestHelper }
import org.apache.mesos.{ Protos => mesos }
import org.scalatest.Matchers

import scala.collection.JavaConversions._

class TaskGroupBuilderTest extends MarathonSpec with Matchers {
  test("Build with single container") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 1.1, mem = 160.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo",
            exec = None,
            resources = raml.Resources(cpus = 1.0f, mem = 128.0f)
          )
        )
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig(
        mesosRole = None,
        acceptedResourceRoles = None,
        envVarsPrefix = None)
    )

    assert(pod.isDefined)
  }

  test("Build with multiple containers") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 4.1, mem = 1056.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo",
            resources = raml.Resources(cpus = 1.0f, mem = 512.0f)
          ),
          raml.MesosContainer(
            name = "Foo2",
            resources = raml.Resources(cpus = 2.0f, mem = 256.0f)
          ),
          raml.MesosContainer(
            name = "Foo3",
            resources = raml.Resources(cpus = 1.0f, mem = 256.0f)
          )
        )
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig()
    )

    assert(pod.isDefined)

    val (_, taskGroupInfo, _) = pod.get

    assert(taskGroupInfo.getTasksCount == 3)
  }

  test("Commands are set correctly") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 4.1, mem = 1056.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo1",
            exec = Some(raml.MesosExec(raml.ShellCommand("foo"))),
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f)
          ),
          raml.MesosContainer(
            name = "Foo2",
            exec = Some(raml.MesosExec(raml.ArgvCommand(List("foo", "arg1", "arg2")))),
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f)
          )
        )
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig()
    )

    assert(pod.isDefined)

    val (_, taskGroupInfo, _) = pod.get

    assert(taskGroupInfo.getTasksCount == 2)

    val command1 = taskGroupInfo.getTasksList.find(_.getName == "Foo1").get.getCommand

    assert(command1.getShell)
    assert(command1.getValue == "foo")
    assert(command1.getArgumentsCount == 0)

    val command2 = taskGroupInfo.getTasksList.find(_.getName == "Foo2").get.getCommand

    assert(!command2.getShell)
    assert(command2.getValue == "foo")
    assert(command2.getArgumentsCount == 2)
    assert(command2.getArguments(0) == "arg1")
    assert(command2.getArguments(1) == "arg2")
  }

  test("Container user overrides pod user") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 4.1, mem = 1056.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo1",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f)
          ),
          raml.MesosContainer(
            name = "Foo2",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            user = Some("admin")
          )
        ),
        user = Some("user")
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig()
    )

    assert(pod.isDefined)

    val (_, taskGroupInfo, _) = pod.get

    assert(taskGroupInfo.getTasksCount == 2)

    assert(taskGroupInfo.getTasksList.find(_.getName == "Foo1").get.getCommand.getUser == "user")
    assert(taskGroupInfo.getTasksList.find(_.getName == "Foo2").get.getCommand.getUser == "admin")
  }

  test("Labels") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 4.1, mem = 1056.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo1",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            labels = Some(raml.KVLabels(Map("b" -> "c")))
          ),
          raml.MesosContainer(
            name = "Foo2",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            labels = Some(raml.KVLabels(Map("c" -> "c")))
          )
        ),
        labels = Map("a" -> "a", "b" -> "b")
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig()
    )

    assert(pod.isDefined)

    val (executorInfo, taskGroupInfo, _) = pod.get

    assert(executorInfo.hasLabels)

    val executorLabels = executorInfo.getLabels.getLabelsList.map { label =>
      label.getKey -> label.getValue
    }.toMap

    assert(executorLabels("a") == "a")
    assert(executorLabels("b") == "b")

    assert(taskGroupInfo.getTasksCount == 2)

    val task1labels = taskGroupInfo
      .getTasksList.find(_.getName == "Foo1").get
      .getLabels.getLabelsList
      .map(label => label.getKey -> label.getValue).toMap

    assert(task1labels("a") == "a")
    assert(task1labels("b") == "c")

    val task2labels = taskGroupInfo
      .getTasksList.find(_.getName == "Foo2").get
      .getLabels.getLabelsList
      .map(label => label.getKey -> label.getValue).toMap

    assert(task2labels("a") == "a")
    assert(task2labels("b") == "b")
    assert(task2labels("c") == "c")
  }

  test("Env") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 4.1, mem = 1056.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo1",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            environment = Some(raml.EnvVars(Map("b" -> raml.EnvVarValue("c"))))
          ),
          raml.MesosContainer(
            name = "Foo2",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            environment = Some(raml.EnvVars(Map("c" -> raml.EnvVarValue("c")))),
            labels = Some(raml.KVLabels(Map("b" -> "b")))
          )
        ),
        env = Map("a" -> EnvVarString("a"), "b" -> EnvVarString("b")),
        labels = Map("a" -> "a")
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig()
    )

    assert(pod.isDefined)

    val (_, taskGroupInfo, _) = pod.get

    assert(taskGroupInfo.getTasksCount == 2)

    val task1EnvVars = taskGroupInfo
      .getTasksList.find(_.getName == "Foo1").get
      .getCommand
      .getEnvironment
      .getVariablesList
      .map(envVar => envVar.getName -> envVar.getValue).toMap

    assert(task1EnvVars("a") == "a")
    assert(task1EnvVars("b") == "c")
    assert(task1EnvVars("MARATHON_APP_ID") == "Foo1")
    assert(task1EnvVars("MARATHON_APP_LABELS") == "A")
    assert(task1EnvVars("MARATHON_APP_LABEL_A") == "a")

    val task2EnvVars = taskGroupInfo
      .getTasksList.find(_.getName == "Foo2").get
      .getCommand
      .getEnvironment
      .getVariablesList
      .map(envVar => envVar.getName -> envVar.getValue).toMap

    assert(task2EnvVars("a") == "a")
    assert(task2EnvVars("b") == "b")
    assert(task2EnvVars("c") == "c")
    assert(task2EnvVars("MARATHON_APP_ID") == "Foo2")
    assert(task2EnvVars("MARATHON_APP_LABELS") == "A B")
    assert(task2EnvVars("MARATHON_APP_LABEL_A") == "a")
    assert(task2EnvVars("MARATHON_APP_LABEL_B") == "b")
  }

  test("Volumes") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 4.1, mem = 1056.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo1",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            volumeMounts = List(
              raml.VolumeMount(
                name = "volume1",
                mountPath = "/mnt/path1"
              ),
              raml.VolumeMount(
                name = "volume2",
                mountPath = "/mnt/path2",
                readOnly = Some(true)
              )
            )
          ),
          raml.MesosContainer(
            name = "Foo2",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            volumeMounts = List(
              raml.VolumeMount(
                name = "volume1",
                mountPath = "/mnt/path2",
                readOnly = Some(false)
              )
            )
          )
        ),
        podVolumes = List(
          raml.Volume(
            name = "volume1",
            host = Some("/mnt/path1")
          ),
          raml.Volume(
            name = "volume2",
            host = None
          )
        )
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig()
    )

    assert(pod.isDefined)

    val (_, taskGroupInfo, _) = pod.get

    assert(taskGroupInfo.getTasksCount == 2)

    val task1Volumes = taskGroupInfo
      .getTasksList.find(_.getName == "Foo1").get
      .getContainer.getVolumesList

    assert(task1Volumes.size == 2)
    assert(task1Volumes.find(_.getContainerPath == "/mnt/path1").get.getHostPath == "/mnt/path1")
    assert(task1Volumes.find(_.getContainerPath == "/mnt/path1").get.getMode == mesos.Volume.Mode.RW)
    assert(task1Volumes.find(_.getContainerPath == "/mnt/path2").get.getMode == mesos.Volume.Mode.RO)

    val task2Volumes = taskGroupInfo
      .getTasksList.find(_.getName == "Foo2").get
      .getContainer.getVolumesList

    assert(task2Volumes.size == 1)
    assert(task2Volumes.find(_.getContainerPath == "/mnt/path2").get.getHostPath == "/mnt/path1")
    assert(task2Volumes.find(_.getContainerPath == "/mnt/path2").get.getMode == mesos.Volume.Mode.RW)
  }

  test("Container images") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 4.1, mem = 1056.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo1",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            image = Some(raml.Image(
              kind = raml.ImageType.Docker,
              id = "alpine",
              forcePull = Some(true)
            ))
          ),
          raml.MesosContainer(
            name = "Foo2",
            resources = raml.Resources(cpus = 2.0f, mem = 512.0f),
            image = Some(raml.Image(
              kind = raml.ImageType.Appc,
              id = "alpine"
            ))
          )
        )
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig()
    )

    assert(pod.isDefined)

    val (_, taskGroupInfo, _) = pod.get

    assert(taskGroupInfo.getTasksCount == 2)

    val task1Container = taskGroupInfo
      .getTasksList.find(_.getName == "Foo1").get.getContainer

    assert(task1Container.getType == mesos.ContainerInfo.Type.MESOS)
    assert(task1Container.getMesos.getImage.getType == mesos.Image.Type.DOCKER)
    assert(task1Container.getMesos.getImage.getDocker.getName == "alpine")
    assert(!task1Container.getMesos.getImage.getCached)

    val task2Container = taskGroupInfo
      .getTasksList.find(_.getName == "Foo2").get.getContainer

    assert(task2Container.getType == mesos.ContainerInfo.Type.MESOS)
    assert(task2Container.getMesos.getImage.getType == mesos.Image.Type.APPC)
    assert(task2Container.getMesos.getImage.getAppc.getName == "alpine")
  }

  test("Health check") {
    val offer = MarathonTestHelper.makeBasicOffer(cpus = 3.1, mem = 416.0).build

    val pod = TaskGroupBuilder.build(
      PodDefinition(
        id = "/product/frontend".toPath,
        containers = List(
          raml.MesosContainer(
            name = "Foo1",
            resources = raml.Resources(cpus = 1.0f, mem = 128.0f),
            healthCheck = Some(raml.HealthCheck(
              http = Some(raml.HttpHealthCheck("foo"))
            ))
          ),
          raml.MesosContainer(
            name = "Foo2",
            resources = raml.Resources(cpus = 1.0f, mem = 128.0f),
            healthCheck = Some(raml.HealthCheck(
              tcp = Some(raml.TcpHealthCheck("foo"))
            )),
            endpoints = List(
              raml.Endpoint(
                name = "health_check",
                containerPort = Some(1234)
              )
            )
          ),
          raml.MesosContainer(
            name = "Foo3",
            resources = raml.Resources(cpus = 1.0f, mem = 128.0f),
            healthCheck = Some(raml.HealthCheck(
              command = Some(raml.CommandHealthCheck(raml.ShellCommand("foo")))
            ))
          )
        )
      ),
      offer,
      Iterable.empty,
      s => Instance.Id(s.toString),
      MarathonTestHelper.defaultConfig()
    )

    assert(pod.isDefined)

    val (_, taskGroupInfo, _) = pod.get

    assert(taskGroupInfo.getTasksCount == 3)

    val task1HealthCheck = taskGroupInfo
      .getTasksList.find(_.getName == "Foo1").get
      .getHealthCheck

    assert(task1HealthCheck.getType == mesos.HealthCheck.Type.HTTP)
    assert(task1HealthCheck.getHttp.getPath == "foo")

    val task2HealthCheck = taskGroupInfo
      .getTasksList.find(_.getName == "Foo2").get
      .getHealthCheck

    assert(task2HealthCheck.getType == mesos.HealthCheck.Type.TCP)
    assert(task2HealthCheck.getTcp.getPort == 1234)

    val task3HealthCheck = taskGroupInfo
      .getTasksList.find(_.getName == "Foo3").get
      .getHealthCheck

    assert(task3HealthCheck.getType == mesos.HealthCheck.Type.COMMAND)
    assert(task3HealthCheck.getCommand.getShell)
    assert(task3HealthCheck.getCommand.getValue == "foo")
  }

  test("URL Artifacts") {
    // TODO
  }
}
