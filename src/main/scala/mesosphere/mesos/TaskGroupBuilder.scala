package mesosphere.mesos

import mesosphere.marathon.MarathonConf
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.raml
import mesosphere.marathon.state.{ EnvVarString, PathId, Timestamp }
import mesosphere.mesos.ResourceMatcher.ResourceSelector
import org.apache.mesos.{ Protos => mesos }
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object TaskGroupBuilder {
  val log = LoggerFactory.getLogger(getClass)

  def build(
    podDefinition: PodDefinition,
    offer: mesos.Offer,
    runningTasks: => Iterable[Task],
    newTaskId: PathId => Instance.Id,
    config: MarathonConf
  ): Option[(mesos.ExecutorInfo, mesos.TaskGroupInfo, Seq[Option[Int]])] = {
    val acceptedResourceRoles: Set[String] = {
      val roles = if (podDefinition.acceptedResourceRoles.isEmpty) {
        config.defaultAcceptedResourceRolesSet
      } else {
        podDefinition.acceptedResourceRoles
      }
      if (log.isDebugEnabled) log.debug(s"acceptedResourceRoles $roles")
      roles
    }

    val resourceMatchOpt: Option[ResourceMatcher.ResourceMatch] =
      ResourceMatcher.matchResources(offer, podDefinition, runningTasks, ResourceSelector.any(acceptedResourceRoles))

    resourceMatchOpt match {
      case Some(resourceMatch) =>
        build(podDefinition, offer, newTaskId, config, resourceMatch)
      case _ =>
        None
    }
  }

  private[this] def build(
    podDefinition: PodDefinition,
    offer: mesos.Offer,
    newTaskId: PathId => Instance.Id,
    config: MarathonConf,
    resourceMatch: ResourceMatcher.ResourceMatch
  ): Some[(mesos.ExecutorInfo, mesos.TaskGroupInfo, Seq[Option[Int]])] = {
    // TODO: probably set unique ID for each task
    val taskId = newTaskId(podDefinition.id)

    val executorInfo = computeExecutorInfo(podDefinition, taskId)

    val taskGroup = mesos.TaskGroupInfo.newBuilder

    podDefinition.containers
      .map(computeTaskInfo(_, podDefinition, offer, taskId, config, resourceMatch))
      .foreach(taskGroup.addTasks)

    Some((executorInfo.build, taskGroup.build, resourceMatch.hostPorts))
  }

  private[this] def computeTaskInfo(
    container: raml.MesosContainer,
    podDefinition: PodDefinition,
    offer: mesos.Offer,
    taskId: Instance.Id,
    config: MarathonConf,
    resourceMatch: ResourceMatcher.ResourceMatch
  ): mesos.TaskInfo.Builder = {
    val builder = mesos.TaskInfo.newBuilder
      .setName(container.name)
      .setTaskId(taskId.mesosTaskId)
      .setSlaveId(offer.getSlaveId)

    builder.addResources(scalarResource("cpus", container.resources.cpus))
    builder.addResources(scalarResource("mem", container.resources.mem))
    builder.addResources(scalarResource("disk", container.resources.disk))
    builder.addResources(scalarResource("gpus", container.resources.gpus.toDouble))

    val labels = podDefinition.labels ++ container.labels.map(_.values).getOrElse(Map.empty[String, String])

    if (labels.nonEmpty)
      builder.setLabels(mesos.Labels.newBuilder.addAllLabels(labels.map {
        case (key, value) =>
          mesos.Label.newBuilder.setKey(key).setValue(value).build
      }.asJava))

    val envPrefix: Option[String] = config.envVarsPrefix.get

    val commandInfo = computeCommandInfo(
      podDefinition,
      taskId,
      container,
      offer.getHostname,
      resourceMatch.hostPorts,
      envPrefix)

    builder.setCommand(commandInfo)

    val containerInfo = computeContainerInfo(podDefinition.podVolumes, container)

    builder.setContainer(containerInfo)

    container.healthCheck.foreach { healthCheck =>
      builder.setHealthCheck(computeHealthCheck(healthCheck))
    }

    builder
  }

  private[this] def computeExecutorInfo(
    podDefinition: PodDefinition,
    taskId: Instance.Id): mesos.ExecutorInfo.Builder = {
    val executorID = mesos.ExecutorID.newBuilder.setValue(f"marathon-${taskId.idString}")

    val executorInfo = mesos.ExecutorInfo.newBuilder
      .setType(mesos.ExecutorInfo.Type.DEFAULT)
      .setExecutorId(executorID)

    // We're deliberately not setting executor resources here. Because we're using the default executor, the
    // default resource values can be used.

    // TODO: set networks (in containerinfo?)

    if (podDefinition.labels.nonEmpty) {
      val labels = mesos.Labels.newBuilder
        .addAllLabels(podDefinition.labels.map {
          case (key, value) =>
            mesos.Label.newBuilder.setKey(key).setValue(value).build
        }.asJava)

      executorInfo.setLabels(labels)
    }

    executorInfo
  }

  private[this] def computeCommandInfo(
    podDefinition: PodDefinition,
    taskId: Instance.Id,
    container: raml.MesosContainer,
    host: String,
    hostPorts: Seq[Option[Int]],
    envPrefix: Option[String]): mesos.CommandInfo.Builder = {
    val commandInfo = mesos.CommandInfo.newBuilder

    container.exec.foreach{ exec =>
      exec.command match {
        case raml.ShellCommand(shell) =>
          commandInfo.setShell(true)
          commandInfo.setValue(shell)
        case raml.ArgvCommand(argv) =>
          commandInfo.setShell(false)
          if (argv.nonEmpty) {
            commandInfo.setValue(argv.head)
          }

          if (argv.size > 1) {
            commandInfo.addAllArguments(argv.tail.asJava)
          }
      }
    }

    // Container user overrides pod user
    val user = container.user.orElse(podDefinition.user)
    user.foreach(commandInfo.setUser)

    val uris = container.artifacts.map { artifact =>
      val uri = mesos.CommandInfo.URI.newBuilder.setValue(artifact.uri)

      artifact.cache.foreach(uri.setCache)
      artifact.extract.foreach(uri.setExtract)
      artifact.executable.foreach(uri.setExecutable)

      uri.build
    }

    commandInfo.addAllUris(uris.asJava)

    val podEnvVars = podDefinition.env.collect{ case (k: String, v: EnvVarString) => k -> v.value }

    val taskEnvVars = container.environment
      .map(_.values)
      .getOrElse(Map.empty[String, raml.EnvVarValueOrSecret])
      .collect{ case (k: String, v: raml.EnvVarValue) => k -> v.value }

    val hostEnvVar = Map("HOST" -> host)

    val taskContextEnvVars = taskContextEnv(container, podDefinition.version, taskId)

    val labels = podDefinition.labels ++ container.labels.map(_.values).getOrElse(Map.empty[String, String])

    val labelEnvVars = labelsToEnvVars(labels)

    // Variables defined on task level should override ones defined at pod level.
    // Therefore the order here is important. Values for existing keys will be overwritten in the order they are added.
    val envVars = (podEnvVars ++
      taskEnvVars ++
      hostEnvVar ++
      taskContextEnvVars ++
      labelEnvVars)
      .map {
        case (name, value) =>
          mesos.Environment.Variable.newBuilder.setName(name).setValue(value).build
      }

    commandInfo.setEnvironment(mesos.Environment.newBuilder.addAllVariables(envVars.asJava))

    commandInfo
  }

  private[this] def computeContainerInfo(
    podVolumes: Seq[raml.Volume],
    container: raml.MesosContainer): mesos.ContainerInfo.Builder = {
    val containerInfo = mesos.ContainerInfo.newBuilder
      .setType(mesos.ContainerInfo.Type.MESOS)

    container.volumeMounts.foreach { volumeMount =>
      podVolumes.find(_.name == volumeMount.name).map { hostVolume =>
        val volume = mesos.Volume.newBuilder
          .setContainerPath(volumeMount.mountPath)

        hostVolume.host.foreach(volume.setHostPath)

        // Read-write mode will be used when the "readOnly" option isn't set.
        val mode = volumeMount.readOnly.getOrElse(false) match {
          case true => mesos.Volume.Mode.RO
          case false => mesos.Volume.Mode.RW
        }

        volume.setMode(mode)

        containerInfo.addVolumes(volume)
      }
    }

    container.image.foreach { im =>
      val image = mesos.Image.newBuilder

      im.forcePull.foreach(forcePull => image.setCached(!forcePull))

      im.kind match {
        case raml.ImageType.Docker =>
          val docker = mesos.Image.Docker.newBuilder.setName(im.id)

          image.setType(mesos.Image.Type.DOCKER).setDocker(docker)
        case raml.ImageType.Appc =>
          // These labels are necessary for AppC images to work.
          // Given that Docker only works under linux with 64bit,
          // let's (for now) set these values to reflect that.
          val labels = mesos.Labels.newBuilder
            .addAllLabels(
              Iterable(
              mesos.Label.newBuilder.setKey("os").setValue("linux").build,
              mesos.Label.newBuilder.setKey("arch").setValue("amd64").build
            ).asJava)

          val appc = mesos.Image.Appc.newBuilder.setName(im.id).setLabels(labels)

          image.setType(mesos.Image.Type.APPC).setAppc(appc)
      }

      val mesosInfo = mesos.ContainerInfo.MesosInfo.newBuilder.setImage(image)
      containerInfo.setMesos(mesosInfo)
    }

    containerInfo
  }

  private[this] def computeHealthCheck(healthCheck: raml.HealthCheck): mesos.HealthCheck.Builder = {
    val builder = mesos.HealthCheck.newBuilder

    if (healthCheck.command.isDefined) {
      builder.setType(mesos.HealthCheck.Type.COMMAND)

      val command = mesos.CommandInfo.newBuilder

      healthCheck.command.get.command match {
        case raml.ShellCommand(shell) =>
          command.setShell(true)
          command.setValue(shell)
        case raml.ArgvCommand(argv) =>
          command.setShell(false)
          if (argv.nonEmpty) {
            command.setValue(argv.head)
          }

          if (argv.size > 1) {
            command.addAllArguments(argv.tail.asJava)
          }
      }

      builder.setCommand(command)
    } else if (healthCheck.http.isDefined) {
      builder.setType(mesos.HealthCheck.Type.HTTP)

      val http = mesos.HealthCheck.HTTPCheckInfo.newBuilder

      healthCheck.http.get.scheme.foreach(scheme => http.setScheme(scheme.value))
      healthCheck.http.get.path.foreach(path => http.setPath(path))

      builder.setHttp(http)
    } else if (healthCheck.tcp.isDefined) {
      builder.setType(mesos.HealthCheck.Type.TCP)

      val tcp = mesos.HealthCheck.TCPCheckInfo.newBuilder

      // TODO: set port

      builder.setTcp(tcp)
    }

    builder
  }

  private[this] def taskContextEnv(
    container: raml.MesosContainer,
    version: Timestamp,
    taskId: Instance.Id): Map[String, String] = {
    Map(
      "MESOS_TASK_ID" -> Some(taskId.idString),
      "MARATHON_APP_ID" -> Some(container.name),
      "MARATHON_APP_VERSION" -> Some(version.toString),
      "MARATHON_APP_RESOURCE_CPUS" -> Some(container.resources.cpus.toString),
      "MARATHON_APP_RESOURCE_MEM" -> Some(container.resources.mem.toString),
      "MARATHON_APP_RESOURCE_DISK" -> Some(container.resources.disk.toString),
      "MARATHON_APP_RESOURCE_GPUS" -> Some(container.resources.gpus.toString)
    ).collect {
        case (key, Some(value)) => key -> value
      }
  }

  private[this] def labelsToEnvVars(labels: Map[String, String]): Map[String, String] = {
    val maxEnvironmentVarLength = 512
    val labelEnvironmentKeyPrefix = "MARATHON_APP_LABEL_"
    val maxVariableLength = maxEnvironmentVarLength - labelEnvironmentKeyPrefix.length

    def escape(name: String): String = name.replaceAll("[^a-zA-Z0-9_]+", "_").toUpperCase

    val validLabels = labels.collect {
      case (key, value) if key.length < maxVariableLength
        && value.length < maxEnvironmentVarLength => escape(key) -> value
    }

    val names = Map("MARATHON_APP_LABELS" -> validLabels.keys.mkString(" "))
    val values = validLabels.map { case (key, value) => s"$labelEnvironmentKeyPrefix$key" -> value }
    names ++ values
  }

  private[this] def scalarResource(name: String, value: Double): mesos.Resource.Builder = {
    mesos.Resource.newBuilder
      .setName(name)
      .setType(mesos.Value.Type.SCALAR)
      .setScalar(mesos.Value.Scalar.newBuilder.setValue(value))
  }
}
