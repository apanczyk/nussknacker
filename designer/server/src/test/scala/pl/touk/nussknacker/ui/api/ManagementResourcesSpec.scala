package pl.touk.nussknacker.ui.api

import akka.http.scaladsl.model.{ContentTypeRange, StatusCodes}
import akka.http.scaladsl.server
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import cats.instances.all._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.BeMatcher
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, OptionValues}
import pl.touk.nussknacker.engine.api.deployment.ProcessActionType.ProcessActionType
import pl.touk.nussknacker.engine.api.deployment.simple.SimpleStateStatus
import pl.touk.nussknacker.engine.api.deployment.{ProcessAction, ProcessActionType}
import pl.touk.nussknacker.engine.api.process.{ProcessName, VersionId}
import pl.touk.nussknacker.engine.api.{MetaData, StreamMetaData}
import pl.touk.nussknacker.engine.build.ScenarioBuilder
import pl.touk.nussknacker.engine.kafka.KafkaFactory
import pl.touk.nussknacker.engine.spel.Implicits._
import pl.touk.nussknacker.restmodel.scenariodetails._
import pl.touk.nussknacker.restmodel.{CustomActionRequest, CustomActionResponse}
import pl.touk.nussknacker.test.PatientScalaFutures
import pl.touk.nussknacker.ui.api.helpers.TestFactory._
import pl.touk.nussknacker.ui.api.helpers._
import pl.touk.nussknacker.ui.process.ScenarioQuery
import pl.touk.nussknacker.ui.process.exception.ProcessIllegalAction
import pl.touk.nussknacker.ui.process.repository.DbProcessActivityRepository.ProcessActivity

import java.time.Instant

class ManagementResourcesSpec
    extends AnyFunSuite
    with ScalatestRouteTest
    with FailFastCirceSupport
    with Matchers
    with PatientScalaFutures
    with OptionValues
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with NuResourcesTest {

  import KafkaFactory._
  import TestCategories._

  private implicit final val string: FromEntityUnmarshaller[String] =
    Unmarshaller.stringUnmarshaller.forContentTypes(ContentTypeRange.*)

  private val processName: ProcessName = SampleProcess.process.name

  private def deployedWithVersions(versionId: Long): BeMatcher[Option[ProcessAction]] = {
    BeMatcher[(ProcessActionType, VersionId)](equal((ProcessActionType.Deploy, VersionId(versionId))))
      .compose[ProcessAction](a => (a.actionType, a.processVersionId))
      .compose[Option[ProcessAction]](opt => opt.value)
  }

  test("process deployment should be visible in process history") {
    saveProcessAndAssertSuccess(processName, SampleProcess.process)
    deployProcess(processName) ~> checkThatEventually {
      status shouldBe StatusCodes.OK
      getProcess(processName) ~> check {
        decodeDetails.lastStateAction shouldBe deployedWithVersions(2)
        updateProcessAndAssertSuccess(processName, SampleProcess.process)
        deployProcess(processName) ~> checkThatEventually {
          getProcess(processName) ~> check {
            decodeDetails.lastStateAction shouldBe deployedWithVersions(2)
          }
        }
      }
    }
  }

  test("process during deploy cannot be deployed again") {
    createDeployedProcess(processName, Category1)

    deploymentManager.withProcessStateStatus(processName, SimpleStateStatus.DuringDeploy) {
      deployProcess(processName) ~> check {
        status shouldBe StatusCodes.Conflict
      }
    }
  }

  test("canceled process can't be canceled again") {
    createDeployedCanceledProcess(processName, Category1)

    deploymentManager.withProcessStateStatus(processName, SimpleStateStatus.Canceled) {
      cancelProcess(processName) ~> check {
        status shouldBe StatusCodes.Conflict
      }
    }
  }

  test("can't deploy archived process") {
    createArchivedProcess(processName)

    deploymentManager.withProcessStateStatus(processName, SimpleStateStatus.Canceled) {
      deployProcess(processName) ~> check {
        status shouldBe StatusCodes.Conflict
        responseAs[String] shouldBe ProcessIllegalAction.archived(ProcessActionType.Deploy, processName).message
      }
    }
  }

  test("can't deploy fragment") {
    createValidProcess(processName, Category1, isFragment = true)

    deployProcess(processName) ~> check {
      status shouldBe StatusCodes.Conflict
      responseAs[String] shouldBe ProcessIllegalAction.fragment(ProcessActionType.Deploy, processName).message
    }
  }

  test("can't cancel fragment") {
    createValidProcess(processName, Category1, isFragment = true)

    deployProcess(processName) ~> check {
      status shouldBe StatusCodes.Conflict
      responseAs[String] shouldBe ProcessIllegalAction.fragment(ProcessActionType.Deploy, processName).message
    }
  }

  test("deploys and cancels with comment") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)
    deployProcess(
      SampleProcess.process.name,
      Some(DeploymentCommentSettings.unsafe("deploy.*", Some("deployComment"))),
      comment = Some("deployComment")
    ) ~> checkThatEventually {
      getProcess(processName) ~> check {
        val processDetails = responseAs[ScenarioWithDetails]
        processDetails.lastStateAction.exists(_.actionType.equals(ProcessActionType.Deploy)) shouldBe true
      }
      cancelProcess(
        SampleProcess.process.name,
        Some(DeploymentCommentSettings.unsafe("cancel.*", Some("cancelComment"))),
        comment = Some("cancelComment")
      ) ~> check {
        status shouldBe StatusCodes.OK
        // TODO: remove Deployment:, Stop: after adding custom icons
        val expectedDeployComment = "Deployment: deployComment"
        val expectedStopComment   = "Stop: cancelComment"
        getActivity(SampleProcess.process.name) ~> check {
          val comments = responseAs[ProcessActivity].comments.sortBy(_.id)
          comments.map(_.content) shouldBe List(expectedDeployComment, expectedStopComment)

          val firstCommentId :: secondCommentId :: Nil = comments.map(_.id)

          Get(s"/processes/${SampleProcess.process.name}/deployments") ~> withAllPermissions(processesRoute) ~> check {
            val deploymentHistory = responseAs[List[ProcessAction]]
            val curTime           = Instant.now()
            deploymentHistory.map(a =>
              (a.processVersionId, a.user, a.actionType, a.commentId, a.comment, a.buildInfo)
            ) shouldBe List(
              (
                VersionId(2),
                user().username,
                ProcessActionType.Cancel,
                Some(secondCommentId),
                Some(expectedStopComment),
                Map()
              ),
              (
                VersionId(2),
                user().username,
                ProcessActionType.Deploy,
                Some(firstCommentId),
                Some(expectedDeployComment),
                TestFactory.buildInfo
              )
            )
          }
        }
      }
    }
  }

  test("rejects deploy without comment if comment required") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)
    deployProcess(
      SampleProcess.process.name,
      deploymentCommentSettings =
        Some(DeploymentCommentSettings.unsafe("requiredCommentPattern", Some("exampleRequiredComment")))
    ) ~> check {
      status shouldBe StatusCodes.BadRequest
    }
  }

  test("deploy technical process and mark it as deployed") {
    createValidProcess(processName, Category1, false)

    deployProcess(processName) ~> checkThatEventually {
      status shouldBe StatusCodes.OK
      getProcess(processName) ~> check {
        val processDetails = responseAs[ScenarioWithDetails]
        processDetails.lastStateAction shouldBe deployedWithVersions(1)
        processDetails.lastStateAction.exists(_.actionType.equals(ProcessActionType.Deploy)) shouldBe true
      }
    }
  }

  test("recognize process cancel in deployment list") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)
    deployProcess(SampleProcess.process.name) ~> checkThatEventually {
      status shouldBe StatusCodes.OK
      getProcess(processName) ~> check {
        decodeDetails.lastStateAction shouldBe deployedWithVersions(2)
        cancelProcess(SampleProcess.process.name) ~> check {
          getProcess(processName) ~> check {
            decodeDetails.lastStateAction.exists(_.actionType.equals(ProcessActionType.Cancel)) shouldBe true
          }
        }
      }
    }
  }

  test("recognize process deploy and cancel in global process list") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)
    deployProcess(SampleProcess.process.name) ~> checkThatEventually {
      status shouldBe StatusCodes.OK

      forScenariosReturned(ScenarioQuery.empty) { processes =>
        val process = processes.find(_.name == SampleProcess.process.name.value).head
        process.lastActionVersionId shouldBe Some(2L)
        process.isDeployed shouldBe true

        cancelProcess(SampleProcess.process.name) ~> check {
          forScenariosReturned(ScenarioQuery.empty) { processes =>
            val process = processes.find(_.name == SampleProcess.process.name.value).head
            process.lastActionVersionId shouldBe Some(2L)
            process.isCanceled shouldBe true
          }
        }
      }
    }
  }

  test("not authorize user with write permission to deploy") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)
    Post(s"/processManagement/deploy/${SampleProcess.process.name}") ~> withPermissions(
      deployRoute(),
      testPermissionWrite
    ) ~> check {
      rejection shouldBe server.AuthorizationFailedRejection
    }
  }

  test("should allow deployment of scenario with warning") {
    val processWithDisabledFilter = ScenarioBuilder
      .streaming("sampleProcess")
      .parallelism(1)
      .source("startProcess", "csv-source")
      .filter("input", "#input != null", Some(true))
      .emptySink("end", "kafka-string", TopicParamName -> "'end.topic'", SinkValueParamName -> "#input")

    saveProcessAndAssertSuccess(SampleProcess.process.name, processWithDisabledFilter)
    deployProcess(processName) ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  test("should return failure for not validating scenario") {
    val invalidScenario = ScenarioBuilder
      .streaming("sampleProcess")
      .parallelism(1)
      .source("start", "not existing")
      .emptySink("end", "kafka-string", TopicParamName -> "'end.topic'", SinkValueParamName -> "#output")
    saveProcessAndAssertSuccess(invalidScenario.name, invalidScenario)

    deploymentManager.withEmptyProcessState(invalidScenario.name) {
      deployProcess(invalidScenario.name) ~> check {
        responseAs[String] shouldBe "Cannot deploy invalid scenario"
        status shouldBe StatusCodes.Conflict
      }
      getProcess(invalidScenario.name) ~> check {
        decodeDetails.state.value.status shouldEqual SimpleStateStatus.NotDeployed
      }
    }
  }

  test("should return failure for not validating deployment") {
    val largeParallelismScenario = SampleProcess.process.copy(metaData =
      MetaData(
        SampleProcess.process.name.value,
        StreamMetaData(parallelism = Some(MockDeploymentManager.maxParallelism + 1))
      )
    )
    saveProcessAndAssertSuccess(largeParallelismScenario.name, largeParallelismScenario)

    deploymentManager.withFailingDeployment(largeParallelismScenario.name) {
      deployProcess(largeParallelismScenario.name) ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe "Parallelism too large"
      }
    }
  }

  test("return from deploy before deployment manager proceeds") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)

    deploymentManager.withWaitForDeployFinish(SampleProcess.process.name) {
      deployProcess(SampleProcess.process.name) ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }

  test("snapshots process") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)
    deploymentManager.withProcessRunning(SampleProcess.process.name) {
      snapshot(SampleProcess.process.name) ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] shouldBe MockDeploymentManager.savepointPath
      }
    }
  }

  test("stops process") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)
    deploymentManager.withProcessRunning(SampleProcess.process.name) {
      stop(SampleProcess.process.name) ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] shouldBe MockDeploymentManager.stopSavepointPath
      }
    }
  }

  test("return test results") {
    val testDataContent =
      """{"sourceId":"startProcess","record":"ala"}
        |{"sourceId":"startProcess","record":"bela"}""".stripMargin
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)

    testScenario(SampleProcess.process, testDataContent) ~> check {

      status shouldEqual StatusCodes.OK

      val ctx = responseAs[Json].hcursor
        .downField("results")
        .downField("nodeResults")
        .downField("endsuffix")
        .downArray
        .downField("variables")

      ctx
        .downField("output")
        .downField("pretty")
        .downField("message")
        .focus shouldBe Some(Json.fromString("message"))

      ctx
        .downField("input")
        .downField("pretty")
        .downField("firstField")
        .focus shouldBe Some(Json.fromString("ala"))
    }
  }

  test("return test results of errors, including null") {

    import pl.touk.nussknacker.engine.spel.Implicits._

    val process = ScenarioBuilder
      .streaming("sampleProcess")
      .parallelism(1)
      .source("startProcess", "csv-source")
      .filter("input", "new java.math.BigDecimal(null) == 0")
      .emptySink("end", "kafka-string", TopicParamName -> "'end.topic'", SinkValueParamName -> "''")
    val testDataContent =
      """{"sourceId":"startProcess","record":"ala"}
        |"bela"""".stripMargin
    saveProcessAndAssertSuccess(process.name, process)

    testScenario(process, testDataContent) ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  test("refuses to test if too much data") {

    import pl.touk.nussknacker.engine.spel.Implicits._

    val process = {
      ScenarioBuilder
        .streaming("sampleProcess")
        .parallelism(1)
        .source("startProcess", "csv-source")
        .emptySink("end", "kafka-string", TopicParamName -> "'end.topic'")
    }
    saveProcessAndAssertSuccess(process.name, process)
    val tooLargeTestDataContentList = List((1 to 50).mkString("\n"), (1 to 50000).mkString("-"))

    tooLargeTestDataContentList.foreach { tooLargeData =>
      testScenario(process, tooLargeData) ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }
  }

  test("rejects test record with non-existing source") {
    saveProcessAndAssertSuccess(SampleProcess.process.name, SampleProcess.process)
    val testDataContent =
      """{"sourceId":"startProcess","record":"ala"}
        |{"sourceId":"unknown","record":"bela"}""".stripMargin

    testScenario(SampleProcess.process, testDataContent) ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[String] shouldBe "Record 2 - scenario does not have source id: 'unknown'"
    }
  }

  test("execute valid custom action") {
    createEmptyProcess(SampleProcess.processName)
    customAction(SampleProcess.processName, CustomActionRequest("hello")) ~> check {
      status shouldBe StatusCodes.OK
      responseAs[CustomActionResponse] shouldBe CustomActionResponse(isSuccess = true, msg = "Hi")
    }
  }

  test("execute non existing custom action") {
    createEmptyProcess(SampleProcess.processName)
    customAction(SampleProcess.processName, CustomActionRequest("non-existing")) ~> check {
      status shouldBe StatusCodes.NotFound
      responseAs[CustomActionResponse] shouldBe CustomActionResponse(
        isSuccess = false,
        msg = "non-existing is not existing"
      )
    }
  }

  test("execute not implemented custom action") {
    createEmptyProcess(SampleProcess.processName)
    customAction(SampleProcess.processName, CustomActionRequest("not-implemented")) ~> check {
      status shouldBe StatusCodes.NotImplemented
      responseAs[CustomActionResponse] shouldBe CustomActionResponse(
        isSuccess = false,
        msg = "not-implemented is not implemented"
      )
    }
  }

  test("execute custom action with not allowed process status") {
    createEmptyProcess(SampleProcess.processName)
    customAction(SampleProcess.processName, CustomActionRequest("invalid-status")) ~> check {
      status shouldBe StatusCodes.Forbidden
      responseAs[CustomActionResponse] shouldBe CustomActionResponse(
        isSuccess = false,
        msg = s"Scenario status: NOT_DEPLOYED is not allowed for action invalid-status"
      )
    }
  }

  test("should return 403 when execute custom action on archived process") {
    createArchivedProcess(SampleProcess.processName)
    customAction(SampleProcess.processName, CustomActionRequest("hello")) ~> check {
      status shouldBe StatusCodes.Forbidden
    }
  }

  test("should return 403 when execute custom action on fragment") {
    createEmptyProcess(SampleProcess.processName, isFragment = true)
    customAction(SampleProcess.processName, CustomActionRequest("hello")) ~> check {
      status shouldBe StatusCodes.Forbidden
    }
  }

  def decodeDetails: ScenarioWithDetails = responseAs[ScenarioWithDetails]

  def checkThatEventually[T](body: => T): RouteTestResult => T = check(eventually(body))

}
