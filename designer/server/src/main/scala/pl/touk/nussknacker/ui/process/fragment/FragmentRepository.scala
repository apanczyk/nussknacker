package pl.touk.nussknacker.ui.process.fragment

import pl.touk.nussknacker.engine.api.process.{ProcessName, ProcessingType}
import pl.touk.nussknacker.engine.canonicalgraph.CanonicalProcess
import pl.touk.nussknacker.ui.process.ScenarioQuery
import pl.touk.nussknacker.ui.process.repository.FetchingProcessRepository
import pl.touk.nussknacker.ui.process.repository.ProcessDBQueryRepository.ProcessNotFoundError
import pl.touk.nussknacker.ui.security.api.LoggedUser

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

trait FragmentRepository {

  // FIXME: async version should be used instead
  final def fetchLatestFragmentsSync(processingType: ProcessingType)(implicit user: LoggedUser): List[FragmentDetails] =
    Await.result(fetchLatestFragments(processingType), 10 seconds)

  def fetchLatestFragments(processingType: ProcessingType)(implicit user: LoggedUser): Future[List[FragmentDetails]]

  // FIXME: async version should be used instead
  final def fetchLatestFragmentSync(fragmentName: ProcessName)(implicit user: LoggedUser): Option[FragmentDetails] =
    Await.result(fetchLatestFragment(fragmentName), 10 seconds)

  def fetchLatestFragment(fragmentName: ProcessName)(implicit user: LoggedUser): Future[Option[FragmentDetails]]

}

final case class FragmentDetails(canonical: CanonicalProcess, category: String)

class DefaultFragmentRepository(processRepository: FetchingProcessRepository[Future])(implicit ec: ExecutionContext)
    extends FragmentRepository {

  override def fetchLatestFragments(
      processingType: ProcessingType
  )(implicit user: LoggedUser): Future[List[FragmentDetails]] = {
    processRepository
      .fetchLatestProcessesDetails[CanonicalProcess](
        ScenarioQuery(isFragment = Some(true), isArchived = Some(false), processingTypes = Some(List(processingType)))
      )
      .map(_.map(sub => FragmentDetails(sub.json, sub.processCategory)))
  }

  override def fetchLatestFragment(
      fragmentName: ProcessName
  )(implicit user: LoggedUser): Future[Option[FragmentDetails]] = {
    processRepository
      .fetchProcessId(fragmentName)
      .flatMap { processIdOpt =>
        val processId = processIdOpt.getOrElse(throw ProcessNotFoundError(fragmentName.toString))
        processRepository.fetchLatestProcessDetailsForProcessId[CanonicalProcess](processId)
      }
      .map(_.map(sub => FragmentDetails(sub.json, sub.processCategory)))
  }

}
