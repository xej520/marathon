package mesosphere.marathon
package storage.migration.legacy

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.state._
import mesosphere.marathon.storage.repository.AppRepository

import scala.concurrent.{ ExecutionContext, Future }

@SuppressWarnings(Array("ClassNames"))
class MigrationTo_1_4_2(appRepository: AppRepository)(implicit
  ctx: ExecutionContext,
    mat: Materializer) extends StrictLogging {

  import MigrationTo_1_4_2.migrationFlow
  val sink =
    Flow[AppDefinition]
      .mapAsync(Int.MaxValue)(appRepository.store)
      .toMat(Sink.ignore)(Keep.right)

  def migrate(): Future[Done] = {
    logger.info("Starting migration to 1.4.2")

    appRepository.all()
      .via(migrationFlow)
      .runWith(sink)
      .andThen {
        case _ =>
          logger.info("Finished 1.4.2 migration")
      }
  }
}

@SuppressWarnings(Array("ObjectNames"))
object MigrationTo_1_4_2 extends StrictLogging {
  private def fixResidentApp(app: AppDefinition): AppDefinition = {
    if (app.isResident)
      app.copy(unreachableStrategy = UnreachableDisabled)
    else
      app
  }

  val migrationFlow =
    Flow[AppDefinition]
      .filter(_.isResident)
      .map { app =>
        logger.info(s"Disable Unreachable strategy for resident app: ${app.id}")
        fixResidentApp(app)
      }
}
