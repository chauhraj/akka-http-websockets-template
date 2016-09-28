package chat

import java.nio.file.Paths
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server._
import Directives._
import akka.actor.Actor.Receive
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson._
import akka.stream.actor.ActorPublisher
import com.typesafe.config.{Config, ConfigFactory}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.io.StdIn

case class Price(id: Long, symbol: String, price: Double, timestamp: LocalDateTime)

object Price extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val pricejsonSupplier = jsonFormat4(Price.apply)

}

object Server {

  def main(args: Array[String]): Unit = {
    val configFile = ConfigFactory.parseFile(Paths.get(sys.props("config.file")).toFile)
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    def handleWebsocket: Directive1[UpgradeToWebSocket] = optionalHeaderValueByType[UpgradeToWebSocket](()).flatMap {
      case Some(upgrade) => provide(upgrade)
      case _ => reject(ExpectedWebSocketRequestRejection)
    }

    val route = pathPrefix("ws" / "chat") {
      pathEndOrSingleSlash {
        handleWebsocket { ws =>
          complete {
            val source = Source.actorPublisher[Int](Props[TickActor]).map{
              i =>
                val v = TextMessage.Strict(i.toString)
                println("Value of v:" + v)
                v
            }
            ws.handleMessagesWithSinkSource(Sink.ignore, source)
          }

        }
      }
    } ~ pathPrefix("subscribe" / "prices") {
      pathEndOrSingleSlash {
        handleWebsocket { ws =>
          complete {
            val source = Source.actorPublisher[Price](Props(classOf[PriceActor], configFile)).map{
              price =>
                val v = TextMessage.Strict(price.toJson.prettyPrint)
                println("Value of v:" + v)
                v
            }
            ws.handleMessagesWithSinkSource(Sink.ignore, source)
          }

        }
      }
    }


    val binding = Await.result(Http().bindAndHandle(route, "127.0.0.1", 8080), 3.seconds)


    // the rest of the sample code will go here
    println("Started server at 127.0.0.1:8080, press enter to kill server")
    StdIn.readLine()
    system.terminate()
  }
}


object Tick
class TickActor extends ActorPublisher[Int] {
  import scala.concurrent.duration._

  implicit val ec = context.dispatcher

  val tick = context.system.scheduler.schedule(1 second, 1 second, self, Tick)

  var cnt = 123456
  var buffer = Vector.empty[Int]

  override def receive: Receive = {
    case Tick => {
      cnt = cnt + 1
      if (buffer.isEmpty && totalDemand > 0) {
        onNext(cnt)
      }
      else {
        buffer :+ (cnt)
        if (totalDemand > 0) {
          val (use,keep) = buffer.splitAt(totalDemand.toInt)
          buffer = keep
          use foreach onNext
        }
      }
    }
  }

  override def postStop() = tick.cancel()
}

class PriceActor(val config: Config) extends ActorPublisher[Price] {
  import scala.concurrent.duration._

  implicit val ec = context.dispatcher

  var buffer = Vector.empty[Price]
  var cancel: Cancellable = null

  var priceIterator: Iterator[Price] = null
  val idGenerator = new AtomicInteger(0)

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    cancel = context.system.scheduler.schedule(1 second, 1 second)(sendPriceUpdate)

    priceIterator = GZIPReader[Price](Paths.get(config.getString("data.prices"))){
      components =>

        Price(idGenerator.addAndGet(1), components(2), components(3).toDouble, components(1).fromLongToLocalDateTime)
    }.iterator
  }

  def sendPriceUpdate(): Unit = {
    if(priceIterator.hasNext) {
      val price = priceIterator.next
      self ! price
    }
  }

  override def receive: Receive = {
    case price: Price => {
      if (buffer.isEmpty && totalDemand > 0) {
        onNext(price)
      }
      else {
        buffer :+ (price)
        if (totalDemand > 0) {
          val (use,keep) = buffer.splitAt(totalDemand.toInt)
          buffer = keep
          use foreach onNext
        }
      }
    }
  }

  override def postStop() = cancel.cancel()
}