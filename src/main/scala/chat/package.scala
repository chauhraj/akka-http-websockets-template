import java.nio.file.{Files, Path}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date
import java.util.zip.GZIPInputStream

import spray.json.{JsString, JsValue, RootJsonFormat}

import scala.io.Source

/**
  * Created by chauhraj on 9/20/16.
  */
package object chat {

  implicit def localDateTimeJsonFormat = new RootJsonFormat[LocalDateTime] {
    val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
    override def read(json: JsValue): LocalDateTime = LocalDateTime.parse(json.asInstanceOf[JsString].value, dateFormatter)

    override def write(obj: LocalDateTime): JsValue = JsString(obj.format(dateFormatter))
  }

  implicit class RichStringsOps(s: String) {
    def fromLongToLocalDateTime = {
      val t = new Date(s.toLong).toInstant
      LocalDateTime.ofInstant(t, ZoneId.systemDefault())
    }
  }

  case class GZIPReader[+T](path: Path)(f: Array[String] => T) extends Iterable[T]  {
    override def iterator: Iterator[T] = {
      Source.fromInputStream(new GZIPInputStream(Files.newInputStream(path))).getLines().map(l => f(l.split(" ")))
    }
  }
}
