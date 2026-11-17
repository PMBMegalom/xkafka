import example.untitled.Page
import example.web.Visit
import xkafka.jsonschema.{JsonSchemaCodec, JsonSchemaFormat}

def check[A](value: A)(using JsonSchemaCodec[A]): Unit =
  val format = JsonSchemaFormat[A]
  val back   = format.writer(format.schema).flatMap(_(value)).flatMap(bytes => format.reader(format.schema).flatMap(_(bytes)))
  require(back == Right(value), s"$value read back as $back")

@main def main(): Unit =
  check(Visit("v-1", tag = Some(Visit.Tag.back)))
  check(Page("/"))
  val seconds: Double = Visit("v").seconds
  require(seconds == 1.5)
