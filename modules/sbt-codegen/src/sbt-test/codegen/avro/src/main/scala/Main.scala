import example.audit.{Entry, Level}
import example.web.Visit
import xkafka.avro.{AvroCodec, AvroFormat}

def check[A](value: A)(using AvroCodec[A]): Unit =
  val format = AvroFormat[A]
  val back   = format.writer(format.schema).flatMap(_(value)).flatMap(bytes => format.reader(format.schema).flatMap(_(bytes)))
  require(back == Right(value), s"$value read back as $back")

@main def main(): Unit =
  check(Visit("v-1"))
  check(Entry("me", Level.HIGH))
  println(s"pages default ${Visit("v").pages}")
