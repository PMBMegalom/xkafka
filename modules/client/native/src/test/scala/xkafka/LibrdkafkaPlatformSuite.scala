package xkafka

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite

final class LibrdkafkaPlatformSuite extends CatsEffectSuite:
  test("loads librdkafka through the native shim") {
    assert(LibrdkafkaPlatform.version.nonEmpty)
  }

  test("allocates and releases a native producer") {
    val serializer = Serializer.const[IO, String](
      Some(Chunk.array(Array.emptyByteArray))
    )
    val settings = ProducerSettings(
      ClientSettings(NonEmptyList.one("localhost:9092"), Some("native-test")),
      serializer,
      serializer
    )

    KafkaClient[IO].producer(settings).use(_ => IO.unit)
  }
