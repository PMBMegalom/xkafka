package xkafka

import cats.effect.IO

private[xkafka] object PlatformKafkaClient:
  val name: String                                = "native"
  val integrationBootstrapServers: Option[String] =
    sys.env.get("XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS")

  def apply(): KafkaClient[IO] = KafkaClient[IO]
