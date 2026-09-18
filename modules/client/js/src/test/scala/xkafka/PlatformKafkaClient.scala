package xkafka

import scala.scalajs.js

import cats.effect.IO

private[xkafka] object PlatformKafkaClient:
  val name: String                                = "js"
  val integrationBootstrapServers: Option[String] =
    js.Dynamic.global.process.env
      .selectDynamic("XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS")
      .asInstanceOf[js.UndefOr[String]]
      .toOption

  def apply(): KafkaClient[IO] = KafkaClient[IO]
