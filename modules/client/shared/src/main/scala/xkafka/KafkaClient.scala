package xkafka

import cats.effect.Async
import cats.effect.Resource

trait KafkaClient[F[_]]:
  def producer[K, V](
      settings: ProducerSettings[F, K, V]
  ): Resource[F, KafkaProducer[F, K, V]]

  def consumer[K, V](
      settings: ConsumerSettings[F, K, V],
      subscription: Subscription
  ): Resource[F, KafkaConsumer[F, K, V]]

object KafkaClient:
  def apply[F[_]: Async]: KafkaClient[F] =
    KafkaClientPlatform[F]
