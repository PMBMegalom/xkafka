package xkafka

import fs2.Chunk
import munit.FunSuite

final class ModelSuite extends FunSuite:
  test("topic rejects an empty name") {
    assertEquals(Topic.from(""), Left(ValidationError.EmptyTopic))
  }

  test("partition rejects a negative value") {
    assertEquals(
      Partition.from(-1),
      Left(ValidationError.NegativePartition(-1))
    )
  }

  test("offset next detects overflow") {
    val offset = Offset.from(Long.MaxValue).toOption.get

    assertEquals(offset.next, Left(ValidationError.OffsetOverflow))
  }

  test("headers preserve duplicates and insertion order") {
    val first   = Header("trace", Some(Chunk.array(Array[Byte](1))))
    val second  = Header("other", None)
    val third   = Header("trace", Some(Chunk.array(Array[Byte](2))))
    val headers = Headers(first, second, third)

    assertEquals(headers.values, Vector(first, second, third))
    assertEquals(
      headers.getAll("trace").map(_.map(_.toList)),
      Vector(Some(List[Byte](1)), Some(List[Byte](2)))
    )
  }
