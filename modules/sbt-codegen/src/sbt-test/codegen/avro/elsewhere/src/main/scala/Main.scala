@main def main(): Unit = require(example.tags.Tag(fs2.Chunk[Byte](1, 2)).bytes.size == 2)
