$version: "2"

namespace example.proto

use alloy.proto#protoIndex
use alloy.proto#protoNumType
use alloy.proto#protoInlinedOneOf
use alloy.proto#protoWrapped
use alloy#UUID
use alloy#LocalDate
use alloy#LocalTime
use alloy#Duration
use alloy#OffsetDateTime
use alloy.proto#protoCompactUUID
use alloy.proto#protoCompactLocalDate
use alloy.proto#protoCompactLocalTime
use alloy.proto#protoTimestampFormat
use alloy.proto#protoOffsetDateTimeFormat

/// A payment, as the till writes it.
structure Payment {
    @required
    id: UUID

    @required
    at: Timestamp

    amount: Double

    cents: Long

    @protoNumType("SIGNED")
    delta: Long

    @protoNumType("FIXED")
    hash: Integer

    @protoNumType("UNSIGNED")
    count: Integer

    small: Byte

    short: Short

    rate: Float

    paid: Boolean

    note: String

    big: BigDecimal

    huge: BigInteger

    receipt: Blob

    day: LocalDate

    extra: Document

    @protoWrapped
    tip: Long

    method: Method

    tender: Tender

    status: Status

    tags: Tags

    totals: Totals

    lines: Lines

    previous: Payment

    @protoNumType("FIXED_SIGNED")
    offset: Long

    opens: LocalTime

    when: OffsetDateTime

    wait: Duration
}

union Method {
    cash: Cash
    card: Card
}

/// How a payment was tendered, its detail inlined, every member numbered.
structure Tender {
    @protoIndex(1)
    label: String

    @protoIndex(2)
    detail: Detail
}

@protoInlinedOneOf
union Detail {
    @protoIndex(3)
    text: String

    @protoIndex(4)
    code: Integer
}

structure Cash {
    @required
    given: Double
}

structure Card {
    @required
    last4: String
}

enum Status {
    PENDING
    SETTLED
    REFUSED
}

list Tags {
    member: String
}

map Totals {
    key: String
    value: Long
}

list Lines {
    member: Line
}

structure Line {
    @required
    sku: String

    quantity: Integer = 1
}

/// A receipt, numbered by hand.
structure Receipt {
    @protoIndex(3)
    @required
    number: String

    @protoIndex(7)
    kind: Kind
}

enum Kind {
    @protoIndex(0)
    UNKNOWN

    @protoIndex(5)
    COPY
}

/// A structure whose inlined union's members take the numbers of its own fields.
structure Clash {
    name: String

    choice: Choice
}

@protoInlinedOneOf
union Choice {
    word: String
    number: Integer
}

/// Shapes a .proto file cannot declare.
structure Lists {
    lists: ListOfTags
}

list ListOfTags {
    member: Tags
}

structure MapOfLists {
    byName: TagsByName
}

map TagsByName {
    key: String
    value: Tags
}

structure UnionOfList {
    held: HoldsTags
}

union HoldsTags {
    tags: Tags
    number: Integer
}

structure Compact {
    @protoCompactUUID
    id: UUID
}

structure SameValues {
    first: First
    second: Second
}

enum First {
    SAME
}

enum Second {
    SAME
}

structure SameNames {
    here: Card
    there: example.elsewhere#Card
}

structure CompactDay {
    @protoCompactLocalDate
    day: LocalDate
}

structure CompactTime {
    @protoCompactLocalTime
    opens: LocalTime
}

structure Millis {
    @protoTimestampFormat("EPOCH_MILLIS")
    at: Timestamp
}

structure ProtobufOffset {
    @protoOffsetDateTimeFormat("PROTOBUF")
    when: OffsetDateTime
}

structure WrappedTime {
    @protoWrapped
    at: Timestamp
}

structure WrappedList {
    @protoWrapped
    tags: Tags
}

/// A ticket whose kind numbers its default second.
structure Ticket {
    @protoIndex(1)
    grade: Grade
}

enum Grade {
    @protoIndex(4)
    HIGH

    @protoIndex(0)
    NONE
}
