$version: "2"

namespace example.shop

use alloy#discriminated
use alloy#untagged
use alloy#nullable
use alloy#UUID
use alloy#LocalDate
use alloy#LocalTime
use alloy#Duration
use alloy#OffsetDateTime

/// An order, as the shop writes it.
structure Order {
    /// Who knows the order by it.
    @required
    id: UUID

    @required
    placed: Timestamp

    @timestampFormat("date-time")
    shipped: Timestamp

    @timestampFormat("http-date")
    billed: Timestamp

    @required
    @length(min: 1, max: 50)
    lines: Lines

    status: Status = "new"

    @range(min: 0, max: 100)
    discount: Integer

    @jsonName("first-name")
    firstName: String

    payment: Payment

    tags: Tags

    counts: Counts

    blob: Blob

    @pattern("^[A-Z]{3}$")
    @length(min: 3, max: 3)
    currency: String

    meta: Document

    priority: Priority

    @nullable
    note: String

    ratio: Double

    big: BigDecimal

    huge: BigInteger

    small: Byte

    short: Short

    flag: Boolean = false

    day: LocalDate

    opens: LocalTime

    wait: Duration

    at: OffsetDateTime

    code: Code

    sparse: SparseNames

    previous: Order
}

list Lines {
    member: Line
}

structure Line {
    @required
    sku: String

    quantity: Long = 1
}

@uniqueItems
list Tags {
    member: String
}

@length(max: 10)
map Counts {
    key: String
    value: Long
}

@sparse
list SparseNames {
    member: String
}

enum Status {
    NEW = "new"
    PAID = "paid"
}

intEnum Priority {
    LOW = 1
    HIGH = 2
}

union Payment {
    cash: Cash
    card: Card
}

structure Cash {
    @required
    amount: Double
}

structure Card {
    @required
    amount: Double

    @required
    last4: String
}

@discriminated("kind")
union Event {
    created: Created
    cancelled: Cancelled
}

structure Created {
    @required
    order: String
}

structure Cancelled {
    @required
    order: String

    reason: String
}

@untagged
union Code {
    text: String
    number: Integer
}

structure Node {
    @required
    value: Integer

    children: Nodes
}

list Nodes {
    member: Node
}

/// Trees, whose nodes hold nodes.
structure Forest {
    @required
    trees: Nodes
}
