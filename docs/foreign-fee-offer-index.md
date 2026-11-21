# Foreign fee offer index

The foreign-fee manager consumes a derived view of current cross-chain offers. Canonical AT
definitions, current-state heads, payloads, and balances remain the source of truth.

## Update model

- Startup builds one complete snapshot with bulk AT-state and balance reads.
- Committed block events are coalesced by minimum changed height and processed on one background
  thread. Incremental refreshes read the compact AT block-result rows for those heights and filter
  them through immutable in-memory ACCT definitions; they do not rescan or join the full AT tables.
- Newly deployed AT definitions are learned in one bulk lookup. Unsupported AT addresses are
  remembered so their immutable definitions are not queried again.
- Bulk synchronization defers index work until synchronization finishes.
- The last 64 immutable tip snapshots are retained. An ordinary reorg restores the exact parent
  snapshot and then applies only the replacement branch's changed ATs. If the parent is outside
  that bounded history, the index safely falls back to a complete repository rebuild.
- An hourly rebuild detects missed events and repairs any derived-state drift.
- A failed rebuild retains the last complete snapshot and retries. Partial snapshots are never
  published.

No index write is part of block processing, and the index has no role in consensus validation.

## Foreign-fee admission

- Peer candidates are admitted only for an offer in the current snapshot.
- Pending candidates are keyed by AT address and only the newest timestamp is retained.
- Message, pending-queue, timestamp, signature-length, and fee checks bound untrusted work.
- Creator public keys come from the offer snapshot, so Ed25519 verification performs no repository
  work and holds no database transaction.
- Verification is capped per run; unprocessed candidates remain queued.

Normal reconciliation contacts up to three rotating peers when an active offer is missing a
signature. A full reconciliation runs hourly for recovery. The existing wire format remains
compatible.

## Persistence

Only active signed fees are retained. Backup files are written to a temporary sibling, forced to
disk, and atomically renamed. A failed write restores the dirty flags so the next backup retries.
