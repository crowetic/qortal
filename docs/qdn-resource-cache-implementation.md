# QDN local-content cleanup index

## Purpose

The cleanup manager previously walked batches from the full arbitrary-transaction history every 30 seconds. Most transactions have no files on a typical node, so the loop repeatedly paid for database queries, Base58 conversion, metadata parsing, and filesystem probes that could not reclaim anything.

The replacement keeps a small index of transaction signatures that this node has actually hosted. The filesystem remains authoritative. The index only narrows the cleanup candidates; it never proves that a file may be deleted.

## Storage format

`LocalQdnContentJournal` stores an append-only binary journal at:

```text
<dataPath>/.qdn-content-journal-v1
```

It is outside HSQLDB and the repository bootstrap. No database table, schema update, or database migration is required.

Each fixed-size record has a versioned header, operation, 64-byte transaction signature, timestamp, and CRC32. The supported operations are present, absent, dirty, and discovery complete. Repeated dirty notifications are coalesced in memory. Periodic compaction writes the current hosted set and unresolved dirty set to a temporary file, forces it to disk, and atomically replaces the journal where supported.

The journal normally contains only signatures with local payload data and signatures awaiting reconciliation. Metadata-only history is not retained in the hosted set. A damaged, truncated, or incompatible journal cannot authorize deletion: valid decoded entries are retained only as hints and a full gradual discovery is required again.

## First-run discovery

On the first upgraded run, cleanup uses its existing historical transaction list and its existing limit of 100 transactions per 30-second cycle. For each transaction it records whether a complete payload or at least one data chunk exists. There is no blocking directory walk and no one-time database migration.

After one complete pass, a durable completion marker is written and the historical list is released. Files created or changed while discovery runs are added to the dirty set, so a transaction skipped because of an active build or concurrent file operation is reconciled after discovery.

If the initial database query fails, completion is not recorded and discovery is retried. A corrupt journal, deleted journal, or changed data path follows the same safe discovery path. Restoring a repository bootstrap does not import another node's local journal.

## Steady-state cleanup

File operations mark their transaction signature dirty. Cleanup processes up to 100 dirty signatures per cycle, resolving their transactions in one batched repository lookup. Candidate selection rotates through the dirty set, so one missing or temporarily unresolved transaction cannot starve later work.

When no dirty work exists, cleanup audits 25 hosted signatures every five minutes. This catches external filesystem changes and missed notifications without returning to a permanent blockchain-history scan.

For each resolved transaction, cleanup rechecks the filesystem and storage policy before doing any work. It can remove data rejected by local policy, remove data replaced by a newer `PUT`, remove a redundant complete file when all chunks exist, or split an eligible complete file into chunks. It rechecks the remaining files before updating the journal.

A repository error defers the whole affected batch. An unresolved transaction with existing files is retained and retried. An unresolved transaction whose signature directory is absent or empty is safely removed from the journal.

## Concurrency and crash safety

There are striped per-signature leases around permanent reads, splitting, joins, saves, moves, relocation, and deletion. Cleanup uses a nonblocking cleanup lease and defers a busy signature. Unrelated signatures continue in parallel.

The build manager counts active builds instead of sharing one boolean between its worker threads. Indexed cleanup and storage-pressure eviction pause while any build is active. Recent files and `.original` seed copies retain their existing protection.

A dirty journal record is forced before a permanent mutation proceeds. If the process stops after the filesystem change but before reconciliation, the dirty signature is retried after restart. If a journal write fails, discovery completion is cleared so the filesystem is rediscovered safely.

## Hosted-data APIs and storage pressure

After discovery, hosted-data APIs resolve the journal's local signatures in repository batches instead of scanning signature directories and querying one transaction at a time. A generation counter invalidates the in-memory hosted list when the confirmed hosted set changes. Before discovery, or after a repository failure, the existing filesystem scan remains the fallback.

When storage is over its configured limit, eviction selects only confirmed local signatures after discovery. It batch-resolves those transactions before touching disk and preserves transaction metadata, `.original` directories, active builds, busy signatures, and files used within the stale-file interval. If the repository lookup fails, eviction is deferred. Before discovery completes, the original random cleanup remains available so a nearly full node is not blocked by warm-up.

## Status and dirty database queue

This journal does not replace `ArbitraryResourcesCache.status` and does not change the durable `ArbitraryResourceDirty` queue. Those database paths retain their current behavior. The journal is limited to finding locally hosted payloads efficiently for cleanup and hosted-data listing.

## Verification

Focused tests cover journal restart, dirty-event coalescing, presence and absence updates, discovery completion, pending work after restart, truncated-journal recovery, candidate fairness, and lease exclusion. Existing arbitrary-file, storage-policy, capacity, dirty-queue, and bootstrap tests cover the surrounding behavior.
