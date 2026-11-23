# qortal.link stall investigation — 23 November 2026

The owner-named `optimize-data-nov-2026` branch continues experimental
`optimize-data-Nov-2027` at `83b0f69a3`. It retains the irreversible **schema 57**
format and experimental auto-update guard described in
[the migration guide](optimize-data-Nov-2027.md). It is separate from
`qdn-performance`; the earlier experimental branch is preserved.

## Incident evidence

Read-only SSH verified the deployed `7477a31` JAR against its exact SHA-256.
Logs and periodic full dumps establish this sequence, in **PST (UTC−8)**:

- 22 November, 06:46:51: last successful synchronization, height **2,805,862**.
- 06:47:10: last logged synchronization attempt before the stall.
- 06:48:16–06:48:23: multiple **Java heap space OutOfMemoryError** failures.
  The configured heap maximum was **7,008 MB** (decimal, about 6.53 GiB), in a VM with **8,908 MiB RAM**.
- Both `Network-IO` and `Network-Scheduler` are present in the 06:42 dump and
  absent from the 06:52 dump and subsequent collected dumps through the next
  morning. `Qortal`, `Synchronizer`, and data-network threads remain alive.
  The synchronizer subsequently sleeps without further synchronization attempts.
- 23 November, 08:42:36: an independently initiated shutdown; startup at
  08:42:50 still reports height 2,805,862. The node catches up to 2,807,142
  by 08:44:38. This investigation did not restart it.

The pre-incident 06:42 dump contains **948 threads**, including 444 peer sender
threads and 344 disk workers. Nginx request volume increases from roughly
30–100 requests/minute to 649 and 1,132 at 06:45 and 06:46. These samples show
concurrent load and memory pressure; they do not identify the object allocation
that exhausted the heap. No historical heap dump or GC log was found, and the
installed remote runtime has no `jcmd`. No kernel OOM entry was available.

This supports a persistent networking failure following heap exhaustion, rather
than proof of a database deadlock. The precise cause of each terminated thread
cannot be reconstructed from periodic dumps alone. Raw evidence remains outside
Git in the owner's private workstation report directory.

## Changes

- Receive buffers start at **8 KiB**, grow for incomplete large frames up to the
  existing receive limit, and return to 8 KiB once empty. Small traffic reuses
  its buffer; disconnect releases the buffer. A full, incomplete frame at the
  limit raises an I/O error instead of spinning indefinitely.
- Already serialized outbound frames are limited to **4 MiB queued per peer**
  and **at most 64 MiB across peers** (or 1/16 of the JVM maximum heap, with a
  4 MiB floor). Queue admission is atomic with disconnect cleanup. Budget returns
  when a frame leaves the queue or the peer shuts down. This budget excludes
  the current output frame, the bounded preload queues, lazy messages, and other
  JVM allocations; it is not a cap on total node memory.
- The priority sender metadata queue now actually enforces its **2,000-entry**
  limit. `PriorityBlockingQueue`'s constructor size never imposed a capacity.
  Overload rejects new work without leaving a hash falsely tracked as queued.
  Admission no longer eagerly serializes lazy messages just to calculate an
  unused size; disk reads and serialization stay in the disk worker stage.
- Invalid negative wire lengths are rejected. A runtime decoder/I/O failure for
  one peer disconnects that peer without terminating the selector serving others.
  Scheduler iterations log and back off after runtime exceptions. Fatal errors
  are logged explicitly; recovery from arbitrary JVM heap exhaustion is not claimed.
- `/admin/status` adds `networkIoThreadAlive`, `networkSchedulerThreadAlive`,
  `dataNetworkIoThreadAlive`, and `dataNetworkSchedulerThreadAlive`. A dead
  networking thread also produces a critical controller log on its peer-maintenance
  cycle. Peer counts alone must not be treated as proof of network health.
- One late block-summary reply or failed initial peer response now skips only
  that item, preserving subsequent tip broadcasts and their synchronization requests.

## Testing and rollout

Run `JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 tools/test-data-optimizations.sh`.
The selection includes real socket fragmentation/growth, exact outgoing wire
bytes, per-peer/global queue pressure and budget reuse, malformed-peer isolation
on both selectors, thread liveness, and mixed late/current summary batches.
Packaged artifact results and identity are recorded separately after qualification.

More VM RAM provides headroom. The deployed `-XX:MaxRAMPercentage=75` already
gives Java about 75% of available RAM on startup. Increasing that percentage in
the existing VM would reduce room for native threads, the OS, and filesystem
cache. RAM alone does not repair failed network threads or unbounded queues.

For an operator-authorized rollout, remove one test backend from load balancing,
stop Core with that instance's existing lifecycle script, verify its Java process
has exited, install the separately named experimental JAR as that instance's
`qortal.jar` with its existing ownership, and start it using its existing options.
Verify the build/hash, four networking liveness flags, current block timestamp
and advancing height, gateway app/assets, memory and thread counts before returning
traffic. Observe sustained load and multiple cache refreshes; a short smoke test
does not establish long-term stability. A later failure needs full dumps plus
GC/allocation evidence before the process is replaced.

This follow-up adds **no database migration beyond schema 57**. For executable
recovery, use the previous verified schema-57 experimental JAR with compatible
data, or the owner's complete existing recovery mechanism. Never run the older
schema-55 `qdn-performance` JAR against the migrated database or lower its schema
version. Retain current wallet/recovery state. No live rollout, configuration
change, migration, service restart, or extra host backup was performed here.
