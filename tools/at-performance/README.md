# Offline AT qualification tools

These tools operate on disposable database copies. They do not start networking or modify the running core.

Compile `ATReplay.java` against the built core jar. Its directory must contain `DISPOSABLE_AT_REPLAY`, a `settings.json` whose repositoryPath points to that directory's `db`, and a consistent backup copied into `db`.

- `ATReplay <directory> 0 migrate` opens/upgrades the copy and closes it.
- `ATReplay <directory> 0 inspect` reports retained history and payload-link ranges.
- `ATReplay <directory> <count> prepare` exports the selected tail blocks and then orphans them, committing each orphan. It is destructive to the disposable copy.
- `ATReplay <directory> <count> replay` validates and reapplies the exported range through the ordinary block path, committing each block.

Keep a closed copy of the prepared parent database for each comparison. Hold JVM, workload and durability settings fixed. Historical payload retention can limit the replay depth; a requested count alone is not evidence that that many blocks were replayed.

`ATCrashProbe.java` belongs to package `org.qortal.repository.hsqldb` and requires test classes/dependencies as well as the core. It builds a small test blockchain in an explicitly marked disposable directory. Copy the clean `init` fixture for each injected boundary, require child exit 83, then run `check` in a new JVM to verify recovery. Its fixture is distinct from a production replay and cannot establish production latency.

Compile probes into a separate directory against a **frozen copy** of the built jar and test classes; do not run against `target/classes` while Maven is rebuilding it. Test fixtures require JUnit and Hamcrest on the classpath and the repository root as working directory. Use `log4j2-console.xml` and redirect each run to its own log.

`run-crashes.py <new-directory> --classpath <classpath> --log-config tools/at-performance/log4j2-console.xml` runs 21 recovery cases: abrupt exit after first initialization without clean shutdown, eight active-contract write/commit boundaries, seven frozen-reference boundaries, and five orphan boundaries. It requires exact ledger fingerprints on reopen, saves all logs, and exits on any unexpected status. A process-crash test does not simulate a power cut.

`ATSoak <marked-empty-directory> 10000` creates 168 simple frozen contracts in a real file-backed test repository and mints 10000 blocks. `resume` as a third argument adds the requested number of blocks to a previously created fixture. This deliberately narrow synthetic workload supplements historical replay.

For comparisons, use the same closed prepared database, cache limits, JVM, log settings and background load. Confirm `SET FILES LOG TRUE` and `WRITE DELAY 5`: older cores disabled redo, and their timings are not equivalent durability measurements. Record startup, GC/checkpoint overlap and storage contention separately. Do not infer 10000 historical blocks from a 10000-block synthetic run.

`rewind` resumes preparation inside the already exported range after interruption. `benchmark` validates and commits only the requested prefix of a prepared range; it explicitly omits original-tip ledger comparison because that tip has not been reached. Use full `replay` for that comparison.

## Integrated schema-57 qualification

Compile the tools with OpenJDK 11 against the experimental JAR, compiled test
classes and the Maven test dependency classpath. Hash formatting and payload
fingerprints use Java-11-compatible APIs and the current split AT store.
`ATCrashProbe` explicitly enables synchronous redo logging on its marked,
disposable fixtures before testing abrupt exits. This verifies transaction
atomicity in that mode; it is **not** a power-loss durability claim for Core's
normal checkpoint-based runtime mode, which the startup migration restores.
Do not change production logging as part of running these offline probes.
