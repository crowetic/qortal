# Networking follow-up package — 23 November 2026

Runtime commit: `17770879a` on **`optimize-data-nov-2026`**. Core build:
`6.1.9-1777087`. It continues experimental `83b0f69a3` / runtime `7477a31`.
`qdn-performance` remains at `d88d0f8d6`, with its original test JAR unchanged.

Artifact: `target/qortal-optimize-data-nov-2026-17770879a-experimental.jar`.
Size: **95,359,762 bytes**. SHA-256:

```text
d58584fa0e9ef134eb625ae531ab5692a40af30ca7bbd8bbd05e0f350a1b3061
```

All **1,149** packaged `org.qortal` classes use Java 11 bytecode, major 55.
All **275 repository, cross-chain, and trade-bot class entries** are byte-identical
to the prior schema-57 experimental JAR. This follow-up has no database migration
or wallet implementation change. The schema-57 format and auto-update guard remain.

The 92-class source suite completed **780 tests**, zero failures/errors and ten
existing assumptions. Final targeted checks completed **12 tests**, including
fragmented large frames, exact drained wire bytes, per-peer/global serialized
budgets, concurrent producers and disconnect, actual metadata queue capacity,
lazy message admission, both selectors' peer-failure isolation/thread liveness,
negative payload lengths, and preservation of tip updates after late replies.

Five focused checks fail against the deployed `7477a31` JAR, demonstrating
concrete behavioral differences. The incident's exact heap allocation failure
was not reproduced; these tests do not establish sustained production stability.

The first packaged run reached 781 checks with one failure: an existing AT
migration crash fixture reopened immediately after its child halted and hit
HSQLDB's normal leftover heartbeat-lock expiry window. That test now waits
11 seconds, matching the adjacent runtime-crash test, before validating the
same migration and data. Database locking is not disabled or changed.
The first attempt's log/result is retained with the verification evidence.

Final packaged qualification completed **781 tests with zero failures** and ten
existing assumptions (nine existing coin/witness cases and the opt-in external
bootstrap-host health check). It loads production classes from the immutable
shaded JAR, verifies build identity and 25 critical production class origins,
and uses frozen test classes/resources.
No Maven compilation runs concurrently against that test snapshot. An initial
harness launch rejected a prefixed expected version string before running tests;
the corrected assertion checks the JAR's actual `6.1.9-1777087` build property.
The same 12 focused checks also pass against the actual JAR with **`-Xmx256m`**
in a separate fixture directory, exercising the heap-scaled global byte budget.
This is a bounded queue regression check, not a full-node memory requirement.

The canonical checkout's `target` contains exactly two JARs: this verified
experimental build and the unchanged `qortal-qdn-arrr-xmr-d0398d13a-testing.jar`
(SHA-256 `2348e22004ee206a944e97c9fa3882e4d793e5eb2abe74fe6688030c973c4ab0`).
IDE-generated build directories were inventoried, checked for active use, and
removed again. The previous compatible schema-57 experimental artifact remains
in its earlier isolated worktree for recovery; it is not the schema-55 test JAR.

Raw SSH logs/dumps, detailed test output, artifact receipts, and local cleanup
inventories are retained in the owner's private workstation report directory
`/home/crowetic/.local/state/codex/reports/qortal-qlink-stall-20261123`.

See [the incident and operator guide](optimize-data-nov-2026.md) for evidence,
memory-budget limits, readiness checks and compatible recovery. This package
has not been deployed by this task. Sustained gateway traffic and historical
mainnet replay remain acceptance work; no claim is made that more RAM alone or
these changes eliminate every gateway failure.
