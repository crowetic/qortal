# Experimental data optimization verification — 2026-11-21

Runtime commit: `7477a31a00ae99becae7405dd07a5ac0874618cb`, Core
`6.1.9-7477a31`, schema **57**, branch `optimize-data-Nov-2027`.
Base: `qdn-performance` `d88d0f8d6`; integrated performance database tip
`07b7edd95` and Phil core optimization tip `78c9058ea`. All three source histories
are reachable from this branch. Existing testing branch/artifacts are separate.

The experimental artifact is
`target/qortal-optimize-data-Nov-2027-7477a31a0-experimental.jar` in the isolated
worktree. It is 95,357,076 bytes; SHA-256:

```text
f010723705e1d855d68c8c474a0648ea282231a1059ed2a8c94f25ff3483c555
```

A companion `.checksums.txt` and `.build.json` record hashes and validation.
All 1,149 packaged `org.qortal` classes use Java 11 bytecode (major 55). The
ARRR JNI 0.3.11 publication pin remains unchanged and no Pirate native library
is bundled. Five critical wallet/recovery class entries are byte-identical to the
prior `d0398d13a` testing JAR.

## Results

- **Actual shaded JAR:** 769 checks across the checked-in 88-class selection,
  zero failures. Ten assumptions: nine existing witness/coin combinations and
  the explicitly opt-in external bootstrap-host health check. Production code
  origins are asserted against the actual experimental JAR, including the new
  AT store, QDN repositories, readers, queues, render endpoints and wallets.
- **Source snapshot:** 768 checks, zero failures/errors, same ten assumptions.
  The additional retry-budget regression and final retry/interrupt changes are
  covered by the 769-check packaged run.
- **Migration:** the actual version-55 schema with nonempty AT payloads and
  pending acknowledged funding upgrades/reopens at 57 without losing the payload,
  funding ID, reserve, unknown output index or receiving data. Existing migration
  interruption, cache-revision, reader ownership and relay tests also pass.
- **HTTP contract:** HTML, binary, empty-root and error responses pass through
  real Jetty/Jersey. Basic requests also pass on the old JAR: these checks confirm
  retained wire behavior, not reproduction or causal proof of the intermittent
  production `MessageBodyWriter` error. Endpoints now avoid returning a servlet
  response as a Jersey entity in all cases.
- **Abrupt exits:** 21 child-process crash/recovery cases passed (fresh initialization,
  active/frozen AT processing and orphaning, including before/after commit), with
  exact expected ledger fingerprints after reopening. These disposable fixtures
  explicitly enable synchronous redo logging. This is not a power-loss durability
  guarantee for Core's normal checkpoint runtime mode.
- **Synthetic file-backed soak:** 1,000 new blocks with 168 frozen ATs completed
  at height 1170 using the actual JAR and normal checkpoint runtime mode.
- **Synthetic QDN query:** 100,000 rows, identical 100 ordered signatures;
  warm medians 417.282 ms before versus 1.227 ms after indexing/hinting. Index
  creation took 3.653 s. Other qualification processes ran concurrently, so these
  are illustrative local query measurements, not isolated throughput claims or
  production gateway speedups.

The initial broad run exposed relay fixtures that lacked the newly negotiated
optional-address capability and an external bootstrap-host HEAD response smaller
than its health threshold. Relay fixtures now advertise the capability; protocol
compatibility tests remain enabled. External host health requires
`-Dqortal.test.bootstrapHosts=true` with bounded network timeouts. Repository and
bootstrap behavior tests remain in the ordinary selection.

An earlier offline probe/focused test attempt encountered missing classes because
concurrent Maven rebuilds replaced the classes it was using. Those attempts are
excluded from the results above. Final qualification uses an immutable JAR and a
frozen copy of test classes/resources. Raw gateway logs/dumps and detailed local
receipts remain in the owner's private workstation report directory, outside Git.

## Acceptance boundary

No production rollout, service restart or database migration was performed by
this task. The separate qortal.link restart observed during diagnostics installed
the existing `d0398d1` testing release, not this experimental build.

Historical mainnet AT replay, production-sized migration space/time, funded ARRR
completion/refund and sustained real gateway concurrency/uptime remain release
acceptance gates. The persistent gateway symptom is not established resolved.
Automatic Core replacement is disabled for this incompatible experimental format.
Use [the isolated-instance testing and recovery guide](optimize-data-Nov-2027.md);
an older JAR cannot safely reopen its migrated repository.
