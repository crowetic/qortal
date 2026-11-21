# Experimental Core/QDN data optimizations

`optimize-data-Nov-2027` is an isolated development/testing branch, named as
requested. It starts at `qdn-performance` `d88d0f8d6` and integrates
`performance-db-improvements` `07b7edd95` and Phil's `core-optimizations`
`78c9058ea`. Existing testing branches and their JAR aliases are unchanged.
This guide supersedes the migration/rollback instructions in the parent-branch
performance documents **for this branch only**.

## Data format and isolation

The final schema is **57**. The existing wallet migrations 53→54 and 54→55 retain
their meaning. Migration 55→56 adds the resource/service, zero-balance and global
QDN timestamp indexes. Migration 56→57 installs Phil's resumable AT storage
migration, indexed normalized QDN transaction keys and durable resource-work
queue. A single chronological index is shared by the startup and recent-QDN
queries. Existing performance-parent indexes and interrupted DDL are accepted.
Startup also repairs missing funding metadata from older conflicting schemas.

The AT migration replaces the legacy state payload storage with checksummed
block bundles/current-state records and removes the old payload table after
validation. **An older JAR cannot safely run against the migrated repository.**
Never lower `DatabaseInfo.version`, reuse this repository with an existing test
release, or restore old financial records over current wallet/recovery state.
Automatic Core updates are disabled in this experimental build even if settings
request them, so an unreviewed release cannot replace its incompatible executable.
Remove that source guard only when the format has been release-qualified.

## Integrated behavior

- Ordered/indexed recent and oldest QDN reads avoid hydrating/sorting full history.
  Search candidates are filtered before metadata joins; small cached pages use
  bounded selection, hash-based name membership and one projection copy.
- Resource cache work is coalesced by resource and persisted with revisions,
  retry scheduling and independent commits. Successful processing cannot erase a
  newer notification. Phil's durable queue replaces the earlier signature-only
  notification loop; the local-content journal replaces whole-history cleanup.
- Build workers claim work without repository I/O under the queue monitor; shared
  reader futures preserve ownership and prevent concurrent partial-file reads.
  Request waits are interruptible and have a 60-second queued/join/retry budget.
  An already executing owner load retains its I/O/query timeouts. A waiting request
  does not cancel another caller's build or delete its output. Synchronous gateway
  waits return a retryable 503 on expiration; raw-download repository failures
  retain a repository error instead of becoming a misleading missing-file error.
- Gateway, domain-map, restricted-render and developer-proxy handlers finish their
  servlet streams without returning a servlet response as a Jersey entity.
  Binary render responses set the long content length before streaming.
- Phil's AT execution/storage, archive reads, fork recovery block batching,
  allocation reductions, concurrent searches, WebSocket serialization and
  background UPnP mapping are retained. Current wallet recovery and per-request
  QDN relay ownership/capability protections are preserved during conflict resolution.

## Gateway evidence, 21 November 2026

Read-only SSH inspection used retained `qlink` and `qname` aliases. Initially both
nodes ran `6.1.9-b109da5`, were synchronized and had about 30 hours uptime. Three
recent periodic full dumps and current log tails were collected per node into
owner-private workstation reports; raw logs, addresses and settings are not in Git.

Observed evidence:

- qortal.link logged multiple Jersey `MessageBodyWriter` errors for Jetty servlet
  responses with `text/html` and `text/plain`. Real HTTP wire-contract tests exercise HTML, binary, empty and error bodies.
  Those basic requests also pass on the prior JAR; they verify preserved behavior,
  not reproduction or root-cause proof of the logged intermittent failure.
- qortal.name logged actual status-UPDATE transaction serialization failures at
  18:56:40 and 18:59:48 UTC, including rollback on repository close. Its dumps show
  cache-manager status/latest-resource work in HSQLDB's disk cache. The integrated
  status reconciliation/indexing and durable queue target this contention.
- The initial samples had 80/35 GiB filesystem headroom and approximately
  1.6/1.4 GiB swap used on link/name respectively. Hundreds of peer sender/disk-I/O
  threads were present, mostly waiting. These observations do not prove an OOM,
  thread leak or build-worker deadlock; sampled normal build workers were sleeping.
- A public Q-Share read returned a loading page on link and a ready page/status on
  name. Link's subsequent 502 coincided with a separate restart into `d0398d1` at
  21:22:23 UTC; Nginx recorded connection refusal during startup. That 502 is not
  evidence of the original long-uptime failure. The same startup logged a 31.161 s
  oldest-QDN query, addressed by the integrated indexed startup check.

No production source/configuration, wallet operations, database migrations or
service changes were performed by this investigation. Ordinary GET/status probes
can perform Core's normal derived-cache bookkeeping. No permanent cure for the long-uptime symptom is claimed;
controlled sustained gateway testing is still required.

## Qualification

Use OpenJDK 11. Tests are explicitly enabled because the POM otherwise skips them:

```sh
JAVA_HOME=/path/to/openjdk-11 tools/test-data-optimizations.sh
JAVA_HOME=/path/to/openjdk-11 PATH=/path/to/openjdk-11/bin:$PATH \
  mvn -Dmaven.gitcommitid.nativegit=true -DskipJUnitTests=true package
```

The checked-in selection combines changed QDN/AT/sync/repository suites and the
previous testing release's wallet/recovery coverage. It excludes native funded
wallet acceptance suites. The external bootstrap-host HEAD health check is opt-in
with `-Dqortal.test.bootstrapHosts=true`; it failed in the initial run because an
external response did not advertise a 100 MiB bootstrap. Offline bootstrap
behavior tests remain enabled. The integrated migration fixture builds the real
version-55 schema, adds a nonempty AT payload and pending acknowledged funding,
then upgrades/reopens at 57 and checks the retained bytes/unknown output index.
Other migration tests exercise interruption, corrupt state rejection and resumable
payload conversion. See `tools/at-performance/README.md` for separate offline
replay, synthetic soak and abrupt-exit qualification. Synthetic performance and
recovery checks are not historical mainnet replay.

Measured results and artifact identity are recorded in the
[verification receipt](optimize-data-Nov-2027-verification.md).

## Owner-operated test rollout and recovery

No gateway rollout is authorized or performed by this source/build task. First
use a **separate test instance** with its own working directory, API/gateway/P2P
ports, repository, payloads, temporary cache, local-content journal and wallets.
Do not share writable paths or active trade wallets with a running node. An empty
fixture or an owner-prepared stopped/disposable repository copy is appropriate.
Use the existing PBS/VM recovery authority where applicable; no additional host
backup archives or recovery images are created by this work.

1. Stop only the dedicated test instance through its actual service/container
   lifecycle. For an unmanaged script instance use its own `stop.sh --port PORT
   --timeout 300`, and verify its process is stopped. Keep the old instance and
   existing testing JAR aliases separate.
2. Verify the experimental JAR's checksum and place it as `qortal.jar` only in the
   dedicated test directory. Retain the existing compatible native-wallet version.
   Review settings for isolated ports/paths before starting with that instance's
   service/container lifecycle or `start.sh`.
3. Allow index creation and resumable AT conversion to finish. Check logs for
   schema 57, AT-store validation and successful startup. Inspect `/admin/info`
   and `/admin/status` at the test API origin, including height and connections.
4. Test ready/uncached apps, identifiers, many simultaneous assets/downloads,
   large files, interrupted requests and build retries. Compare latency, DB query
   timings, resource queue depth, CPU/heap/swap and disk headroom under comparable
   load. Capture full dumps/logs during a failed request and correlate timestamps
   with proxy errors. Test sustained uptime, new blocks and fork/orphan behavior.
5. For recovery, stop the experimental instance. Continue with a verified compatible
   schema-57 executable/current data, discard only the disposable test fixture, or
   use the existing owner-managed complete recovery point. Switching only the JAR
   back to qdn-performance is not a recovery method after AT migration. Never roll
   pending funding, settlements or recovery records back independently.

Historical mainnet AT replay, production-sized conversion timing/space, funded
ARRR completion/refund acceptance and sustained real gateway load remain separate
release gates. This branch is not an auto-update or production release.
