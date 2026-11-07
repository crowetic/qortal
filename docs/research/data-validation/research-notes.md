# Research notes and unfinished leads

Date: 2026-11-07. These notes intentionally retain partial findings and uncertainties. No runtime fixes were made.

## Repository and working state

The supplied workspace was a clean linked worktree of `/home/crowetic/git-repos/crowetic/qortal`, initially on its `bridge/ripper-linux-job-…` branch at `af48d0a3705f1b4e3c74655a52694581c6942abd`. The user's requested `data-validation-research` branch was created from that exact supplied tip. The owner explicitly requested this task branch, so no `main` branch was invented and no unrelated source history was integrated.

`origin` is the owner's `crowetic/qortal` GitHub fork; `qortal-upstream` is `Qortal/qortal`. Live remote-ref checks confirmed the base exists as `origin/pirate-changes` and that both public `master` refs point to `108bf191d42d710ec617f535af30cfd82fc03c87`. The requested research branch was absent from the owner remote at the initial check.

No on-disk ancestor `AGENTS.md` was found in the workspace/canonical-checkout ancestry. The session's provided instructions were followed. No graph output, credentials, generated binary or pre-existing local edit belongs in the research commit. The checked-in workflow runs Maven tests on pushes to `master`; no workflow in this snapshot deploys a research-branch push. Server-side automation outside tracked configuration was not independently inventoried.

## Research path

1. Resumed Context-MCP, inspected the branch/remotes/worktree, created a persisted research plan and built the Core graph.
2. Queried the graph for following, storage policy, transaction commitments and transfer handlers; then read exact relevant implementations. Core graph: 14,066 nodes and 17,778 edges. Its predominantly inferred edges were treated cautiously.
3. Located clean local `qortal-docs` and `qortal-scripts` checkouts and recorded their revisions. Initial graph queries failed because no graph existed. Built navigation graphs: docs 159 nodes/3 edges; scripts 25 nodes/0 edges. Selected documentation and script searches then supplied exact evidence. Generated graph directories in those related repositories were moved to temporary research storage so those checkouts remained clean.
4. Searched the web for QDN following, proof of data possession, integrity verification, content authentication, original PDP/PoR work and primary documentation for IPFS, Filecoin, Storj, Sia, LOCKSS, ZK and attestation.
5. Separated current behavior, mathematical examples, proposed extensions and open investigations. Original-paper PDF access failed for some endpoints; the source register records alternatives and limited-access leads.

## Findings worth preserving

- Core follows are local prefix-matched lists. They do not establish an on-chain social graph, a wallet-to-node binding or a public follower count.
- Following includes storage intent, but free space, policy, disabled features and delayed fetching can prevent possession.
- The follower's query name is misleading: it returns ordered publications and a tuple-based wrapper selects the first version per resource during a scan.
- A QDN transport digest is different from a digest of rendered plaintext or an extracted directory.
- Current QDN transaction metadata contains a flat chunk list, not the audit Merkle tree the concept might suggest.
- A publisher-authorized root still needs a verified relationship to the QDN transport digest when publisher/holder collusion matters.
- An account signature, Merkle path or ZK proof cannot by itself establish physical disk location, multiple independent replicas or continuous historical storage.
- Node identity and publication transaction `signature` fields must not be confused with a response signed by a delegated follower node.

## Source caveats requiring separate investigation

### Incoming hash validation may be self-consistency only

In the inspected `ArbitraryDataFileMessage.fromByteBuffer`, the incoming file is constructed from received bytes. That `ArbitraryDataFile` constructor derives its own hash from those bytes. `ArbitraryDataFileManager.receivedArbitraryDataFile` obtains `hash` from that object and calls `adf.validateHash(hash)` in relay/save branches. By itself this recomputes/checks a hash derived from the same data, rather than demonstrating equality with an independently requested chunk hash. [Q11](sources.md#q11), [Q14](sources.md#q14)

This is a source-level limitation of that specific check. It is **not** a completed analysis of all request correlation, scheduling, storage admission and later reconstruction paths, and this report does not claim a demonstrated exploit. The reader has an independent joined-payload check, and a cached-chunk reuse path in `ArbitraryDataFileRequestThread` passes the requested `fileHashBytes` to `validateHash`.

Follow-up: trace every message/request identifier and expected-hash binding under intentionally wrong chunks, wrong signatures, unexpected chunks and relay responses. A new auditor must compare received bytes to its own transaction-authenticated expectation; it must not accept self-consistency as publication integrity.

### Array equality in a repository fallback

`HSQLDBArbitraryRepository.fetchData()` joins chunks and uses `digest.equals(arbitraryDataFile.digest())` where `digest` is a `byte[]`. Java array `equals` compares references, so this check appears unsuitable for content equality and may reject a valid joined payload. The main reader uses `Arrays.equals` in its digest comparison. [Q12](sources.md#q12), [Q13](sources.md#q13)

No call-path/reproduction test was performed. Do not silently depend on this fallback as an audit primitive. Reproduce and fix it as a separate implementation task if relevant.

### Existence and digest caches are not fresh scrubs

`allFilesExist()` and local-resource status use existence/count/cache information. `digest()` can return a cached digest, with an invalidation method available elsewhere in the class. A fresh disk-integrity audit should stream bytes under a stable snapshot/read policy and independently verify commitments. This is a distinction between operational caching and audit evidence, not proof that cached state is always incorrect. [Q11](sources.md#q11), [Q13](sources.md#q13)

### Patch and directory digest assumptions

Patch support remains in types/metadata/combiner code, but the active builder rejects `PATCH`. Establish deployed version behavior before defining a historical recovery obligation. [Q15](sources.md#q15)

`ArbitraryDataDigest.compute()` sorts paths and hashes path bytes followed by content bytes without explicit path/content length framing. Its own platform-normalization and `.qortal/` exclusion behavior should be checked with cross-platform fixtures before being reused as a canonical audit format. A new manifest should explicitly frame paths, lengths and file boundaries. This report does not assert a demonstrated collision exploit.

## Questions deliberately left open

| Question | Why it matters | Next investigation |
| --- | --- | --- |
| Which Hub/mobile/Q-App subscriptions write Core lists versus app-local state? | Determines the intended “follower” population | Pin target interface versions and trace UI/network actions |
| Which raw immutable transport retrieval route should an auditor use? | Rendered APIs can fetch or transform content | Small companion endpoint versus capability-negotiated Core message |
| What is the existing wallet-supported arbitrary-message signing path? | Needed for unnamed accounts/delegations | Review actual interface permissions and signing UX; do not assume an API |
| Is the goal serving health, archival recovery or independent storage credit? | Changes required cryptography dramatically | Run the baseline pilot before specifying incentives |
| Which scopes and freshness policies match actual use? | “Everything under a name” can grow without bound | Inventory representative publisher namespaces and set explicit quotas |
| How expensive are tree bootstrapping and audits on small devices? | Qortal targets commodity hardware | Cold/warm device benchmarks and disk-read accounting |
| What finality/checkpoint policy is appropriate? | Reorgs and transfers affect authority/version claims | Align with existing Qortal transaction acceptance policy |
| Can QDN metadata gain optional audit roots compatibly? | Old parsers and publishers must remain usable | Serialization/parser interoperability fixtures and root-link validation |
| How should private follows and ciphertext be handled? | Public evidence can reveal relationships or content | Opt-in scopes and confidential audits before public proofs |
| Which PoR/PoRep library and scheme are suitable? | Papers are not vetted Java dependencies | Separate implementation survey, licensing and cryptographic review |

## Validation and delivery scope

Validation for this branch covers source-claim review, internal links and source paths, reproducible arithmetic, finite-population cross-checks, Markdown structure and a focused Git diff. It does not establish measured audit performance, a formal security proof or compatibility with every Core/interface release. Core runtime tests are unnecessary for documentation and arithmetic-only changes; no application behavior is changed.

The arithmetic helper is an illustrative research artifact. It does not generate audit indices, handle keys, issue network requests or verify a storage proof. Implementation and adversarial tests described in the roadmap remain future work.

No deployment is needed. Review/merge the research documents through the repository's chosen workflow. A normal revert removes this documentation change if desired; no service or data rollback is involved.
