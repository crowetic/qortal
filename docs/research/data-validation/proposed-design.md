# Proposed Qortal design and implementation roadmap

Everything in this document is a proposal. Paths, fields, messages, labels and parameter examples are not existing API contracts. Existing building blocks are linked in [Current mechanisms](current-mechanisms.md).

## 1. Initial product definition

Offer a separate **storage commitment** alongside social following. A user voluntarily commits a node to retaining an explicit snapshot or a bounded rolling collection. Show coverage and recent successful audits. Keep ordinary following available without public disclosure or a storage obligation.

The initial security claim should be:

> An endpoint authorized by this account returned authentic selected bytes for snapshot S in response to challenge C, observed by auditor A at time T.

This is meaningful for finding serving sources and assessing collection health. It does not promise independent physical storage, uninterrupted retention or future cooperation.

Use four separate components:

```text
Publisher / accepted chain state
            |
            v
  immutable snapshot manifest <---- independent enumeration / verification
            |
            v
Auditor -- fresh challenge --> delegated serving node
Auditor <-- bytes + evidence -- delegated serving node
   |
   v
signed observations and local coverage dashboard
```

The manifest defines what is being checked. Delegation defines whose endpoint answered. Hash/proof verification defines which bytes are authentic. Audit scheduling and observer policy define how much weight to give a history of observations.

## 2. Define immutable scope first

A canonical manifest should include at least:

| Field | Purpose |
| --- | --- |
| Protocol and manifest format version | Prevent different encodings or algorithms from sharing an identifier |
| Network/genesis identifier | Prevent mainnet/testnet and fork replay |
| Accepted checkpoint height and block signature/hash | Pin chain history and publication eligibility |
| Publisher name and resource-selection policy | Define scope; do not infer “all” from a display name |
| Ownership reference/authorizing account as needed | Explain who defined a collection at the checkpoint |
| Ordered resource entries | Fix service, normalized identifier and selected transaction signature |
| Transport hash, byte length and data type | Bind each entry to the actual publication representation |
| Metadata hash and required metadata bytes/reference | Authenticate chunk lists where present |
| Retention mode | Latest selected versions, explicit history, or bounded rolling set |
| Optional audit-tree algorithm and root per object | Enable smaller proofs after correct derivation is established |
| Canonical ordering and limits | Avoid duplicate entries, unstable hashes and unbounded verification |

Define `snapshotId = SHA256(canonicalManifestBytes)`. Use a documented deterministic encoding with explicit integer widths, string encodings and length framing. If JSON is the presentation format, do not hash arbitrary serializer output. Define null/default identifiers, name normalization and ordering in terms compatible with Core; simple Unicode lowercasing is not a substitute for Qortal's name rules.

A verifier independently resolves each transaction, its accepted status, its publisher authority and its commitments. It rejects duplicate or conflicting entries and independently checks namespace enumeration when the selection policy promises completeness. A manifest for one website must not be displayed as coverage of every video, image and document under that name.

Initial scope: **explicit latest-`PUT` snapshots**, covering exact off-chain transport data and required metadata. Treat `RAW_DATA` as an authenticated chain-data case that earns no separate off-chain storage credit. Investigate historical `PATCH` reconstruction before allowing it in a recoverability claim.

For rolling following, every accepted update produces a new immutable snapshot with a previous-snapshot reference and an activation/grace policy. Holders acknowledge the new target before being evaluated against it. Evidence for S1 remains evidence for S1; it must not be silently relabeled as evidence for S2.

## 3. Bind the account to a serving node

Core normally does not hold the user's wallet signing key. Generate a dedicated audit key on the node, preferably separated from other peer identities. Have the user authorize its public key through their wallet/interface. Do not import a wallet seed, reward-share key or unrelated private key into an audit worker.

A possible delegation record contains:

```text
domain: QORTAL-STORAGE-DELEGATION-v1
network-id
account-public-key / account-address
optional follower-name + ownership reference
node-audit-public-key
snapshot-id or bounded selection-policy-id
valid-from and valid-until
delegation-sequence and revocation reference
permitted audit endpoint / transport identity
```

There are two plausible authorization routes:

1. A wallet signs a domain-separated delegation message through an explicitly supported signing flow. Arbitrary-message signing support and UI permissions need investigation; this report does not assume an existing Q-App action for it.
2. The account publishes a delegation document as an ordinary QDN `JSON` resource under a name it controls. The verifier checks the publishing transaction and its committed document. This uses existing publication authority, with publication cost and update/retrieval latency. Accounts without names need the first route or another deliberately designed authorization path.

Define expiration, revocation and key rotation before exposing a public “validated follower” count. A name transfer must not grant the buyer retrospective ownership of the previous owner's delegated node. Bind authority to the relevant account and history, and reauthorize future commitments after transfer.

Multiple nodes per account are legitimate. Multiple accounts per node are also possible. Count observed endpoints, accounts and assessed independent failure domains separately. None is automatically a count of replicas.

## 4. Baseline audit without new publication formats

1. The auditor validates a snapshot and fetches its metadata independently. It hashes metadata against the signed metadata commitment and checks expected lengths and chunk order.
2. The auditor validates the node delegation and transport identity. An IP address discovered through a relay is only a route hint.
3. It generates an unpredictable nonce and a bounded random selection of object/chunk indices. It records the challenge before sending it and starts a monotonic timer.
4. The target returns the **complete selected chunk bytes**, the immutable transaction/object reference and a signature binding the response to the challenge.
5. The auditor checks sizes, indices, signatures, request correlation and `SHA256(chunkBytes)` against the independently authenticated list. A single-part object is checked against its transaction payload hash. For chunked data, metadata establishes chunk membership; an independently validated manifest or occasional full reconstruction must also check that the list describes the committed full payload.
6. It signs a receipt describing exactly what it observed, retains an unresolved outcome when necessary and later schedules a fresh independent challenge. After repair, mark recovery without erasing earlier failures.

The cryptographic transcript should bind:

```text
challenge = Encode(domain, version, network-id, snapshot-id,
                   delegation-id, target-node-key, auditor-key,
                   challenge-id, random-nonce, selected-objects-and-indices,
                   selection-policy, expiry, limits)

response-signature = Sign_node(Encode(response-domain,
                                     Hash(challenge), Hash(encoded-response)))
```

Use at least 128 bits of nonce entropy; 256 bits is a straightforward candidate. The byte encoding and algorithms need a fixed version. Include byte values or their checked digest in `encoded-response`, not just the node's success flag. Reject stale, duplicated, oversized and cross-target responses. Retain recently consumed challenge IDs within a bounded replay window.

Raw data messages already carry bytes, but existing message IDs and chunk hashes are not this signed audit protocol. A passive proof-of-concept can record ordinary retrievals as auditor observations. A third party must trust that observer's endpoint attribution; a new node-signed challenge protocol improves attribution without proving physical locality.

## 5. Avoid an audit that fetches on behalf of its subject

Rendering/status APIs can build or retrieve missing content. Calling them during a local inventory could improve the subject's storage before measuring it. A local audit mode should read only committed files already present and fail explicitly on missing files. Use immutable transaction references rather than a changing default resource URL.

Existing `setCanRequestMissingFiles(false)` is a useful cooperative implementation hook, but it does not remotely constrain a malicious node. Likewise, an HTTP response from the auditor's usual Core proves availability somewhere along that Core's retrieval path. It cannot identify a selected follower as the source.

Implement a small authenticated companion service for the first experiment, or extend Core with a capability-negotiated audit handler. Restrict it to known snapshot objects, committed hashes and bounded reads. It must never accept arbitrary filesystem paths or use the public API key as an audit credential. Keep control-plane API access private.

## 6. Optional small-block Merkle mode

A candidate object-level audit tree uses 4 KiB blocks of the exact transport payload. This is an example for benchmarking, not a selected network standard.

To avoid circular commitments, the tree context uses the already computed transport digest and size, **not** a snapshot ID or transaction signature that may itself commit the tree:

```text
context = Encode("QDN-AUDIT-TREE-v1", SHA256(payload), payloadLength, blockSize)
leaf_i  = SHA256(0x00 || context || u64be(i) || u32be(len(block_i)) || block_i)
node    = SHA256(0x01 || leftHash || rightHash)
```

Use the RFC 9162 split rule for non-power-of-two lists, with tree shape determined by the explicit leaf count. Specify the empty-object root separately, for example `SHA256(0x02 || context)`, and give empty objects no byte-retention credit. The final leaf uses its actual length; no ambiguous implicit padding. All algorithms and encodings belong to the format version. This adapts a known tree pattern; it does not claim that RFC 9162 defines these QDN leaves. [S04](sources.md#s04)

For each selected index, return the actual block and its inclusion path, bound to the fresh signed response. Check index range, exact leaf length, root, sibling order, tree size and proof length. A multiproof can reduce shared sibling hashes but requires a separately specified parser and verification algorithm.

Bootstrap root validity in one of three explicit ways:

| Method | Benefit | Cost / trust |
| --- | --- | --- |
| Verifier downloads original payload, checks QDN hash and derives tree | Independent root/transport consistency | O(payload size) initial work |
| Named auditor signs that derivation | Cheap later verification | Trust in auditor's derivation, beyond signature validity |
| Reviewed proof of equivalence between payload commitment and audit root | Independent verification with smaller communication | New cryptographic implementation and proving costs |

A publisher committing a root in metadata is useful authentication but does not remove the consistency requirement when publisher/holder collusion is in scope. A root over easy dummy bytes can otherwise pass every opening.

Existing publications remain auditable with the baseline mode. Storing a tree alongside original data is a local optimization; publishing an additional manifest through an existing QDN service can be opt-in. Adding fields to native transaction metadata needs parser compatibility testing; changing transaction serialization or consensus rules is a separate protocol proposal.

## 7. Randomness, scope weighting and schedules

For a private pilot, use an auditor CSPRNG and choose indices after a node accepts its snapshot commitment. Keep selections unknown until issue time. A holder choosing its own indices defeats the audit. A single chosen challenge success says little about the rest of a large collection.

Use two complementary policies:

- **Byte-oriented sampling:** choose blocks with known probabilities across the full snapshot. This measures missing-byte fractions under the stated model.
- **Resource coverage:** require minimum checks per resource or stratify by service/name/version. This prevents a large video from hiding the complete absence of a small but critical document.

Record the policy in every receipt. Uniform sampling of resource names, chunks and bytes gives different distributions. Fixed small/large final chunks require care when interpreting byte loss. Report counts and denominators instead of a single unqualified validation percentage.

Public or reward-bearing audit scheduling needs more: commitments fixed before future unpredictable randomness, resistance to grinding and selective publication, independent observer selection and a bounded resource budget. A future Qortal block value is not assumed to be an unbiased beacon. Block producers, reorgs and reveal withholding need analysis. A signature or VRF does not fix a seed chosen by the beneficiary.

Keep unresolved challenge history. Retrying only random new chunks lets a holder evade a known missing region. Repeating the original challenge after a generous delay can test recovery, but cannot establish that the data was present at the original deadline. Preserve the original outcome and issue a separate fresh possession check.

## 8. Outcomes and presentation

Store evidence in at least these distinct states:

| State | Meaning |
| --- | --- |
| Declared | A valid account/node commitment exists |
| Preparing / partial | Cooperative local inventory has not completed the agreed scope |
| Audited pass | Authentic selected bytes verified for a particular challenge |
| Invalid evidence | Response failed signature, commitment, bounds or format checks |
| Timeout / unreachable | Auditor did not observe timely completion; reason uncertain |
| Stale | Last sufficient observation is older than the display policy permits |
| Revoked / superseded | Delegation or snapshot no longer active |

A display might say: “64 blocks checked for snapshot abc; passed at T; auditor A; last full retrieval at U; independent replicas unknown.” A pass should expire from the current-coverage view according to an explicit policy.

Local status, auditor statements and objectively verifiable bytes/proofs have different trust. An observer's signed receipt makes its assertion attributable; it does not independently prove its wall-clock time, that the target was unreachable, or that it chose unbiased samples. Selective sharing of only successful receipts can create a false impression of reliability.

Do not slash funds or invalidate blocks based on timeout receipts. Network partitions and auditor dishonesty make nonresponse difficult to prove to third parties. Even objectively invalid data needs a designed adjudication model before economic consequences are introduced.

## 9. Threats and controls

| Threat | Initial control | Residual limit |
| --- | --- | --- |
| Replay or cross-network proof | Fresh nonce; full transcript/network/snapshot binding | Public data can still be fetched anew |
| Store only hashes or Merkle nodes | Require actual bytes or a reviewed preimage-dependent proof | Selected byte subsets can be retained |
| Store only easy resources | Independent scope enumeration and stratified/random selection | Unchecked regions remain uncertain |
| Proxy/helper answers | Authenticated target; measured latency; concurrent independent audits | No cryptographic physical-locality guarantee |
| Many identities share one copy | Separate account, endpoint and replica counts | Stronger replica encoding needed for storage credit |
| Auditor/follower collusion | Independent observers, precommitted schedule, reproduce verification | Collusion assumptions remain explicit |
| Publisher signs dummy audit root | Independently verify derivation from committed payload | Auditors relying on a derivation signer trust it |
| Name transfer or chain reorg | Pin history; expire/revalidate delegation and snapshot | Finality policy remains to be chosen |
| Audit amplification or disk exhaustion | Consent, per-key quotas, caps, bounded concurrent reads | Sybil identities can evade naïve quotas |
| Endpoint used for SSRF | No arbitrary auditor-supplied URLs; validated routes and private-network policy | Private deployments need deliberate allowlists |
| Sensitive follow graph disclosure | Opt-in scope only; confidential audit transport | Timing and metadata can still identify interests |
| Cleanup removes committed data | Explicit local reservations/retention policy for opted-in scopes | Cooperative policy is not an external proof |
| Resource growth exhausts storage | Quotas, maximum object/snapshot sizes, renegotiation and update grace | No unlimited commitment implied by a follow |

For public ciphertext, audit its original committed bytes. For genuinely sensitive data, avoid public raw openings and use authorization/confidential transport; evaluate ZK only against a specific confidentiality requirement. Rate limits should account for both bytes returned and disk reads, including invalid challenges that force expensive work.

## 10. Implementation phases and decision gates

| Phase | Work | Gate to proceed |
| --- | --- | --- |
| 0: inventory | Define scope; add read-only local scrub; surface missing/corrupt/version status | Correct results for complete, partial and intentionally damaged datasets |
| 1: serving pilot | Companion audit service, delegation, full/chunk retrievals, signed evidence | Target attribution works; replay and invalid-data cases rejected; honest-node costs measured |
| 2: repeated audits | Schedules, coverage tracking, unresolved failures, repair and snapshot updates | Acceptable false timeout rate; no misleading certainty or replica count |
| 3: compact openings | Optional small-block trees and explicit derivation verification | Proven bandwidth benefit and affordable CPU/disk/storage on target devices |
| 4: archival proofs | Evaluate reviewed PDP/PoR implementations and recovery model | Recovery tests and independent cryptographic review |
| 5: replica incentives | Decide whether distinct storage credit is needed; evaluate PoRep/time model | Economic/security design, governance and independent review before consensus changes |

Candidate Core touchpoints: lists for **local intent**, storage/cleanup managers for cooperative retention, `ArbitraryDataFile` and transaction metadata for byte commitments, `Crypto` for existing SHA-256/Ed25519 primitives, `NetworkData`/messages for optional transport, and resource status for an operational dashboard. New audit state and evidence should remain separate from ordinary `READY` semantics.

Do not reuse existing cached digests as evidence of a fresh disk read. Do not equate existing network `signature` fields, which often identify a publication transaction, with a new signature by the serving node.

## 11. Experiments required before selecting parameters

Use an isolated local fixture network, not production nodes. Include one holder with all data, a holder missing random chunks, a holder missing an entire resource, a tree-only holder, a corrupt holder, a relay and a deliberately outsourcing proxy.

Measure first-time manifest/bootstrap cost, warm/cold audit latency, bytes returned, seeks, CPU time, memory, extra tree/tag storage, and retry rate. Include a small-board device and HDD/SSD hosts. Use representative small JSON, multi-file applications and large media; include repeated publication updates and private ciphertext.

Exercise replay, reordered/duplicate indices, wrong versions, name transfer, delegation rotation, simulated reorg, truncation, invalid paths, oversized proofs, metadata loss and disk-full cleanup. Test that a missing-file audit does not start a background fetch. Calibrate deadlines from measured distributions; report any acceptance of an outsourcing proxy instead of hiding it as a successful locality test.

Check the statistical sampler independently against exact finite-population calculations. Simulate adaptive deletion and selective timeout behavior, since the simple missing-fraction formula does not cover them. Periodically retrieve complete snapshots to validate recovery and the manifest-to-payload relationship.

## 12. Compatibility and rollout boundaries

The first phases can be entirely opt-in and off consensus. A Q-App can present and publish records, but target-specific byte challenges still need a serving capability. Legacy nodes should remain eligible sources for ordinary QDN retrieval while being shown as unaudited or unsupported for the new protocol.

A future implementation should feature-gate audit handlers, bound their queues and preserve standard QDN behavior when disabled. Pilot rollout would require explicit deployment authorization, device readiness checks, monitored false-failure rates and a disable/rollback path that stops new audits without deleting content or rewriting history.

**This research branch needs no runtime rollout.** Review or merge the documents as desired; undo the documentation commit with a normal revert if needed. No restart, database migration, key change or data cleanup is required.
