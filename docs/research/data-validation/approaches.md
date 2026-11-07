# Approaches and cryptographic tradeoffs

The comparisons below are architectural judgments for Qortal, not benchmark results. Definitions and external examples are supported by the primary sources linked inline. Proposed adaptations require protocol specification and review.

## 1. Choose the claim before the primitive

There are three useful assurance levels:

1. **Content authentication:** supplied bytes agree with an authorized publication.
2. **Serving or possession evidence:** an identified endpoint can produce authentic, unpredictably selected data now.
3. **Independent retention:** each credited holder maintains its own recoverable representation over time.

The first is already substantially supported by QDN. The second is a feasible addition. The third introduces storage economics, outsourcing resistance, identity multiplicity and time assumptions.

For publicly available QDN content, a prover can often fetch missing bytes from others after being challenged. Any assessment of possession must say whether cooperation and outsourcing are allowed. A cryptographically valid response does not reveal where the bytes were physically stored. A network cannot infer a hard drive or a distinct human from a wallet signature.

## 2. Comparison

“Root” in this table means an independently authenticated commitment, not a value chosen by the audited holder.

| Approach | What a pass establishes | Verifier state and cost | Qortal integration effort | Main weakness |
| --- | --- | --- | --- | --- |
| Signed statement: “I hold X” | A key signed a claim | Public key; signature verification | Low | No data-dependent evidence |
| Full direct retrieval and hash | Endpoint delivered the entire committed payload | Expected transaction hash; O(file size) traffic and hashing | Low to moderate | Expensive; proxying remains possible |
| Random existing chunks | Endpoint delivered sampled committed chunks | Authenticated metadata; O(samples × 512 KiB) traffic | Moderate | Large samples; no independent-replica guarantee |
| Fresh hash of selected actual bytes | Endpoint computed a data-dependent answer | Verifier needs those bytes or a separate authenticated proof | Moderate | A compact answer is not automatically verifiable |
| Merkle block openings | Selected bytes belong to an authenticated tree | Root; O(log block count) sibling hashes per opening | Moderate | Root trust and public-data outsourcing |
| Hash-chained receipts | Integrity/order of an evidence log | Trusted/checkpointed head; log verification | Low to moderate | Chains claims; does not create possession evidence |
| Homomorphic PDP | Storage evidence under the selected scheme's security game | Verification key and authenticated tags; compact aggregate proof | High | Setup, tag authenticity and model compatibility |
| PoR | Extractability from a sufficiently successful prover in a defined model | Scheme-specific metadata/key and recovery assumptions | High | Not every spot-check protocol is a PoR |
| Replica-specific encoding + periodic proofs | Costly-to-regenerate committed replicas under stated assumptions | Replica commitments, randomness and proof verifier | Very high | Sealing cost, complexity and economic assumptions |
| ZK wrapper around a precise data relation | Knowledge/correctness of the specified witness without revealing it | Commitment, public inputs, proof-system parameters | High to very high | Wrong relation can be sound but irrelevant to storage |
| Hardware attestation | Evidence about a measured implementation under hardware trust | Manufacturer roots, appraisal and freshness policy | High | Hardware dependencies; storage locality still needs a design |

## 3. Existing hashes and signatures

The lightest useful audit downloads the exact committed payload and recomputes SHA-256, or samples chunks against a transaction-authenticated metadata list. It can verify legacy publications without asking publishers to create new cryptographic tags.

Hashes and signatures answer different questions. Hash equality gives content integrity; the publication signature ties the commitment to the authorized publishing key. A follower signature on a fresh response gives attribution and replay protection when correctly bound to a challenge. It does not make the underlying data private or stop the signer asking another node for help.

For a small resource, retrieving everything is often more useful than a fancy proof: it both checks integrity and demonstrates delivery. Test this baseline before assuming proof compression is necessary.

## 4. Nonce challenge/response

A schematic data-dependent answer is:

```text
response = H(domain || challenge-id || nonce || framed(actual selected bytes))
```

The verifier can recompute it if it knows the selected bytes. A public full-file digest cannot generally be used to verify this new digest without the bytes. A keyed-hash approach with precomputed secret challenges is another possible finite-audit design, at the cost of verifier preprocessing and state. Juels–Kaliski discuss that baseline and its costs. [S02](sources.md#s02)

Two tempting constructions fail:

```text
H(nonce || published-file-hash)
sign(nonce || published-chunk-hashes)
```

Anyone with the public commitments can answer them without holding the file. The data-dependent computation must require actual bytes, authenticated openings, or a reviewed storage-proof relation.

Unpredictable challenges prevent precomputing a small fixed response set. They do not prevent forwarding the challenge to an available helper. Short deadlines can make outsourcing less convenient, but local slow disks and remote fast caches overlap in latency. No universal deadline proves local disk storage.

## 5. Merkle trees and authenticated byte sampling

A Merkle tree allows a verifier holding a root to validate an opening of one small byte block. Domain-separated leaves and internal nodes, explicit ordering and a deterministic tree shape avoid ambiguous interpretations. RFC 9162 is a useful example of a fully specified tree and inclusion-proof algorithm, although its certificate-log protocol is not itself a storage proof. [S04](sources.md#s04)

Qortal can use its current 512 KiB chunks directly, or introduce an optional audit tree with, for example, 4 KiB leaves over the exact encrypted/compressed transport payload. The small leaves reduce data returned per audit. They do not automatically reduce disk work proportionally: fragmented disks, storage APIs and checksums may read larger units.

There are two distinct root problems:

1. **Authenticity:** who commits to this root and its algorithm parameters?
2. **Correct derivation:** does this root actually describe the bytes committed by the QDN transaction?

A publisher signature answers the first but not necessarily the second. A dishonest publisher and follower can sign a root for unrelated easy data. A third-party auditor can download the original payload, verify its QDN digest and build the tree; other verifiers then trust that auditor's root derivation unless they reproduce it or verify a proof of equivalence. This is an explicit bootstrap trust choice.

One staged solution is to retain legacy chunk audits for independent verification and add optional trees for scopes whose roots have been independently rebuilt. New publishers could commit audit roots inside authenticated metadata, subject to a backward-compatible extension design. Root/transport consistency must still be checked, not assumed.

A branch path and leaf hash alone demonstrate a committed hash. For a serving audit, require the actual selected block, or a fresh proof that depends on its preimage. Allowing a public leaf hash as the complete response defeats the byte-possession goal. The tree itself may be public; retaining tree nodes alone must be insufficient to pass.

## 6. Hash chains, signed manifests and provenance

Hash chains can connect audit receipts, manifest versions or publisher edit history. They make a change to an earlier entry detectable relative to a trusted later checkpoint. They do not prevent presenting different chains to different observers; use shared checkpoints, consistency proofs or gossip to detect equivocation.

A separate provenance manifest can describe authorship keys, parent content, transformations, publication signatures and file hashes. Qortal can anchor such statements through ordinary authenticated publications. C2PA is a useful content-provenance comparison: it connects signed assertions to media, while explicitly distinguishing provenance from factual truth. [S13](sources.md#s13)

This addresses the user's authenticity and provenance use cases without requiring a storage reward system. A distributor can demonstrate delivery of authentic bytes while an application independently validates their origin history. Merely hosting a copied document does not make the host its author.

## 7. Provable data possession (PDP)

Ateniese and colleagues formalize remote possession checks using random block challenges and compact proofs, with verifier metadata much smaller than the stored file. Their constructions use authenticated algebraic tags rather than just asking for public hashes. This is the relevant research lineage for bandwidth-efficient remote checks. [S01](sources.md#s01)

For Qortal, the design questions are tag generation, who authenticates tags, tag storage, version binding, public versus private verification, and resistance to a publisher colluding with a holder. Existing Ed25519 publication signatures cannot simply be linearly aggregated into a PDP proof. A new scheme needs its own mathematics, library and reviewed encoding.

Tags could be published once for an immutable resource and reused by holders. That is attractive for popular content but also preserves shared-helper attacks: several accounts can rely on one tag-and-data service. PDP alone should not be credited as independent replicas.

## 8. Proofs of retrievability (PoR)

PoR asks for a stronger statement: sufficient success at the protocol should enable an extractor to recover the file under the specified security model. Juels–Kaliski introduced this formulation; Shacham–Waters provide compact constructions with a publicly verifiable pairing-based variant and a privately verifiable PRF-based variant. [S02](sources.md#s02), [S03](sources.md#s03)

Erasure coding is often relevant to turning successful checks into recoverability: enough intact encoded symbols can reconstruct the original even when some are missing. Coding, authentication and extraction assumptions have to be designed together. Merely adding Reed–Solomon parity to a Merkle tree is not a complete proof of a PoR security property.

This is promising for voluntary long-term archival commitments. Costs include encoding overhead, tag management, repairs, recovery testing and key lifecycle. Each immutable publication snapshot is a simpler starting object than a constantly changing name. Dynamic PDP/PoR work exists, but incremental edits are a later research track rather than a prerequisite for a versioned audit pilot. [S14](sources.md#s14)

## 9. Replication and time

Filecoin separates proof of replication from proof of spacetime and uses replica-specific encodings; its Stacked DRG design aims to make regeneration costly. Its specification also explicitly discusses challenge timing and the economics of discarding/refetching data. These are useful concepts when independent storage, rather than shared delivery, is the requirement. [S07](sources.md#s07), [S08](sources.md#s08)

For Qortal this would be a new subsystem: replica identity, sealing/encoding, commitments, future randomness, proof windows, proof verification, failure handling and perhaps incentives. A naïve transformation such as `replica = data XOR stream(account-key)` is cheaply regenerable from the original and does not force an independent stored replica.

Repeated ordinary audits provide retention evidence at sampled times. Calling them “PoSt” does not give the guarantees of a reviewed spacetime construction. Neither a replica-specific proof nor an IP address proves different owners, geography, power supplies or disks. Report administrative diversity as a separately assessed property.

Qortal's minting and transaction nonce mechanisms solve different problems. This report recommends no change to minting, block validity or rewards as part of the first storage-audit implementation.

## 10. Zero knowledge and succinct proofs

A suitable ZK relation could assert knowledge of sampled bytes and authentication paths opening to an approved root, bound to a fresh challenge and node identity, while hiding the bytes. A more costly relation could prove consistency between a transport digest and an audit root. Public inputs must include the algorithm, snapshot, challenge and commitment; unconstrained or omitted bindings can make the proof useless for the desired claim.

Groth16 demonstrates succinct pairing-based preprocessing arguments; STARK research offers a transparent alternative. Their setup, proof-size and proving-cost tradeoffs differ. Neither paper alone establishes a turnkey Qortal storage protocol or performance on its target devices. [S09](sources.md#s09), [S10](sources.md#s10)

Deduction for Qortal: use ZK only when confidentiality, proof aggregation or public verification has a measured need. Proving a SHA-256-heavy relation adds a second execution environment and implementation risk. Replacing QDN's hash with a circuit-friendly hash would change compatibility; an additional commitment instead needs a proved or independently checked link to existing data.

ZK does not inherently prove a witness was kept continuously or on a particular disk. A helper can produce the witness or proof after seeing the challenge. Hiding bytes also does not automatically hide the followed name, dataset size, timing or node identity.

## 11. Attestation, ownership and availability sampling

Hardware-backed attestation can provide evidence that a particular software configuration handled a fresh request. The RATS architecture describes evidence, verifiers and appraisal policies. Applying it to storage would still require a trusted path to data and a clear hardware threat model. [S11](sources.md#s11)

“Proof of ownership” in deduplicated storage is a neighboring research topic, concerned with showing a client has sufficient file information rather than accepting only a short hash as authority. It should not be confused with legal ownership, authorship, independent storage or continuous retention. This investigation found the original publication record but did not access its full text; no construction recommendation relies on it. [S15](sources.md#s15)

Data-availability sampling asks whether committed data can be recovered from a network, often with erasure coding and dissemination assumptions. It need not identify a specific follower as its holder. It is relevant to measuring the health of a QDN collection but solves a different attribution problem. [S12](sources.md#s12)

## 12. Lessons from deployed systems

| System/pattern | Relevant idea | Lesson for Qortal |
| --- | --- | --- |
| IPFS content addressing | CIDs authenticate a structured content graph; a CID need not equal a whole-file SHA-256 | Verify the committed representation; naming a digest does not retain content [S05](sources.md#s05) |
| IPFS pinning | Pins express retention policy and protect local content from normal garbage collection | A policy declaration needs external audits if remote observers need evidence [S06](sources.md#s06) |
| Storj audits | Auditing and containment separate transient failures from repeated refusal of a specific request | Preserve unresolved challenges and reasoned outcomes; the cited design is satellite-coordinated, not a decentralized proof recipe [S16](sources.md#s16) |
| Sia storage contracts | Contract resolution checks Merkle storage proofs within a proof window | On-chain enforcement adds economic and consensus design beyond Merkle hashing [S17](sources.md#s17) |
| LOCKSS preservation | Polling and repair maintain distributed archives | Audit findings should drive repair and fresh checks; copy counts alone are insufficient [S18](sources.md#s18) |

## 13. Recommended order

Start with local integrity inventories and externally checked delivery of immutable QDN snapshots. Add node/account delegation and repeated independently scheduled audits. Optimize with authenticated small-block trees only after validating root derivation and measuring costs. Pursue PoR for archival recovery requirements. Pursue replica-specific encoding only if the intended product must credit independent storage rather than access to a shared serving pool.

Keep provenance verification as a parallel capability using the existing transaction/name authority chain and optional richer manifests. It brings immediate value even when no subscriber has made a storage commitment.
