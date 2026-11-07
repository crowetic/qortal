# Qortal data validation: follower storage and content verification

Research date: **2026-11-07**. Status: **exploratory design, no protocol implementation**.

## Finding

Qortal has useful foundations for this concept: signed publication transactions, SHA-256 payload commitments, authenticated metadata commitments, chunked data transfer, local following lists, and storage policy machinery. It does **not** currently turn a follow into a publicly verifiable promise that a particular account or machine retains the publisher's data.

The practical next step is an **opt-in audit of data served by an identified node**, tied to an immutable snapshot of the content being followed. Begin with independently verified retrievals using existing QDN chunks. Add Merkle proofs over smaller blocks if measurements justify reducing audit traffic. Keep results off consensus initially. Independent replica accounting, storage rewards, and strong claims about continuous retention require a substantially different threat model and more work.

## Read the report

| Document | Contents |
| --- | --- |
| [Current mechanisms](current-mechanisms.md) | What following means; policies and prefetch; QDN commitments; encryption, versions, status and code reuse |
| [Approaches and tradeoffs](approaches.md) | Hashes, signatures, challenge/response, Merkle trees, hash chains, PDP, PoR, PoRep, PoSt, zero knowledge, attestation and other systems |
| [Proposed design and roadmap](proposed-design.md) | Snapshot manifests, account/node binding, audit flow, security controls, compatibility, experiments and decision gates |
| [Sampling calculations](sampling.md) | Detection probabilities, bandwidth estimates, assumptions and reproducible calculations |
| [Sources](sources.md) | Commit-pinned code references, related repository evidence, papers and primary protocol documentation |
| [Research notes](research-notes.md) | Search trail, incomplete investigations, surprising source findings, open questions and delivery scope |

## What can be claimed?

| Claim | Evidence needed | Limit |
| --- | --- | --- |
| These bytes match this publication | Recompute hashes against a validated signed transaction | Says nothing about whether the content is truthful |
| This name authorized this publication | Valid transaction and name ownership at the publication's accepted chain state | A name can be sold; a key can be compromised |
| This node served selected authentic data recently | Fresh node-bound audit, byte/proof verification and measured response | May be a proxy, cache or cooperating node |
| This node can recover the entire agreed dataset | Full retrieval, or a reviewed PoR construction with its extraction assumptions | One successful sample is insufficient |
| There are several independently maintained replicas | Replica-specific evidence plus explicit economic and failure-domain assumptions | Several names or signatures do not establish independent machines |
| Data was retained throughout an interval | Repeated unpredictable audits or a suitable spacetime construction | Ordinary audits observe discrete events, not every instant |

These distinctions are central to the recommendation. The proposed initial product label is **“recently audited serving of this snapshot”**. A social follow and a storage commitment should remain separate choices.

## Evidence and boundaries

Core analysis is pinned to this fork at [`af48d0a3705f1b4e3c74655a52694581c6942abd`](https://github.com/crowetic/qortal/tree/af48d0a3705f1b4e3c74655a52694581c6942abd), the supplied workspace base and the verified remote `origin/pirate-changes` tip during research. Public upstream and fork `master` resolved to `108bf191d42d710ec617f535af30cfd82fc03c87`; this report does not assume that every deployed node runs the fork's code. Related `qortal-docs` and `qortal-scripts` snapshots are recorded in [Sources](sources.md).

Statements marked **observed** come from those sources. Proposed formats, parameter examples, cost estimates and judgments are **research recommendations or deductions**, not existing Qortal features. No production nodes were queried, no private follow lists were collected, and no services or chain state were changed.

The branch contains documentation and a standalone arithmetic helper only. No Core runtime behavior, transaction format, configuration, dependency or version changes. Reading or merging this report requires no deployment, migration or restart.
