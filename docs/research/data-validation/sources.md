# Source register

Research/access date: **2026-11-07**. Sources are primary code, project documentation, standards or original research. Search-result crawl dates were not treated as publication dates. No papers or external documents are copied into this repository.

## Repository provenance

| Repository | Inspected revision | Location and relevance |
| --- | --- | --- |
| This Core fork | `af48d0a3705f1b4e3c74655a52694581c6942abd` | [Immutable tree](https://github.com/crowetic/qortal/tree/af48d0a3705f1b4e3c74655a52694581c6942abd); supplied workspace base, verified live `origin/pirate-changes` tip |
| Public Core upstream | `108bf191d42d710ec617f535af30cfd82fc03c87` | [Upstream tree](https://github.com/Qortal/qortal/tree/108bf191d42d710ec617f535af30cfd82fc03c87); live `master` ref checked for provenance, not separately audited file by file |
| `qortal-docs` | `4cbd2c4797cf8742fb5c8fb2fbfac56f24fc8dc5` | Clean local `main` checkout of `https://gitea.qortal.link/crowetic/qortal-docs.git` |
| `qortal-scripts` | `266f23456e2a7a87a258ee01250e5919881b62c4` | Clean local `main` checkout of `https://gitea.qortal.link/crowetic/qortal-scripts` |

Core links below are pinned to the inspected fork. Related Gitea links identify the locally inspected committed files; local Git objects were the evidence source. Those checkouts were not updated, edited or committed by this task. This report is not a deployment inventory or an audit of all upstream changes.

## Core evidence map

<a id="q01"></a>
**Q01 — Local lists.** [ListUtils.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/utils/ListUtils.java), [ResourceList.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/list/ResourceList.java), [ResourceListManager.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/list/ResourceListManager.java). See `followedNames`, `isFollowingName`, `getFilePath`, `save`, `getStringsInListsWithPrefix` and `listWithPrefixContains`.

<a id="q02"></a>
**Q02 — List API and Q-App permissions.** [ListsResource.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/api/resource/ListsResource.java), [Q-Apps.md](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/Q-Apps.md). See `/lists`, `addItemstoList`, `getItemsInList` and the documented list actions near line 809.

<a id="q03"></a>
**Q03 — Storage policy.** [ArbitraryDataStorageManager.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/controller/arbitrary/ArbitraryDataStorageManager.java), [Settings.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/settings/Settings.java). See `StoragePolicy`, `canStoreData`, `shouldPreFetchData`, `isDataTypeAllowed` and the QDN setting defaults near line 396.

<a id="q04"></a>
**Q04 — Cleanup and capacity.** [ArbitraryDataCleanupManager.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/controller/arbitrary/ArbitraryDataCleanupManager.java). See `deleteRandomFile` and `.original` protection; storage thresholds are in Q03.

<a id="q05"></a>
**Q05 — Follow-driven prefetch and deduplication.** [Follower.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/controller/arbitrary/Follower.java), [ArbitraryTransactionDataHashWrapper.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/controller/arbitrary/ArbitraryTransactionDataHashWrapper.java), [HSQLDBArbitraryRepository.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/repository/hsqldb/HSQLDBArbitraryRepository.java). See `start`, `fetch`, wrapper `equals`/`hashCode` and `getLatestArbitraryTransactionsByName` near line 361.

<a id="q06"></a>
**Q06 — Broader data fetching.** [ArbitraryDataManager.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/controller/arbitrary/ArbitraryDataManager.java). See `fetchAndProcessTransactions` and newer-`PUT` skipping near line 343.

<a id="q07"></a>
**Q07 — QDN resource semantics.** [Q-Apps.md](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/Q-Apps.md). Section 0 covers names, services, identifiers, defaults, private services and permissionless app-specific identifiers.

<a id="q08"></a>
**Q08 — Publication authority.** [ArbitraryTransaction.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/transaction/ArbitraryTransaction.java). See `isValid` ownership check near line 197 and `isSignatureValid` near line 217. Later branches distinguish fees and historical memory-PoW rules; those are not possession proofs.

<a id="q09"></a>
**Q09 — Signing commitments and cryptography.** [ArbitraryTransactionTransformer.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/transform/transaction/ArbitraryTransactionTransformer.java), [Crypto.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/crypto/Crypto.java). See `toBytesForSigningImpl`, SHA-256 digest helpers, `verify` and `sign`.

<a id="q10"></a>
**Q10 — Publication preparation.** [ArbitraryDataWriter.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataWriter.java), [ArbitraryDataTransactionBuilder.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataTransactionBuilder.java). See writer `save` pipeline and identifier normalization; builder payload/hash selection near line 187.

<a id="q11"></a>
**Q11 — Chunks and metadata.** [ArbitraryDataFile.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataFile.java), [ArbitraryDataTransactionMetadata.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/metadata/ArbitraryDataTransactionMetadata.java). See `CHUNK_SIZE`, `split`, `join`, `digest`, metadata `chunks` parsing and serialization.

<a id="q12"></a>
**Q12 — Byte verification and decoding.** [ArbitraryDataReader.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataReader.java), [ArbitraryDataFile.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataFile.java). See joined digest check near line 505, size check, decryption and `validateHash` near line 666.

<a id="q13"></a>
**Q13 — Local status and presence.** [ArbitraryDataResource.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataResource.java), [ArbitraryResourceStatus.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/data/arbitrary/ArbitraryResourceStatus.java), [ArbitraryResource.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/api/resource/ArbitraryResource.java), [HSQLDBArbitraryRepository.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/repository/hsqldb/HSQLDBArbitraryRepository.java). See `getStatus`, chunk counts, status/hosted API routes, `isDataLocal`, `fetchData` and Q11's existence helpers.

<a id="q14"></a>
**Q14 — Transfer, relay and discovery.** [NetworkData.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/network/NetworkData.java), [ArbitraryDataFileManager.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/controller/arbitrary/ArbitraryDataFileManager.java), [GetArbitraryDataFileMessage.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/network/message/GetArbitraryDataFileMessage.java), [ArbitraryDataFileMessage.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/network/message/ArbitraryDataFileMessage.java), [ArbitraryDataFileListMessage.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/network/message/ArbitraryDataFileListMessage.java). See `receivedArbitraryDataFile`, `processDataFile`, incoming message construction and discovery fields.

<a id="q15"></a>
**Q15 — Current build path and retained patch logic.** [ArbitraryDataBuilder.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataBuilder.java), [ArbitraryDataCombiner.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataCombiner.java), [ArbitraryDataDigest.java](https://github.com/crowetic/qortal/blob/af48d0a3705f1b4e3c74655a52694581c6942abd/src/main/java/org/qortal/arbitrary/ArbitraryDataDigest.java). See builder's `validateTransactions`, combiner signature/hash checks and directory digest framing. Defaults are in Q03.

## Related repository evidence

<a id="r01"></a>
**R01 — qortal-docs.** Selected files at the recorded revision: [QDN architecture](https://gitea.qortal.link/crowetic/qortal-docs/src/commit/4cbd2c4797cf8742fb5c8fb2fbfac56f24fc8dc5/docs/architecture/qdn.md), [developer architecture](https://gitea.qortal.link/crowetic/qortal-docs/src/commit/4cbd2c4797cf8742fb5c8fb2fbfac56f24fc8dc5/docs/developers/qdn/architecture.md), [data management](https://gitea.qortal.link/crowetic/qortal-docs/src/commit/4cbd2c4797cf8742fb5c8fb2fbfac56f24fc8dc5/docs/using-qortal/qdn/data-management.md), [resource identifiers](https://gitea.qortal.link/crowetic/qortal-docs/src/commit/4cbd2c4797cf8742fb5c8fb2fbfac56f24fc8dc5/docs/developers/qdn/resource-identifiers.md). Read from `/home/crowetic/git-repos/gitea/crowetic/qortal-docs`. Their front matter identifies v6.1.9 and 2026-09-07 verification.

<a id="r02"></a>
**R02 — qortal-scripts.** [README](https://gitea.qortal.link/crowetic/qortal-scripts/src/commit/266f23456e2a7a87a258ee01250e5919881b62c4/README.md), [tx.py](https://gitea.qortal.link/crowetic/qortal-scripts/src/commit/266f23456e2a7a87a258ee01250e5919881b62c4/tx.py), [nginx generator](https://gitea.qortal.link/crowetic/qortal-scripts/src/commit/266f23456e2a7a87a258ee01250e5919881b62c4/generate-nginx-node-config.sh), [Hub package publisher](https://gitea.qortal.link/crowetic/qortal-scripts/src/commit/266f23456e2a7a87a258ee01250e5919881b62c4/publish-qortal-hub-deb.sh). Read/searched from `/home/crowetic/git-repos/gitea/crowetic/qortal-scripts`; no scripts executed.

## Cryptography and other systems

<a id="s01"></a>
**S01 — PDP.** Ateniese et al., [Provable Data Possession at Untrusted Stores](https://eprint.iacr.org/2007/202), 2007. Abstract/revision record read; PDF endpoint returned 403. Also consulted the author-hosted expanded paper [Remote Data Checking Using Provable Data Possession](https://web.njit.edu/~crix/publications/acm-tissec11a.pdf), which has a different title and author list. Supports probabilistic block challenges and compact authenticated proofs.

<a id="s02"></a>
**S02 — Original PoR.** Juels and Kaliski, [PORs: Proofs of Retrievability for Large Files](https://www.arijuels.com/wp-content/uploads/2013/09/JK07.pdf), CCS 2007, author-hosted preprint. Consulted definition, extraction interpretation and keyed-hash baseline.

<a id="s03"></a>
**S03 — Compact PoR.** Shacham and Waters, [Compact Proofs of Retrievability](https://www.cs.utexas.edu/~hovav/dist/verstore.pdf), originating in ASIACRYPT 2008; author-hosted extended text. Supports public pairing-based and private PRF-based verification distinctions.

<a id="s04"></a>
**S04 — Specified Merkle trees.** [RFC 9162, Certificate Transparency Version 2.0, section 2.1](https://www.rfc-editor.org/rfc/rfc9162.html#section-2.1), 2021. Domain separation, deterministic non-power-of-two tree shape and inclusion proofs. Applied as a design pattern, not as a claim about QDN's current structure.

<a id="s05"></a>
**S05 — IPFS verification.** [Content Identifiers](https://docs.ipfs.tech/concepts/content-addressing/), IPFS documentation. Distinguishes CIDs/structured content from ordinary file checksums.

<a id="s06"></a>
**S06 — IPFS retention policy.** [Pin files](https://docs.ipfs.tech/how-to/pin-files/), IPFS documentation. Pinning and garbage collection; local and remote retention are operational choices.

<a id="s07"></a>
**S07 — Filecoin storage/time model.** [Proof-of-Storage](https://spec.filecoin.io/algorithms/pos/), Filecoin specification. Consulted challenge timing, refetch economics and time-based assurance; page identifies its own audit status.

<a id="s08"></a>
**S08 — Replica encoding.** [Stacked DRG PoRep](https://spec.filecoin.io/algorithms/sdr/), Filecoin specification. Consulted overview of replica-specific, regeneration-resistant encoding; not a full implementation/security audit.

<a id="s09"></a>
**S09 — Succinct arguments.** Groth, [On the Size of Pairing-based Non-interactive Arguments](https://eprint.iacr.org/2016/260), 2016. Original research record and abstract. Supports the succinct preprocessing SNARK comparison; no Qortal benchmark inferred.

<a id="s10"></a>
**S10 — Transparent arguments.** Ben-Sasson et al., [Scalable, transparent, and post-quantum secure computational integrity](https://eprint.iacr.org/2018/046), 2018. Original research record and abstract; basis for the STARK comparison, not evidence that Qortal's full stack is post-quantum secure.

<a id="s11"></a>
**S11 — Attestation trust model.** [RFC 9334, RATS Architecture](https://www.rfc-editor.org/rfc/rfc9334.html), 2023. Evidence, appraisal, freshness and privacy considerations. An architecture, not a disk-possession protocol.

<a id="s12"></a>
**S12 — Network data availability.** Al-Bassam, Sonnino and Buterin, [Fraud and Data Availability Proofs](https://arxiv.org/abs/1809.09044), first submitted 2018. Consulted abstract for the distinction between network availability and one holder's storage. No detailed Qortal adaptation established.

<a id="s13"></a>
**S13 — Content provenance.** C2PA [Content Credentials explainer, version 2.3](https://spec.c2pa.org/specifications/specifications/2.3/explainer/Explainer.html). Consulted provenance/identity/truth distinctions. Version pinned deliberately; no claim that 2.3 is the newest specification.

<a id="s14"></a>
**S14 — Dynamic variants.** Erway et al., [Dynamic Provable Data Possession](https://eprint.iacr.org/2008/432), ePrint record 2008, later revised/published work. Establishes a research direction for updates; no scheme selected or ported here.

<a id="s15"></a>
**S15 — Neighboring ownership concept, partial access.** Halevi, Harnik, Pinkas and Shulman-Peleg, [Proofs of Ownership in Remote Storage Systems](https://doi.org/10.1145/2046707.2046765), CCS 2011. Publication record/search excerpt found; publisher full text inaccessible via the research tool. Retained as a follow-up lead, not an implementation authority.

<a id="s16"></a>
**S16 — Operational audit failure policy.** Storj, [Auditing and Node Containment](https://github.com/storj/design-docs/blob/main/20190909-auditing-and-node-containment.md), design document dated 2019-09-09. A historical design, not a claim that every current Storj deployment follows it unchanged.

<a id="s17"></a>
**S17 — Contract-based storage proofs.** Sia, [Consensus Validation](https://sia.tech/learn/validation). Consulted file contract/proof-window/Merkle validation rules. No proposed reuse of Sia's consensus inside Qortal.

<a id="s18"></a>
**S18 — Preservation and repair.** LOCKSS, [Frequently Asked Questions](https://www.lockss.org/about/frequently-asked-questions), especially integrity polling/repair. Useful preservation-system comparison, not a Sybil-resistant open-network storage proof.

## Method and attribution

The code graph was consulted before implementation reads. Graph results were treated as navigation hints, not semantic proof: the generated Core report marked most edges inferred. Exact claims were checked in source. External sources were found using web searches for proof of data possession, integrity verification, content authentication, QDN following, Merkle audit patterns and the named primary constructions.

Recommendations, proposed encodings, failure scenarios and numerical examples are this report's analysis. They are not claimed to be existing protocol features or security theorems. A formal scheme's published guarantees apply only with its own definitions, assumptions and correct implementation.
