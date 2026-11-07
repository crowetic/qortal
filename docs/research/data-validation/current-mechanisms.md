# Current Qortal mechanisms

## 1. Following is a local storage preference

**Observed:** Core stores resource lists as JSON files under its configured lists directory. `ListUtils.followedNames()` gathers entries from every list whose name begins with `followedNames`; `isFollowingName()` searches those lists without case sensitivity. This includes the unsuffixed list and any matching suffixed lists. A single exact `/lists/followedNames` query need not represent the complete aggregate used by Core. [Q01](sources.md#q01)

The list API offers `GET`, `POST` and `DELETE /lists/{listName}` behind `Security.checkApiCallAllowed`. Adding a name saves a list; it does not create a following transaction, obtain a signature from the publisher, verify storage, or reserve disk capacity. The Q-App documentation exposes generic `GET_LIST_ITEMS`, `ADD_LIST_ITEMS` and `DELETE_LIST_ITEM` actions with user approval. [Q02](sources.md#q02)

For example, adding `ExampleCompany` to a following list means that this Core node's aggregate following policy recognizes that name. It does not establish that wallet A follows wallet B on a network-wide registry. Core may be shared by users or used remotely. A wallet identity, a local node, a hosted node account and a public network peer are distinct objects.

**Deduction:** “Prove a follower has the data” requires a new explicit association among an account, a serving node and a particular storage scope. An app's social subscriptions may also differ from Core lists. This research verifies the Core mechanism; it does not establish uniform subscription semantics across every Q-App, Hub release or mobile interface.

## 2. How following influences storage

**Observed:** `Settings` defaults to `FOLLOWED_OR_VIEWED`. `ArbitraryDataStorageManager` defines these policies: [Q03](sources.md#q03)

| Policy | Automatic prefetch, subject to other checks | Data permitted by `canStoreData()` |
| --- | --- | --- |
| `FOLLOWED_OR_VIEWED` | Followed names | Nonblocked data of enabled types |
| `FOLLOWED` | Followed names | Followed names of enabled types |
| `VIEWED` | No automatic prefetch through this policy decision | Nonblocked data of enabled types |
| `ALL` | Named and unnamed eligible data | Nonblocked data of enabled types |
| `NONE` | No | No |

This table describes code policy decisions, not a guarantee that every eligible resource gets fetched. The prefetch path rejects on-chain `RAW_DATA`, blocked names, disallowed public/private data and insufficient capacity. It uses an 80% capacity threshold; cleanup has a 90% threshold and can remove files, with original uploads receiving special protection. `canStoreData()` deliberately does not enforce the capacity threshold and its branches are not identical to the prefetch checks. [Q03](sources.md#q03), [Q04](sources.md#q04)

`Follower.start()` schedules a recent-publication scan with a 10-minute initial delay and a two-minute fixed delay, using a 100-block window. A second scan starts after four hours and repeats with a 24-hour fixed delay. Those are scheduling parameters, not maximum delivery latency. For each followed name it obtains publications in descending timestamp order, deduplicates by `(service, name, identifier)`, checks local availability and policy, and requests missing data through the file-list machinery. [Q05](sources.md#q05)

The repository method named `getLatestArbitraryTransactionsByName()` actually queries all matching transactions ordered by timestamp; the follower's wrapper equality collapses versions of the same resource tuple. The broader data manager also checks for a newer `PUT` before fetching older data. Consequently, following should not be described as a promise to archive every historical publication. [Q05](sources.md#q05), [Q06](sources.md#q06)

There are several ordinary reasons for a followed name to lack a complete local copy: newly enabled following, delayed scans, an offline source, transfer failure, disabled QDN, disabled data types, an incompatible storage policy, or storage pressure. Audits should distinguish these from cryptographically invalid responses.

## 3. What exactly is “the name's data”?

**Observed:** A QDN resource is addressed by `(registered name, service, optional identifier)`. A name can publish many independent resources under many services. An omitted identifier is the default resource; the writer normalizes an empty identifier and the reserved `default` value to null. Republishing a tuple updates its state. Current name ownership controls permission to publish under the name. [Q07](sources.md#q07), [Q08](sources.md#q08)

A possession claim therefore needs more information than a name:

1. Which services and identifiers are included?
2. Which transaction signatures and accepted chain checkpoint select the versions?
3. Does “all” mean all latest resources, a publisher-curated collection, every historical publication, or an explicit byte quota?
4. Does the claim cover original transport payloads, metadata, reconstructed files, or private ciphertext?
5. How quickly must a holder incorporate later updates?

A publisher-curated collection is useful, but it is not automatically a complete inventory of all publications under that name. An auditor must independently enumerate the agreed namespace if completeness is part of the claim. A manifest generated by the follower must never define its own unverified coverage denominator.

## 4. Publication authenticity and byte integrity

**Observed:** `ArbitraryTransaction.isValid()` checks that a named publisher exists and that the creator address owns it. `isSignatureValid()` verifies the transaction's signing bytes through `Crypto.verify`, which uses Ed25519. The signing transformation includes the name, identifier, service, method, compression, payload commitment, size and optional metadata hash for modern arbitrary transactions. [Q08](sources.md#q08), [Q09](sources.md#q09)

The writer performs service validation, prepares files, processes the publication, compresses, encrypts, splits and creates metadata. The transaction builder generally selects `DATA_HASH` and uses the payload digest. It can choose `RAW_DATA` for qualifying small on-chain data. [Q10](sources.md#q10)

The useful verification chain is:

```text
accepted chain / publication history
  -> valid publisher transaction and ownership at that state
     -> signed transport payload hash and length
     -> signed metadata hash, if present
        -> exact metadata bytes
           -> ordered chunk hash list
              -> actual chunk bytes
```

The metadata hash is a commitment; the off-chain metadata still has to be fetched and hashed. A list supplied by a peer is not authoritative merely because it contains plausible Base58 hashes. Hashes must ultimately connect to the validated publication. [Q09](sources.md#q09), [Q11](sources.md#q11)

`ArbitraryDataFile.CHUNK_SIZE` is 512 KiB in this snapshot. Transaction metadata serializes an ordered `chunks` array along with content metadata. It is a flat chunk list, **not an existing QDN Merkle tree**. A proposed Merkle root would be an additional commitment and format. [Q11](sources.md#q11)

`ArbitraryDataReader` compares a joined payload digest with the transaction's data hash and validates the expected size before decryption. `ArbitraryDataFile.validateHash(expectedHash)` is available to compare bytes against an independently supplied expected digest. These primitives can be reused, but their call sites need review before being treated as a complete audit implementation. [Q12](sources.md#q12)

A hash establishes equality to a commitment. A valid signature attributes a commitment to a key with publication authority at that point in chain history. Neither proves that the underlying statement is true, that the named company has a verified real-world identity, or that the serving peer retained the data before responding.

## 5. Transport bytes, rendered content and privacy

**Observed:** Payload preparation can involve compression and encryption before splitting. Reader decryption includes explicit AES/CBC/PKCS5Padding and a legacy AES fallback. Publicly retrievable data can have an on-chain secret; storage policy separately recognizes data without such a secret as private/nondecryptable. [Q10](sources.md#q10), [Q12](sources.md#q12), [Q03](sources.md#q03)

For possession audits, the simplest stable target is the **exact committed transport byte sequence** and its metadata. A rendered HTTP response may be decrypted, uncompressed, routed through an index page or reconstructed from cache. Hashing that response against the transport hash will generally be the wrong check.

Holding ciphertext is a useful storage claim without proving authorization to decrypt it. Conversely, owning a recipient key does not prove possession of the ciphertext. Do not publish private content, recipient keys, secret follows or raw audit samples as a consequence of opting into storage verification. A confidential audit channel or a reviewed zero-knowledge construction may be needed for sensitive scopes.

If an application also needs readable-content authenticity, define a separate canonical file manifest containing framed paths, lengths, hashes and publication references. Validate application schemas after decoding. Exact archive bytes can differ even when extracted files appear equivalent; transport retention and semantic equivalence should be reported separately.

## 6. Local status is useful operational evidence

**Observed:** `ArbitraryDataResource.getStatus()` checks built cache availability before reporting `READY`. Other states include `DOWNLOADED`, `DOWNLOADING`, `BUILDING`, `BUILD_FAILED`, `MISSING_DATA` and `BLOCKED`. Local chunk counts are partly derived from directory entries. `HSQLDBArbitraryRepository.isDataLocal()` delegates to complete-file/chunk existence checks for off-chain data. [Q13](sources.md#q13)

These are useful for a local dashboard and for preparing a node to accept an audit. They are not fresh signed proofs to a remote verifier. A malicious node can report any status; even an honest node's existence checks or digest caches do not substitute for rereading bytes in a disk integrity scrub. `READY` can remain useful for rendering while the original transport evidence is unavailable.

A local scrub could enumerate the agreed snapshot, stream-rehash payloads and metadata, check length and ordering, and produce a missing/corrupt inventory. This is a feasible first feature. Remotely trusting that inventory would still require trusting the reporting software or adding an external byte challenge.

## 7. Data networking and attribution

**Observed:** QDN data transfer uses `NetworkData`, distinct from blockchain networking. Existing file-list and file messages identify transactions/chunks and exchange actual bytes. List responses include discovery information such as peer address, node ID and relay/direct-connection indicators. The serving code checks permanent storage and also supports relaying/cache paths. [Q14](sources.md#q14)

This is enough infrastructure to prototype retrieval-based integrity observations. It does not automatically bind a peer's data to a subscriber wallet. A response can traverse a relay or be fetched on demand; a node ID is not by itself a verified wallet delegation. A new “local-only” request flag would be a promise by cooperative software, not remotely enforceable proof against a modified node.

A pure Q-App can manage voluntary claims and show results, but an ordinary `FETCH_QDN_RESOURCE` request asks its connected Core to obtain content. It does not prove that a particular remote follower served it. Raw immutable payload access, target attribution and authenticated node challenges need a companion service or Core protocol/API extension, even if the UI remains a Q-App.

## 8. Versions and retained patch code

**Observed:** This checkout retains `PATCH` types and `ArbitraryDataCombiner` code for previous signatures and before/after state hashes. However, the active `ArbitraryDataBuilder` fetches a single latest signature and rejects a transaction whose method is not `PUT`. `validateAllDataLayers` defaults to false. [Q15](sources.md#q15)

It would be misleading to describe the inspected implementation as always rebuilding an active patch chain. Legacy interoperability is an open investigation. If an archival scope includes patches, its manifest must include all actual reconstruction dependencies and an explicit build algorithm. A latest-`PUT` scope is much easier to define and audit first.

## 9. Related repositories and reuse boundaries

The inspected `qortal-docs` snapshot documents namespace addressing, hash-backed verification, separate v6 data networking and the distinction between publication and local cache. Its selected pages say they were last verified against v6.1.9 on 2026-09-07. Those are documentation claims, not evidence about all current deployments. [R01](sources.md#r01)

The inspected `qortal-scripts` snapshot provides transaction tooling, a public-node nginx configuration generator and a Hub package publishing helper. `tx.py` supports arbitrary transaction construction and delegates signing to Core's transaction-signing API. No follower-possession challenge protocol was found in these tracked scripts. They could help with test fixtures or publishing manifests, but no existing script can be credited as a possession verifier. Scripts that sign transactions are not a reason to put wallet keys in an audit daemon. [R02](sources.md#r02)

Source-level caveats discovered during this work, including potentially misleading hash checks, are retained in [Research notes](research-notes.md). They were not fixed or presented as confirmed network vulnerabilities by this documentation task.
