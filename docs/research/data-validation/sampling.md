# Sampling calculations and their limits

These are analytical examples, not measurements of Qortal or selected protocol parameters. The helper [sampling-calculations.py](sampling-calculations.py) uses only Python's standard library and does not contact a node, inspect user data or implement proofs.

Run from the repository root:

```bash
python3 docs/research/data-validation/sampling-calculations.py
```

## Detection of a fixed missing fraction

Suppose a prover lacks fraction `f` of equal-sized, independently selectable blocks and cannot obtain the missing blocks during the audit. With `k` independent uniform samples **with replacement**:

```text
P(pass despite missing data) = (1 - f)^k
P(detect at least one missing block) = 1 - (1 - f)^k
k for detection probability c = ceil(log(1 - c) / log(1 - f))
```

| Missing fraction | 32 samples | 64 samples | 128 samples | Samples for 99% detection |
| --- | --- | --- | --- | --- |
| 50% | 100.0000% | 100.0000% | 100.0000% | 7 |
| 10% | 96.5663% | 99.8821% | 99.9999% | 44 |
| 1% | 27.5020% | 47.4404% | 72.3748% | 459 |
| 0.1% | 3.1509% | 6.2025% | 12.0203% | 4603 |

Rounded displays of 100% are not exact certainty. This table shows why 64 successful samples are strong evidence against very large fixed loss but weak evidence against a missing 1%.

For `N` blocks, `m` missing blocks, and `k` uniformly selected **distinct** indices:

```text
P(pass) = choose(N - m, k) / choose(N, k)
P(detect) = 1 - P(pass)
```

If `k > N - m`, detection is certain under the model. A single missing chunk among 512 chunks is detected with probability `64/512 = 12.5%` by 64 distinct samples. Full retrieval detects it deterministically if the endpoint must deliver the original committed bytes.

## Why a pass is not “99% sure all data exists”

These formulas condition on a specified missing fraction and fixed prover state. They are not a posterior probability that all bytes exist after a pass. A posterior would need a prior distribution over holder behavior and an observation model.

Additional assumptions include:

- The sampling distribution is correct and unknown to the holder before it fixes its retained data.
- All selected indices are verified and failures are retained, not selectively omitted.
- Missing blocks cannot be fetched, reconstructed or supplied by a cooperating node in time.
- Blocks have the intended equal weighting. Resource-uniform sampling is not byte-uniform sampling.
- The holder's missing set does not adapt to the audit. Aggregating checks across time requires assumptions about state changes.

If a holder can obtain bytes from a helper, the formulas measure inability to answer, not the fraction absent from its local disk. Repeated successes from the same shared backend do not establish independent replicas. A timeout policy also introduces an honest failure distribution unrelated to missing data.

## Bandwidth example: hypothetical 256 MiB object

Use 64 sampled openings and 32-byte SHA-256 digests. The object size is an arithmetic example, not a network resource-size guarantee.

| Representation | Number of leaves | Raw selected data | Sibling hashes without sharing | Data + siblings |
| --- | --- | --- | --- | --- |
| Existing 512 KiB QDN chunks, flat authenticated list | 512 | 32 MiB | No Merkle path required once metadata is authenticated | 32 MiB plus setup/encoding |
| Proposed 4 KiB audit blocks, binary tree | 65,536 | 256 KiB | 64 × 16 × 32 = 32 KiB | 288 KiB plus encoding |

The tree uses a power-of-two leaf count here, so every path has depth 16. A multiproof can reduce duplicated siblings. Protocol headers, signatures, transport overhead, metadata and bootstrap payload retrieval are excluded. The helper also reports the optional, different case of a Merkle tree over 512 KiB blocks; existing QDN uses a flat list instead.

If all leaf and internal hashes are stored, the 4 KiB tree has `2N - 1` hashes: 4,194,272 bytes, approximately 4 MiB or 1.5625% of the payload. Other storage layouts can omit or recompute portions. Generation requires reading/hashing the payload at least once. Memory and disk layout are implementation choices.

A 4 KiB Merkle audit's smaller response does not imply an identical reduction in disk I/O. Device block size, filesystem behavior, cache state, seek cost, read amplification and concurrency matter. These numbers do not estimate ZK or PDP proof sizes.

## Fleet budget example

Auditing 1,000 holders once per hour with the above 64-opening example yields approximately:

- Existing chunk responses: `1,000 × 32 MiB = 31.25 GiB/hour` of returned payload.
- Small-block data plus unshared paths: `1,000 × 288 KiB = 281.25 MiB/hour`.

Both exclude setup, failures/retries, observation replication and full recovery checks. They describe one audited object per holder. They do not imply coverage of every resource that holder follows.

Large-scale deployment therefore needs explicit budgets for audit frequency, object selection and full-retrieval frequency. Select parameters from observed costs and the acceptable undetected-loss model, rather than a single attractive confidence percentage.
