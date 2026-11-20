# AT deployment approval and an eventual XMR activation

Reviewed 2026-11-20 from `qdn-performance` at `09bc1a393`, with development on
`feat/xmr-trading`. This audit and its tests do not change consensus or approve
another AT.

## The current restriction

`DeployAtTransaction.isValid()` parses the CIYAM creation bytes, obtains the code
segment, computes SHA-256 and Base58-encodes the digest. For a candidate deployment
height **strictly greater than** `BlockChain.getAtValidateHeight()`, the digest
must appear in its private static `APPROVED_CODE_HASHES` list. Otherwise validation
returns `INVALID_CREATION_BYTES`.

The standard chain configuration in `src/main/resources/blockchain.json` sets
`featureTriggers.atValidateHeight` to **2,521,500**. Thus the hash check starts at
candidate block **2,521,501**. The candidate height is the repository's current
blockchain height plus one, not the deployment's timestamp. The same check is
called during block transaction validation (`Block.areTransactionsValid`) as well
as unconfirmed-transaction validation. It is a consensus rule, not an API-only
restriction.

There is no on-chain registration transaction, group vote, name ownership or
administrative endpoint that adds a hash to this list. Approval is part of Core's
source. A label such as `MoneroACCTv1` cannot authorize different code. Conversely,
changing the offer's data segment, amounts or addresses does not itself change the
code-segment hash. Those inputs still need the contract's own validation.

Other deployment limits remain in force: v2+ CIYAM creation format, code of at
most 1,024 bytes and serialized state of at most 1,024 bytes. The transaction wire
decoder separately bounds creation bytes to 4,096 bytes. Metadata, assets, funding,
fees, signatures and references retain their existing checks. A hash entry does
not waive any of those conditions.

## Approved code in this branch

The labels below come from the comments in `DeployAtTransaction`; the tests also
compile and validate the six current v3 trading programs against the actual gate.

| Program | Approved Base58 SHA-256 of code |
| --- | --- |
| Q-Fund v1, refund | `9gS2L74FdaG3zuEeYv815xVyHkhvLguq7ZGD6pf24i8F` |
| Q-Fund v2, no refund | `HaqJBVVr9gZqgARZ5UZd7EU9ybyvVK2fCo9sx3gMMFsr` |
| Lottery | `GA2GF79hfJeTy1TezS4sUZWS4vxJKs4qqwJ49h3NN8H9` |
| LTC | `4KdJETRAdymE7dodDmJbf5d9L1bp4g5Nxky8m47TBkvA` |
| BTC | `7xmoVpA6aR7cm7E9XZXAz3BRMQufdkTiku5CGxouFjcT` |
| DOGE | `FTe4QDjNre2vRrGJQLKgfNdVYahvbd8CnVdhir9Zk7D` |
| DGB | `GXPEHes39dxiasWDSAZXyhGjaT44LKvZ7cJnHYgBYtng` |
| RVN | `Amtrk89znaGrHqsRqtEhEKQZzKfbWAHdetB5SSN8cYL6` |
| ARRR | `HyKCg5oiwMKkRrZKk1e5gET27kfrEVUwCNXGQALXqg9x` |
| Escrow v1-lite | `5xwLAjTo1RyKLQS5gM1TvUvDR53H2ARCGyNwyyGcDp4q` |

`SupportedBlockchain` is a separate registry of chain names, ACCT implementations
and code hashes used for discovery, parsing and trade-bot dispatch. It recognizes
some historical v1 programs which are not approved for new deployments after the
hash restriction activates. Existing contracts are not redeployed on every block;
this deployment gate does not stop historical AT execution. A new XMR hash needs
both consensus admission and the appropriate versioned parser/dispatcher, plus a
real protocol implementation. Wallet network selection does not add either.

## How to add the future XMR AT safely

1. Freeze a reviewed protocol with executable claim and recovery evidence. Compile
   its new `MoneroACCTv1` deterministically; freeze code bytes, hash, data layout,
   cost/size budget and test vectors. Existing contract hashes must remain intact.
2. Prototype on an isolated Qortal development chain with worthless Monero coins.
   Use an explicit test-chain fixture for admission; never weaken the standard
   mainnet list or treat Qortal-mainnet/Monero-stagenet wallet mode as an AT test
   network.
3. Add an explicit **per-new-code activation height** in a coordinated consensus
   release. Appending the hash to today's list would immediately accept it at
   every height past the old global trigger. It does not provide a future XMR
   activation boundary. Before the new boundary, nodes with the new software must
   still reject the new program; at/after it they must accept the frozen program.
4. If the protocol needs a VM function, coordinate its deterministic validation,
   metered cost and execution activation as well. Deployed AT code is public:
   neither its code nor data can keep a private spend share secret. An AT cannot
   call a daemon to establish foreign payment truth during consensus execution.
5. Test old and new nodes at the activation boundary, pre-activation admission,
   fork/reorg/orphan paths, arbitrary hashes, existing ATs and replay from genesis.
   Older nodes retain their fixed list and can reject blocks containing the new
   AT. This needs a network upgrade, rather than just a private test JAR.
6. Register trading APIs/bots only after the protocol and activation plan pass.
   New persistent schemas belong on `performance-db-improvements`, per the owner's
   branch policy, with explicit backup and downgrade/recovery behavior. This
   branch introduces no migration.

The test-chain fixture currently sets a very large historical trigger, and
`getAtValidateHeight()` narrows it to `int`. The new characterization tests set a
small explicit trigger in memory and restore it after each test, so they actually
exercise the boundary instead of inheriting a far-future test value. No live
settings or chain configuration is modified.

## Reproduce the admission tests

```bash
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
mvn -o -B -Dmaven.gitcommitid.nativegit=true -DskipJUnitTests=false \
  -Dtest=DeployAtApprovalTests test
```

The tests use the in-memory test repository and actual `DeployAtTransaction`
validation. They do not deploy to a running node, add a hash or broadcast a
transaction. See [the XMR research plan](monero-crosschain-trades.md) and
[the offline model](../tools/xmr-trading/README.md) for the next protocol gate.
