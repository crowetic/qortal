# QORT / XMR trades: protocol and Core integration work

Status: research in progress; transport plan updated 2026-11-23. `feat/xmr-trading` starts from the
verified `qdn-performance` tip `09bc1a39325219d010bb059a096cc690e9169373`.
The matching Hub/Q-Wallets testing builds already support local Monero stagenet
scanning/payments, and the owner has reported a successful payment. That wallet
path supplies no cross-chain trade protocol. No trading AT, approved hash,
consensus opcode or trade-bot entry is enabled by this research increment.

## First executable milestone

- [AT admission characterization tests](../src/test/java/org/qortal/test/at/DeployAtApprovalTests.java)
  exercise the real validator before, at and after its activation boundary,
  spoofed metadata, historical recognition versus admission, all six latest
  trading programs, data-only changes and state-size enforcement.
- [An offline finite-state model](../tools/xmr-trading/README.md) generates
  counterexample traces for four incomplete protocols. Tests also establish
  reachable honest completion and early recovery paths, replay every reported
  trace and fail explicitly if exploration exceeds its bound.
- [The AT approval audit](at-deployment-approval.md) records the ten accepted
  hashes, exact enforcement boundary and a separate future activation plan.
  The [wallet roadmap](monero-support-plan.md) tracks integration milestones.

These are executable research checks, not a cryptographic implementation or a
claim that atomicity has been established. No database migration is introduced.

## Findings from the current Core

A new Monero trade protocol should have its own versioned AT and code hash. The
existing Bitcoin-family/Pirate ACCT implementations assume a compatible foreign
redemption mechanism. Registering XMR beside them does not supply that mechanism.

* `transaction/DeployAtTransaction.java` bounds code and serialized state to
  1024 bytes each. The wire decoder separately bounds creation bytes to 4096.
  When candidate block height is **greater than** `atValidateHeight` (2,521,500
  in the standard chain config), it compares Base58(SHA256(code)) with ten
  `APPROVED_CODE_HASHES`. From block **2,521,501**, unknown code is rejected as
  `INVALID_CREATION_BYTES` in transaction/block validation, regardless of labels.
* `crosschain/SupportedBlockchain.java` separately maps coin, ACCT name and exact
  code hash to implementations. `controller/tradebot/TradeBot.java` resolves those
  implementations and their persistent state handlers. A whitelist entry alone
  neither indexes nor executes an XMR trading workflow.
* The pinned CIYAM AT engine (`com.github.iceburst:AT:1b731d1`) supplies hashes,
  message handling, balances and payouts. Inspection of `FunctionCode` and
  `at/QortalFunctionCode.java` found no Edwards point/scalar verification or
  adaptor-signature primitive. Qortal transaction signature verification outside
  the AT VM is not an AT-accessible Monero verification primitive.

Adding an accepted AT hash changes consensus acceptance: older nodes can reject
blocks containing it. New VM behavior has the same coordination requirement.
An owner testing JAR cannot unilaterally activate a new protocol on Qortal mainnet.
Use an isolated Qortal test chain for the AT prototype, then arrange reviewed,
height-gated network activation and compatible node upgrades. Appending a hash to
the existing list gives no future activation boundary: it permits it immediately
past the old global trigger. A future XMR contract needs its own activation
height, with existing hashes and behavior preserved. Mainnet Qortal paired with
Monero stagenet is a wallet testing mode, not an isolated Qortal AT network.

## Why the existing HTLC cannot simply be copied

Monero lacks Bitcoin's script-based HTLC redemption path. The published
[Bitcoin/Monero protocol paper](https://arxiv.org/abs/2101.12332) uses adaptor
signatures; the [h4sh3d reference protocol](https://github.com/h4sh3d/xmr-btc-atomic-swap)
describes verifiably encrypted signatures and cross-curve proofs. Those are
primary references for the security argument, not drop-in Qortal implementations.
Qortal uses a different transaction/AT model and signature scheme from Bitcoin.

An AT that accepts a hash preimage does not prove that the preimage is the spend
share needed for the actual Monero deposit. A daemon's assertion that payment was
sent is not an atomicity proof. An ordinary `SEND_COIN` approval also does not
establish trade intent, refunds, counterparties or completion conditions.

## Roles and recovery requirements

For the first candidate transcript, call the party depositing QORT the **QORT
seller**, and the party depositing XMR the **QORT buyer**. A candidate joint-address
construction has the seller fund a Qortal escrow first, then the buyer fund a
Monero address whose spend key requires both independently verified shares.
This is a modeling convention, not a selected safe protocol. Both offer directions
must map to these economic roles explicitly; reversing a UI price does not prove
that funding order or recovery remains safe.

The model currently demonstrates:

| Incomplete rule | Counterexample |
| --- | --- |
| Buyer may claim only after the seller marks readiness; ordinary AT timeout returns QORT | Seller disappears after QORT funding, buyer funds XMR, AT refunds QORT; no unilateral Monero refund share exists. |
| Buyer may instead claim after a timeout without readiness | An unavailable seller's QORT can be claimed even when the buyer never funded XMR. A design requiring active early refund must state that monitoring assumption. |
| A claim reveals the buyer's spend share before confirmation | The claim can miss its inclusion deadline; QORT refunds while the public share still lets the seller take XMR. |
| A claim checks only an unrelated hash preimage | QORT pays out without revealing a usable spend share for the funded Monero output. |

These are deductions from the sandbox's stated transitions and abstractions,
not claims about vulnerabilities in an external implementation. A simple
two-secret escrow needs additional mechanisms or explicit availability
assumptions to address these cases. An AT has public state and cannot hide a
private refund share for later disclosure.

The next transcript must name every witness and verify its binding to the exact
trade, networks, parties, output, keys and refund destinations **before funding**.
For every funded boundary, document which already-held material allows completion
or recovery if either process or counterparty disappears. Include an encrypted
journal/backup contract for per-trade material, independent of the ordinary wallet
seed, before adding side effects. Pre-signed recovery transactions, adaptor
signatures and watchdog availability are candidates to investigate, not assumed
features of today's Hub wallet.

Choose confirmation and timeout budgets from explicit finality, inclusion,
network-delay and reorg assumptions. Budget Monero output unlock time, Qortal AT
message processing, payout/sweep fees and AT execution reserves separately.
A synchronized wallet and an unlocked output are different conditions. Model
publication of every revealing message separately from confirmation; orphaning a
transaction never makes its disclosed witness private again. No safe production
timeout values have been selected yet.

## Protocol choices to investigate before choosing bytecode

| Candidate | Required work | Main unresolved gate |
| --- | --- | --- |
| Adapt an established adaptor-signature construction to a Qortal escrow/payout transaction | Exact Qortal signing algebra, adaptor extraction, canonical scalars/points, pre-signed recovery transactions, transcript proofs | A full proof that either party can recover after every abort; Bitcoin ECDSA code cannot be reused unchanged |
| A purpose-built AT and joint Monero spend-key construction | Bind spend shares to the funded output and claim/refund paths; possibly bounded Edwards verification in the VM | Independent refund liveness: a public secret/point check alone does not create a unilateral Monero refund |

The [Athanor ETH/XMR implementation](https://github.com/athanorlabs/atomic-swap)
is a contract-based design reference, but its repository was archived on
2026-09-01. Do not adopt it as an actively maintained security dependency. Any
contract design must be re-derived for Qortal's AT timing, messages, reorgs and
cost/size limits. Prefer a narrowly scoped, reviewed primitive over embedding
large cryptographic proofs into every AT state.

These alternatives are research directions, not a claim that either is already
safe. Freeze one protocol transcript and recovery proof before implementing an
AT whitelist entry. In particular, reject a naive design in which a maker can
redeem QORT without a confirmed XMR deposit, or a failed counterparty must reveal
a secret voluntarily before the other party can recover locked XMR.

## Work packages and acceptance gates

1. **Protocol specification.** Assign QORT seller/XMR seller roles; define both
   network IDs, amounts (8-decimal QORT / 12-decimal XMR), per-trade keys, joint
   address/view-key ownership, authenticated transcript, bounded messages,
   confirmations, fee changes and timeout units. Enumerate every abort boundary,
   including one-sided funding, readiness withheld, claim/refund races, deep
   reorg, malicious subgroup points, duplicate messages and a restarted client.
   Produce a written atomicity/refund-liveness argument and independent review.
2. **Cryptographic prototype outside consensus.** Use disposable keys and local
   chains. Pin reviewed implementation dependencies and independent vectors;
   validate extraction and refund transactions under adversarial transcripts.
   Account wallet keys must never double as shared trade secrets. Ordinary Qortal
   account backups recover the wallet; any additional per-trade secret/transcript
   and pending recovery state require an explicit encrypted, recoverable design.
3. **AT/VM prototype.** Specify `MoneroACCTv1` only after step 1. Compile bytecode
   deterministically, assert the exact hash and size limits in tests. If needed,
   define a bounded, deterministic VM verification function with canonical input
   validation, subgroup checks, metered cost, invalid-input behavior and an
   activation height; test every boundary and rollback/orphan path. Do not run
   daemon RPC, wallet scanning or spend-key handling inside consensus execution.
4. **Core plumbing.** Register the frozen ACCT/code hash in `SupportedBlockchain`,
   add public trade data/API and presence handling, persistence/migrations and
   recovery semantics. Keep private signing in Hub. Review the existing Core
   trade-bot ownership assumptions before adding a Hub-owned XMR state machine.
   Keep schema/index/migration changes on `performance-db-improvements`, per the
   owner's branch policy. Define a versioned 12-decimal trade DTO rather than
   changing the shared eight-decimal adapter. Decide checked signed-long limits
   versus wider storage explicitly. Add the exact deployment hash only with
   coordinated activation.
5. **Hub/Q-App execution.** Give trades their own approval flow; bind
   every approval to account, both networks, counterparty, exact amounts, AT hash,
   escrow address, fee limits and timeout schedule. Persist encrypted transcripts
   before releasing signatures; resume after crash, support monitoring/refunds,
   and never let a Q-App receive either account private key or trade spend share.
6. **Testing and rollout.** Both trade directions, every abort/refund, unknown
   broadcast outcome, concurrent requests, malformed proofs, stale chain tips,
   reorgs and backup recovery must pass on isolated chains. Then use worthless
   test networks with independent review and a coordinated upgrade before
   considering real-value trading. Wallet-only Qortal-mainnet/stagenet testing
   does not authorize placing a real QORT trade offer.

## Next implementation queue

| Order | Deliverable | Evidence required before advancing |
| --- | --- | --- |
| 1 | Compare an adaptor-signature construction with a purpose-built AT construction | Exact signing algebra, proof dependencies, refund material, online/watchdog assumptions, bounded VM feasibility and a written decision. Reject any candidate that leaves a funded party dependent on voluntary cooperation. |
| 2 | Freeze a versioned transcript and extend the executable model | Both roles, withheld readiness, no foreign funding, delayed disclosure, reorgs, fee exhaustion and restart traces; safety/liveness claims state their assumptions. |
| 3 | Implement a local two-party cryptographic prototype | Independent positive/negative vectors for scalar/point validation, witness binding/extraction and actual completion/recovery on isolated chains. Independent cryptographic review remains required. |
| 4 | Specify `MoneroACCTv1` and any required VM primitive | Deterministic compiled hash/data layout, metered worst-case execution and size limits, admission/execution activation tests and existing-chain regressions. |
| 5 | Integrate public Core coordination with private Hub execution | Versioned capabilities/messages and crash-safe encrypted recovery, followed by Q-Trade offer/approval/recovery UI. Coordinate any schema work through the database branch. |

The next coding milestone is the transcript and local cryptographic prototype,
after selecting a construction whose recovery argument survives the modeled
failures. The final AT hash and network activation come after that evidence.
This initial branch needs no new JAR or service restart: production behavior,
the current AT list and database schema are unchanged.

## Planned Core-managed daemon transport

Owner decision, 2026-11-23: keep this work on `feat/xmr-trading` for a future
increment. This is a plan, not an implemented connector. Leave the already-tested
`qdn-performance` and matching Hub STAGENET pre-release code, configuration and
binaries unchanged. Direct STAGENET daemon access remains the current testing
path until a separate, compatible Core/Hub candidate passes the gates below.

The target connection path is:

```text
Q-App -> trusted Hub wallet/worker -> selected Qortal Core -> Monero daemon pool
```

Core owns daemon connections, candidate discovery/loading, network validation,
health checks, selection, load distribution and failover. Hub connects only to
the selected Core for XMR network operations, with no direct-daemon fallback.
Hub continues to derive keys, scan locally, prepare/sign payments, obtain
approvals and preserve encrypted caches and pending-payment journals. Core
receives public chain requests and locally signed transaction bytes, never root
seeds, private spend/view keys, wallet files or wallet-RPC signing requests.
This transport work does not enable mainnet wallets or cross-chain trading.

Implementation work packages:

1. **Transport contract.** Trace the pinned Hub worker's actual daemon calls,
   including binary scan traffic and transaction relay. Define a versioned,
   capability-advertised Core API with an explicit network and typed errors.
   Preserve exact integer and binary payloads. Allow only the required daemon
   operations; do not expose arbitrary URLs, unrestricted RPC or daemon admin
   methods. Select the authentication, hosted-node access and cancellation
   contract before implementing the adapter. A transport change must not move
   wallet cryptography into Core.
2. **Pool and discovery.** Load operator-configured seeds into separate pools for
   each supported Monero network. Define and verify a discovery source against
   the pinned daemon protocol before accepting discovered candidates; a peer
   address alone does not establish a usable RPC endpoint. Bound candidate
   counts, discovery frequency and probes. Validate endpoint, DNS and redirect
   policy, with explicitly configured local daemons supported. Q-Apps cannot
   add endpoints. Do not mix networks or silently enable a broader wallet mode.
3. **Health and load distribution.** Validate network/genesis, supported protocol,
   sync status, tip freshness and lag before serving a wallet. Track latency,
   failures and in-flight work; distribute load among healthy candidates while
   retaining a stable daemon for a bounded scan session. Apply cooldown/backoff
   to unhealthy nodes and bounded reselection after failure. Define how
   conflicting tips, all-unavailable pools and recovery are reported; never
   return an unavailable wallet as a zero balance. Health checks do not make a
   remote daemon's data trusted.
4. **Bounded forwarding and failure semantics.** Limit request/response sizes,
   concurrent scans, queues and connect/read/total deadlines so daemon work
   cannot exhaust Core's gateway threads or heap. Retry safe reads within one
   overall deadline. A relay timeout has an unknown submission outcome: retain
   the transaction hash and Hub journal, reconcile by hash, and do not blindly
   replay a state-changing call on another daemon or construct another payment.
   Return useful pool health/reason codes without logging private wallet data,
   credentials or full transaction bodies.
5. **Hub integration and compatibility.** Route both the service's daemon probes
   and the real wallet worker through Core, bound to the selected account, node
   and network. Invalidate stale requests on node/account switches. Require the
   new Core capability; an older Core must produce an upgrade/unavailable result
   rather than trigger direct Internet access. Preserve the frozen account
   derivation, ordinary Qortal backup recovery, permission flow and local signer.

Acceptance requires mock-daemon fault injection for discovery admission, wrong
networks, stale/busy nodes, malformed/oversized payloads, timeouts, concurrent
scans, pool exhaustion, failover and recovery. Use the actual pinned wallet
worker on local chains to verify scan/balance/history, reorg handling and lost
relay replies. Deny Hub outbound daemon access while allowing its selected Core
connection and demonstrate the complete wallet flow through Core. Check that
keys remain local and existing QDN/ARRR/other-wallet behavior is preserved under
load. Record a Core/Hub compatibility matrix and operator pool configuration
before a separate owner-authorized rollout. No runtime tests or builds are
claimed for this documentation update.

## Verification of this milestone

On 2026-11-20, Java 11 compiled all production/test sources and **141 selected
Core tests passed with zero failures, errors or skips**. This covers the ten new
admission tests, all six current v3 ACCT suites, BTC/LTC v1 suites, AT repository
tests and Monero address/amount/network tests. The offline model's **ten tests**
passed; its CLI emitted all four expected rejected-candidate traces. The legacy
browser probe's five Node fixture tests also passed; its browser scenarios and
Hub's runtime suite were not rerun for these documentation/test-only changes.

```bash
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
mvn -o -B -Dmaven.gitcommitid.nativegit=true -DskipJUnitTests=false \
  -Dtest=DeployAtApprovalTests,ACCTTests,BitcoinACCTv3Tests,LitecoinACCTv3Tests,DogecoinACCTv3Tests,DigibyteACCTv3Tests,RavencoinACCTv3Tests,PirateChainACCTv3Tests,LitecoinACCTv1Tests,BitcoinACCTv1Tests,AtRepositoryTests,MoneroAddressTests,MoneroAmountsTests,MoneroNetworkSelectionTests \
  test
python3 -B -m unittest discover -s tools/xmr-trading -p 'test_*.py' -v
python3 -B tools/xmr-trading/protocol_model.py
```
