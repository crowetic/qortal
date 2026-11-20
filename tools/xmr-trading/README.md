# Offline QORT/XMR protocol counterexamples

This sandbox is the first executable research increment on `feat/xmr-trading`.
It rejects incomplete candidate protocols before wallet, AT or signing work is
connected. It has no RPC client, private keys, cryptographic implementation or
transaction builder. No production code imports it.

Run with Python 3.8+ and only the standard library:

```bash
python3 -B -m unittest discover -s tools/xmr-trading -p 'test_*.py' -v
python3 -B tools/xmr-trading/protocol_model.py
```

The second command prints shortest counterexample traces as JSON. Exit zero means
that the expected **unsafe candidate examples were found**, not that XMR trading
is safe or enabled. An exhausted exploration bound raises an error rather than
silently reporting that no failure exists.

## What is modeled

The QORT seller wants XMR; the QORT buyer supplies XMR. The seller first funds a
Qortal escrow. The buyer can fund a joint Monero address. Each participant initially
knows one private spend share; both are required to move the joint funds. A claim
witness may expose the buyer's share; a refund witness may expose the seller's.

The model tracks ownership of each principal, readiness, three coarse deadline
phases, seller disappearance, public shares, and publication versus confirmation
of a claim. Exploration is breadth-first over immutable states, deduplicated by
all state fields. Both honest completion and an early mutual recovery are
reachable, but those happy paths do not establish safety.

Two deliberately incomplete claim policies are explored:

- Require a seller readiness message before any buyer claim.
- Also allow buyer claims after a first timeout, even without readiness.

An autonomous QORT refund transfers QORT but cannot invent the seller's private
Monero share. An available seller may explicitly release that share after a
refund. A published claim's share stays public even if the claim is never included.

## Failing traces and implementation consequences

| Failure | Consequence for the real design |
| --- | --- |
| Seller disappears, buyer funds XMR, deadlines pass, AT returns QORT without a private recovery witness | Joint XMR stays locked. A two-party wallet plus an ordinary AT refund is insufficient. |
| Seller disappears, buyer never funds XMR, timeout-only claim pays QORT | A fallback that solves readiness withholding can require active monitoring/refunding to prevent theft. That dependency must not be hidden. |
| Buyer publishes a claim near expiry, it never confirms, AT refunds, seller uses the public share to take XMR | Claim confirmation and knowledge disclosure are different events. Deadline margins, inclusion assumptions and reorg behavior need a protocol argument. |
| The claim pays QORT for a preimage unrelated to the joint spend share | Hash validity alone does not bind a foreign spend key. An independently verified cryptographic binding is necessary. |

These are our model's deductions about its stated rules. They are not a report of
an exploit in an existing deployed contract, nor a comprehensive audit of any
external project. The readiness/timeout pattern is informed by the primary
[Athanor protocol description](https://github.com/athanorlabs/atomic-swap/blob/master/docs/protocol.md)
and [contract](https://github.com/athanorlabs/atomic-swap/blob/master/ethereum/contracts/SwapCreator.sol).
That contract requires a refund witness; our autonomous-refund variant exposes
why a direct copy of Qortal's automatic-refund expectation needs additional work.

## Deliberate omissions

There is no point/scalar arithmetic, proof verification, output scanning, amount
or fee calculation, transaction serialization, journal, chain fork, clock skew,
AT resource accounting or network scheduler. Early refund is treated as already
confirmed, while claims expose a separate unconfirmed-witness step. Returning
online, delayed refund witnesses, watchtowers, pre-signed recovery material and
adaptors are not implemented. These are explicit model limits to extend once a
candidate protocol is specified.

The `claim_bound_to_spend_share` flag is an assumption used to contrast valid and
invalid binding. It is not a cryptographic proof and must never be used as a
runtime trust flag. Funding/readiness transitions assume a locally verified
matching deposit and confirmed setup. Real implementations must establish those
facts independently of an untrusted daemon or counterparty.

No new AT bytecode, approved hash, VM instruction, trade API, offer capability,
database change or funded test is introduced by this sandbox. Next steps and
activation rules are in [the research plan](../../docs/monero-crosschain-trades.md)
and [AT approval audit](../../docs/at-deployment-approval.md).
