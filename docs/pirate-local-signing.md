# PirateChain local signing investigation

Reviewed 29 October 2026 on `pirate-local-signing`, created from
`pirate-changes` at `866f8565b`. This document is an implementation proposal;
this branch does not yet enable client signing or change runtime behavior.

## Finding

Local signing is feasible. The shortest supported route appears to be running
Pirate's existing native wallet engine inside the wallet host, initially desktop
Hub. It already supports shielded transaction construction, signing and broadcast.
This conclusion is based on the source and contract checks below; a funded
Hub integration has not been demonstrated.

The important goal is that wallet entropy, mnemonics and shielded spending keys
stay on the user's device. This applies to balance, history and receive-address
requests as well as payments and trade funding. Moving only the send operation
would leave the existing read APIs transmitting the same wallet secret to Core.
Local signing still needs synchronized chain data; it does not imply an
air-gapped signer.

## How the existing implementations differ

| Area | BTC/LTC/DOGE/DGB/RVN | PirateChain today |
| --- | --- | --- |
| Discovery | Core receives a root XPUB and returns public UTXO data | Core receives 32-byte wallet entropy and opens a spend-capable native wallet |
| Payment signing | Hub builds and signs a legacy transaction locally | Core calls native `send`, which builds, signs and broadcasts |
| Trade funding | `/crosschain/tradebot/respond/local` prepares HTLC destinations and waits for client funding | `PirateChainACCTv3TradeBot.startResponse` receives and backs up entropy, then funds immediately |
| Recovery | Core keeps trade-specific keys and secrets | Core also retains wallet entropy to derive Alice's refund address |

Relevant implementation points:

- [LocalWalletSupport](../src/main/java/org/qortal/crosschain/LocalWalletSupport.java)
  implements discovery, chain checks and signed broadcasts for five BTC-family
  currencies. Its whitelist, P2PKH discovery and bitcoinj transaction parser do
  not support ARRR.
- [CrossChainPirateChainResource](../src/main/java/org/qortal/api/resource/CrossChainPirateChainResource.java)
  exposes entropy-backed wallet queries and `/send`. It has no equivalent
  client-signed send/broadcast API.
- [PirateWallet.initialize](../src/main/java/org/qortal/crosschain/PirateWallet.java)
  converts the entropy into the deterministic mnemonic and initializes native
  storage. A Hub implementation must preserve this derivation and restore
  birthday, including the legacy Sapling account path `m/32'/141'/0'`.
- [PirateChain.sendCoins/fundP2SH](../src/main/java/org/qortal/crosschain/PirateChain.java)
  invoke native `send`/`sendp2sh` while holding the wallet controller lock.
- [CrossChainTradeBotResource.prepareLocalFunding](../src/main/java/org/qortal/api/resource/CrossChainTradeBotResource.java)
  explicitly excludes PirateChain. Removing that exclusion alone is insufficient.
- [PirateChainACCTv3TradeBot](../src/main/java/org/qortal/controller/tradebot/PirateChainACCTv3TradeBot.java)
  stores entropy in `foreignKey`, including recovery backups. Alice's refund
  currently calls `getWalletAddress(foreignKey)`. The actual refund transaction
  uses a trade-specific private key through a null-seed wallet, so refund
  address discovery can be separated from wallet spending authority.

See [the existing local-wallet contract](local-wallet-signing.md) for the
BTC-family behavior we want to match.

## What the released Pirate library already provides

The GitHub release API reported `v1.2.6` as the latest Stashi Wallet release at
review time. The tag resolves to commit
`dedf63381996c5770945d16a4afa63ec7e35a676`. All upstream links below use that
immutable commit. The existing local Pirate development checkout contains
uncommitted work, so this investigation used a separate release-source download.

The [typed wallet service](https://github.com/PirateNetwork/Stashi-Wallet/blob/dedf63381996c5770945d16a4afa63ec7e35a676/crates/pirate-wallet-service/src/service.rs)
exposes these JSON methods:

- `configure_wallet_storage`, `restore_wallet`, `start_sync` and
  `get_spendability_status` for a persistent local wallet.
- `build_tx` for preparing shielded payments.
- `sign_tx` for returning a signed transaction without broadcasting it.
- `broadcast_tx` for submitting the signed result, scoped to a wallet.
- `import_viewing_wallet` for Sapling/Ironwood viewing keys. Viewing keys reveal
  wallet activity and must be treated as sensitive even though they cannot spend.

The [native C ABI](https://github.com/PirateNetwork/Stashi-Wallet/blob/dedf63381996c5770945d16a4afa63ec7e35a676/crates/pirate-ffi-native/src/lib.rs)
exports `pirate_wallet_service_invoke_json` and
`pirate_wallet_service_free_string`. This is a plausible desktop integration
surface; Hub still needs a native bridge or helper process. Java's
`LiteWalletJni.invokeJson` is useful for Core and verification, but does not
provide an Electron bridge by itself. The release also supplies Android/iOS SDK
artifacts; their integration needs separate platform testing.

There are three important constraints in the
[transaction flow](https://github.com/PirateNetwork/Stashi-Wallet/blob/dedf63381996c5770945d16a4afa63ec7e35a676/crates/pirate-wallet-service/src/api/tx_flow.rs):

1. `build_tx` caches selected notes in the current process. The
   [PendingTx model](https://github.com/PirateNetwork/Stashi-Wallet/blob/dedf63381996c5770945d16a4afa63ec7e35a676/crates/pirate-wallet-service/src/models.rs)
   is a summary, not a portable unsigned transaction with all witnesses.
   `sign_tx` consumes matching cached context, with a ten-minute lifetime.
   Building on remote Core and forwarding that summary to Hub will not work.
2. Signing needs the local encrypted wallet database, spending keys, spendable
   notes and validated anchors. Readiness checks can trigger synchronization.
   Restarted or expired preparation must be rebuilt and approved again.
3. Signing also records process-local broadcast context. An integration must
   preserve wallet attribution and reconcile pending spends after cancellation,
   restart and ambiguous submission. Returning raw bytes is only part of the job.

Ordinary `build_tx` outputs are shielded addresses. It cannot replace the special
Qortal P2SH funding path. The
[Qortal P2SH service](https://github.com/PirateNetwork/Stashi-Wallet/blob/dedf63381996c5770945d16a4afa63ec7e35a676/crates/pirate-wallet-service/src/api/qortal_p2sh.rs)
currently combines construction/signing and broadcast in `qortal_send_p2sh`.
The underlying [P2SH builder](https://github.com/PirateNetwork/Stashi-Wallet/blob/dedf63381996c5770945d16a4afa63ec7e35a676/crates/pirate-core/src/qortal_p2sh.rs)
already returns signed bytes, so a separate prepare/sign funding API can reuse
that implementation. It must preserve funding output zero and Qortal's redeem
script/prefix conventions.

## Recommended architecture

Run the native service in a privileged Hub worker or helper, with account-scoped
encrypted storage. Hub derives/restores the existing ARRR wallet locally, scans
compact blocks from lightwalletd, and handles balances, addresses and history
locally. Core remains the trade coordinator.

Expose a narrow host interface for wallet status, payment preparation, approved
signing and submission. Do not expose arbitrary native JSON requests to Q-Apps
or the renderer: the service also contains key export, wallet deletion and file
operations. Keep the native pending object in the privileged process, tied to
the approved account, network, recipient, exact atomic amount, memo and fee.
Do not treat a caller-supplied pending summary as proof of approval.

Use decimal-string atomic amounts across JavaScript boundaries. Pin and verify
native artifacts, keep expensive proving off the UI thread, and test logout,
account switching and cancellation. Preserve native rescan/anchor gates rather
than inferring spendability from total balance or displayed sync height.

For initial payments, native `broadcast_tx` can submit directly to lightwalletd.
A new Core broadcast endpoint is optional for that milestone. If Core is used
as a relay later, add bounded Pirate-specific parsing, network/consensus checks,
correct transaction-ID calculation and submission-status reconciliation first.
`PirateLightClient.broadcastTransaction(byte[])` already submits raw bytes over
gRPC, but its success response is not independent transaction-ID validation.
`PirateChain.deserializeRawTransaction` handles legacy v3/v4 transparent data
incompletely and rejects other overwintered formats; it is not a complete modern
shielded transaction validator. Do not reuse bitcoinj or assume every Pirate
transaction ID is a Bitcoin-style hash of serialized bytes.

No browser/WASM build of this complete wallet service was identified in the
reviewed release. SQLite, native networking, threads and proving need separate
porting work. A browser-only path should be a later feasibility task. Another
possible design is remote viewing-only scanning plus a portable signing package,
but the released pending model does not supply that protocol. That option needs
more native API and privacy work than keeping scanning and signing in Hub.

## Implementation sequence

### 1. Desktop wallet-host proof

- Package a checksum-pinned native engine behind Hub's privileged boundary.
- Restore a synthetic wallet from the same public entropy vector as Core and
  compare Sapling/Ironwood derivation, including internal change handling.
- Use isolated storage and a loopback lightwalletd fixture to exercise sync,
  readiness and prepare/sign separation. Establish cancellation, restart,
  memory/storage cost and platform packaging before using real wallets.

This work belongs primarily in Hub. No ARRR entropy endpoint should be needed
for the local mode, including read operations. Native C ABI packaging must be
tested separately from the existing Qortal JNI artifact.

### 2. Ordinary shielded payments

- Implement local balance/history/address/status and immutable payment approval.
- Use local `build_tx` then `sign_tx`, submitting only after approval. Persist
  enough pending-send state to reconcile an uncertain submission and avoid
  selecting the same notes for another payment.
- Complete controlled funded Sapling/Ironwood send, change and restore tests.
  A timeout must not automatically create and sign a second payment.

### 3. Client-funded Pirate trades

- Add a Pirate-specific prepare-funding contract in Core with a persisted
  waiting-for-funding state. Reuse the lifecycle ideas in `LocalTradeFunding`,
  while retaining Pirate's script, address and funding-discovery behavior.
- Return the offer identity, chain identity, P2SH script/address, exact amount,
  separate funding/redemption fees and deadline. Hub validates these against
  the selected offer before signing locally. Persist an approved shielded
  refund address independently of wallet entropy.
- Add a native P2SH prepare/sign operation that does not broadcast, reusing the
  existing builder and retaining note-selection/broadcast accounting. Keep the
  old combined operation for legacy callers.
- Make repeated preparation idempotent. Resume only after Core independently
  checks sufficient confirmed funding to the expected script. A client-reported
  txid is a lookup hint, not proof that the trade was funded.
- Retain trade-specific keys/secrets in Core so completion and refunds work
  while Hub is closed. New trade rows/backups need an explicit mode and refund
  address; legacy rows must continue using their existing entropy-backed path.
  Do not overload `foreignKey` with an address and let old code interpret it as
  entropy. Include repository/backup compatibility and restart tests.

The trade milestone crosses Core, Hub and the native wallet project. It should
follow ordinary payments because it adds durable state and recovery behavior.

### 4. Platform coverage and transition

Integrate/test Android and iOS SDKs, then decide whether browser support needs
a WASM port or another design. Advertise actual client capabilities so an
unsupported host does not silently fall back to sending entropy to remote Core.
Existing Core wallet databases and trade backups can still contain secrets from
earlier use; enabling local mode does not remove them or migrate active trades.

## Verification performed

The focused offline suite passed **30 tests, zero failures/errors/skips**:

```bash
mvn -B -DskipJUnitTests=false \
  -Dtest=LocalWalletTests,LocalWalletApiTests,PirateChainAddressTests,PirateSpendResponseTests,PirateChainACCTv3TradeBotTests,CrossChainPirateChainResourceTests \
  test
```

A separate standalone JVM loaded the existing Linux x86_64 v1.2.6 JNI library.
`get_build_info` reported version `1.2.6` and the release commit above.
Incomplete `build_tx`, `sign_tx`, `broadcast_tx` and `qortal_send_p2sh` requests
were recognized and rejected during deserialization for missing fields. This
checks method availability without dispatching signing or broadcast operations.
No wallet was restored, synchronization requested, or funds signed/broadcast
by that probe. It does not prove funded signing or C ABI integration.

The local release ZIP matched its existing pinned SHA-256:

```text
60c91d1abfba16f97e22bf0f78b4aa846f9bd5b7455ab7d2f7155a3f5f9d1bc5
```

Future acceptance must cover both pools and internal change; wrong network,
recipient/amount/fee/memo changes; expired/replayed or wrong-wallet preparation;
restart/account switching; missing witnesses and reorg/rescan gates; uncertain
broadcast; confirmed HTLC funding; completed trades and timed-out refunds with
Hub closed. Assert that local-mode requests and new trade backups contain no
wallet entropy, mnemonic or spending key. The existing historical HTLC discovery
limitation remains documented in
[Pirate library testing](pirate-chain-wallet-library-testing.md).

This investigation requires no deployment, service restart or wallet migration.
The recommended next implementation is milestone 1 in Hub, using the released
native engine and synthetic test data.
