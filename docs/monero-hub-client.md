# XMR client: Hub ownership and unified recovery

Decision date: 2026-11-17; status reviewed 2026-11-20. Hub's `xmr-support` branch
now implements account/backup recovery and a desktop stagenet wallet with local
scanning, balances/history and approved sends. The owner has reported a successful
stagenet payment. Core supplies network metadata and contains no Hub wallet
engine. See [Hub's current wallet guide](https://github.com/crowetic/Qortal-Hub/blob/xmr-support/docs/monero-stagenet-wallet.md)
and [the combined manual test guide](arrr-xmr-testing.md).

The standalone [`tools/xmr-hub-client`](../tools/xmr-hub-client/README.md) directory
is a **legacy, optional browser feasibility probe**. It is not loaded by Core,
Hub, or Hub's Q-App bridge. The manual `tools/xmr-account-test` page remains useful
for fresh-profile backup checks; its automated mock-bridge runner currently lives
in the probe. See [the dependency audit](../tools/xmr-hub-client/README.md#dependency-and-retirement-audit)
before retiring that test harness. The browser findings below are historical
measurements, separate from the integrated desktop worker.

## Responsibilities

The owner requires all wallet cryptography to run locally inside Qortal Hub. XMR key creation belongs in the same account lifecycle as the existing coin wallets. One Qortal backup must recover them all.

```text
Q-Wallets / another Q-App
  | approved wallet requests; public results
  v
Trusted Qortal Hub account service and Monero worker
  | public chain queries / locally signed transaction bytes
  v
Monero daemon (direct transport or bounded Qortal Core connector)
```

Hub owns root-seed access, XMR spend/view keys, scanning, subaddresses, transaction preparation, confirmation, signing, encrypted cache storage and account-lock cleanup. Load the engine from Hub's own pinned assets. A Q-App must not supply executable wallet code, daemon URLs, worker URLs or a private-key destination. Never copy an unlocked account into a QDN iframe/webview.

The Q-App displays receive addresses, sync state, balances, history and send requests. Hub authenticates the requesting app, obtains wallet permissions and displays its own transaction confirmation before signing. Root spend/view keys, mnemonic, seed, wallet blobs and raw signing internals do not cross the app bridge. The daemon receives no private view key: scanning remains local. A light-wallet server that requires that key does not meet this boundary.

Core stays a source of Qortal mode and approved daemon connection information, with a restricted transport adapter only if needed. It does not create wallets or perform wallet cryptography. The Monero daemon still needs a full or pruned chain database. Public daemon hosts/domains are not selected and no server rollout is part of this increment.

## Existing Hub backup contract

Reviewed canonical source: `/home/crowetic/git-repos/gitea/crowetic/Qortal-Hub`.

| Hub source | Existing behavior and XMR consequence |
| --- | --- |
| `src/utils/generateWallet/phrase-wallet.ts` | `PhraseWallet` derives the Qortal address seed by wallet version and address nonce, then builds the current coin wallets. XMR must join the creation and restoration lifecycle without changing any existing result. |
| `src/utils/generateWallet/storeWallet.ts` | `generateSaveWalletData` encrypts `_byteSeed` and returns `address0`, `encryptedSeed`, `salt`, `iv`, `version`, `mac`, and `kdfThreads`. The backup represents the shared root, rather than a list of independent coin keys. |
| `src/background/background.ts` | `decryptWallet` decrypts that seed, constructs `PhraseWallet` and explicitly extracts the existing coin keys. XMR's trusted handoff and cleanup must be added here as well as to creation. |
| `src/qortal/get.ts` | `getUserWallet` is the permissioned Q-App entry point. XMR responses must be an explicit public projection, never a serialized account object. |
| `src/components/Auth/{BackupWalletModal,DownloadWallet}.tsx`, `src/App.tsx` | Backup/export and phrase-restore paths also rebuild `PhraseWallet`. Test all of them against the same XMR derivation contract. |

Creating an unrelated random XMR key would break the backup promise. Hub's fixed,
versioned derivation reconstructs XMR keys from the recoverable account seed, so
older Qortal backups can recover the new wallet without extra private fields.
The engine's scan cache and pending-operation state have their own lifecycle.

The account foundation's implemented contract is:

1. Define and freeze `xmrDerivationVersion = 1`: input bytes, domain label, account/address index, Qortal wallet version, network separation, hash/KDF, scalar reduction, byte order and zero-scalar handling. Use maintained cryptographic primitives and the reference wallet for independent vectors. Do not reinterpret an Ed25519 signing key, Bitcoin HD key or mnemonic as a Monero spend key. Do not bump the Qortal wallet version to add one coin.
2. Derive/create the same XMR wallet on new-account creation, phrase login and encrypted-backup recovery. Route secret material only to trusted Hub code. Review the Hub key-storage guard and logout/account-switch cleanup before adding any secret field.
3. Preserve root-seed backup compatibility. Any additive derivation metadata needs an explicit legacy default and round-trip tests; the algorithm must remain reproducible from old backups. Future derivation versions must preserve recovery of funded older wallets.
4. Keep cache/restore-height data separate from spending authority. If the cache or optional birthday is missing, scan from a safe height rather than silently starting at today's tip. Define recovery of allocated subaddress indexes beyond normal lookahead.
5. Prove recovery on a fresh profile using only the ordinary encrypted Qortal backup. Compare XMR address and private-key results internally, while reporting only pass/fail and public test fixtures. Verify BTC/LTC/DOGE/DGB/RVN/ARRR/QORT outputs are unchanged for existing wallet versions and nonces. Only then expose a receive address.

The exact derivation algorithm is deliberately not invented in the browser probe. Its public scalar fixture demonstrates deterministic engine reconstruction; it does not test Hub's encrypted-backup KDF or account derivation.

## Engine choice and measured feasibility

Hub now pins `monero-ts` 0.11.22 in its own `electron/package.json`. Its
`electron/src/monero-wallet-service.ts` starts the bundled
`monero-wallet.worker`, resolving that SDK locally. The worker owns scanning,
encrypted cache/pending-payment journals and payment preparation/relay. Hub's
request handlers provide permissions, account/network binding and signing
approval. Keep these product files and dependencies when cleaning up the probe.

The original browser experiment selected
[`monero-ts` 0.11.22](https://github.com/woodser/monero-ts/releases/tag/v0.11.22).
The [previous release](https://github.com/woodser/monero-ts/releases/tag/v0.11.21)
updated the underlying Monero code to 0.18.5.4. Pin the package lock, worker bytes
and license notices; reproducible native/WASM source builds and a dependency/license
review remain release work.

The isolated [`tools/xmr-hub-client`](../tools/xmr-hub-client/README.md) probe uses only a published synthetic scalar and loopback daemon fixtures. It tests ordinary Chromium and a cross-origin sandboxed iframe, each on mainnet and stagenet. Results cover local wallet/message signing, wrong-password rejection, encrypted wallet/cache persistence and reopen after worker restart, deterministic reconstruction without cache files, view-only creation and native daemon-height transport. The iframe is a compatibility stress test; the product's secrets belong in Hub.

No cross-origin isolation or `SharedArrayBuffer` was required. The engine reported a 23,658,496-byte WASM heap in this probe; that is not total browser/process memory or evidence of practical full-chain scanning performance. The copied worker is about 3.7 MB and embeds its WASM. The SDK also requires browser shims, including a working `memfs` implementation to reopen wallet bytes. The main application bundle is also several MB. Bundle sizes and performance must be reassessed inside Hub's actual Vite 7/Electron/mobile builds; the standalone probe uses its own Vite toolchain.

The browser probe establishes no real scan, payment, Hub permission behavior or
funded acceptance. Product verification lives in Hub: account/backup and request
tests, worker lifecycle tests, plus `scripts/verify-xmr-wallet-worker.mjs` using
the actual worker against a local Monero development chain. An npm advisory check
is a limited dependency check, not a security audit. Supported-platform and
independent security acceptance remain separate release gates.

## Network and transport contract

| Qortal `/admin/info` / `GET_NODE_INFO` result | Monero network |
| --- | --- |
| Explicit `moneroNetwork: "mainnet"` or `"stagenet"` with valid node mode | Explicit network wins |
| No explicit hint, `isTestNet: false` | `mainnet` |
| No explicit hint, `isTestNet: true` | `stagenet` |
| Invalid explicit hint, or missing/non-boolean node mode | Unavailable; fail closed |

Core's `moneroNetwork` settings names are case-insensitive and `/admin/info`
reports lowercase. Explicit stagenet on a Qortal-mainnet node is supported without
changing Qortal consensus. The current functional desktop wallet permits stagenet
only; mainnet account derivation does not enable mainnet spending. Monero's
separate `testnet` is not used. Hub checks daemon `get_info.nettype` before
scanning/preparing, distinguishes local scan from daemon synchronization, and
scopes cache and operations to the account/network. Account/Core changes invalidate
pending approvals and terminate the old session. The historical browser probe
retains the earlier `isTestNet`-only selector; it is not a product policy test.

Two transport details were found in the pinned engine:

- The default worker URL points at the origin root. Resolve it beneath the trusted Hub asset directory. The fixture tests a nested `/render/APP/.../` path to catch accidental root resolution; real Hub must not take its worker path from a Q-App.
- The native full wallet drops the path prefix of a configured daemon URI and requests origin-root endpoints such as `/json_rpc`. The JavaScript daemon wrapper preserves a prefix, so testing that wrapper alone gives incomplete evidence. The fixture verifies the native wallet against a separate CORS-enabled daemon origin. A Core URL such as `/crosschain/xmr/...` needs a tested worker transport adapter; configuring that URL alone is insufficient.

Prefer Hub-owned networking to an open server proxy. Direct browser transports require suitable HTTPS/CORS; Electron/mobile adapters must preserve endpoint policy and cancellation. A Core relay, if required, needs a fixed daemon pool per network and a small allowlist of required JSON and binary daemon routes, bounded payloads/deadlines and exact integer handling. Do not accept arbitrary user-supplied upstream URLs. Full scan binary endpoints, broadcast error semantics, node switching and mainnet-scale performance are not established by the height fixture.

## Delivery status and remaining acceptance

| Order | Component | Completion evidence |
| --- | --- | --- |
| 1 | Hub derivation and unified-backup integration | Implemented on Hub `xmr-support`; independent/Monero-engine vectors and real encrypted-backup tests pass. Run the fresh-profile manual acceptance in the combined test guide. |
| 2 | Trusted Hub worker and vault lifecycle | Desktop stagenet implementation and process-lock tests exist; encrypted cache/journal, account lock/switch cleanup; broader platform acceptance remains. |
| 3 | Public Q-App wallet interface | Address/balance/info/history and send bridge implemented; exact atomic strings, permission and approval tests; Q-Wallets testing consumes it. |
| 4 | Daemon pool and scan transport | Stagenet scanning and network checks implemented with one fixed daemon; multi-daemon failover and broader reorg/platform acceptance remain. |
| 5 | Hub-confirmed local sends | Approval, fee cap, local signing and durable pending journal implemented; real-worker local-chain regression exists and owner reports a stagenet payment; full release matrix remains. |

Hub work remains on `xmr-support`. The Core research branch `feat/xmr-trading`
changes no Hub runtime files and requires no restart or rollout. Its protocol and
AT work is [tracked separately](monero-crosschain-trades.md). The optional browser
probe can be retired independently after retaining/moving any useful tests;
removing Hub's own worker or SDK would break the integrated wallet.
