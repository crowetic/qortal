# Performance and Lock Notes (2026-03-10)

## Context

This note captures all tuning and diagnostic changes made during the unconfirmed transaction, chat latency, and DB lock analysis pass.

Observed symptoms:

- Repeated slow-query bursts where many simple `SELECT` statements all showed ~12s to ~16s durations at the same timestamps.
- Large repeated SQL history dumps (`HSQLDBRepository:1079`) after slow-query events.
- Outbound chat sends that started fast, then progressively slowed after a few messages.
- Transaction signature import cycles dominated by signatures already existing in DB.

Branch:

- `Mar10-2026-unconfirmed-chat-lock-analysis`

Committed baseline change included in this branch:

- `1014fb25` optimize active groups query in chat repository path.

---

## Change Log by Area

### 1) Unconfirmed signature import and queue pressure

Files:

- `src/main/java/org/qortal/controller/TransactionImporter.java`
- `src/main/java/org/qortal/repository/TransactionRepository.java`
- `src/main/java/org/qortal/repository/hsqldb/transaction/HSQLDBTransactionRepository.java`

Changes:

- Added bounded per-cycle processing controls:
  - `MAX_IMPORT_TRANSACTIONS_PER_CYCLE`
  - `MAX_GET_TRANSACTION_MESSAGES_PER_CYCLE`
  - `MAX_SIGNATURE_MESSAGES_PER_CYCLE`
  - `MAX_SIGNATURES_TO_CHECK_PER_CYCLE`
  - `MAX_SIGNATURE_DB_BATCH`
- Added known-existing signature cache in importer:
  - `knownExistingSignatures`
  - `KNOWN_SIGNATURE_RECHECK_INTERVAL`
  - Prevents repeated DB lookups for signatures already known to exist.
- Added `TransactionRepository.getExistingSignatures(List<byte[]>)`.
- Implemented batched `getExistingSignatures()` in HSQLDB repository.
- Added queue cycle INFO logging in importer:
  - `queuedBefore`, `drained`, `queuedAfter`
  - `candidatesBeforeDb`, `existingInDb`, `remainingCandidates`
  - `hitSignatureLimit`
- Added fixed reply thread pool for GET_TRANSACTION responses instead of one thread per reply.
- Added synchronized access fixes around incoming queue mutation/copy.
- Added scheduler shutdown handling for the reply executor.

Expected impact:

- Lower CPU and DB overhead under signature spam / duplicate-signature floods.
- Fewer giant `IN (...)` operations.
- Better visibility into importer backlog and duplicate pressure.

---

### 2) Unconfirmed transaction query path efficiency

Files:

- `src/main/java/org/qortal/repository/TransactionRepository.java`
- `src/main/java/org/qortal/repository/hsqldb/transaction/HSQLDBTransactionRepository.java`

Changes:

- Added `getUnconfirmedTransactionsCreatedBefore(long)` API and HSQLDB implementation.
- Refactored unconfirmed fetch methods to select base transaction columns directly from join:
  - Avoids fetching signatures first and then re-loading each tx via `fromSignature(...)`.
- Added chunking for `fromSignatures(...)` with `MAX_SIGNATURES_PER_QUERY`.

Expected impact:

- Lower N+1 overhead and fewer repeated per-signature round-trips.
- Better behavior for large signature lists and unconfirmed scans.

---

### 3) Expired unconfirmed cleanup optimization

File:

- `src/main/java/org/qortal/controller/Controller.java`

Changes:

- Reworked `deleteExpiredTransactions()` to prefilter candidates:
  - Uses `getUnconfirmedTransactionsCreatedBefore(...)` for standard expiry path.
  - Handles PRESENCE via dedicated query.
  - Merges candidates by signature to avoid duplicate processing.

Expected impact:

- Smaller validation workload per cleanup cycle.
- Reduced full unconfirmed-pool scanning frequency.

---

### 4) Arbitrary metadata fetch load reduction

File:

- `src/main/java/org/qortal/controller/arbitrary/ArbitraryDataManager.java`

Changes:

- Removed up-front full-history fetch for unnamed metadata pass.
- Switched to paged retrieval (`limit=100`, `offset`) for name==null path.
- Kept named path targeted.
- Extracted a reusable helper for signature processing from pre-fetched transaction lists.

Expected impact:

- Significantly less memory and DB pressure from metadata sweeps.
- Reduced chance of long blocking metadata queries.

---

### 5) Trade-bot failed-offer checks and unconfirmed MESSAGE query storms

Files:

- `src/main/java/org/qortal/controller/tradebot/TradeBot.java`
- `src/main/java/org/qortal/api/websocket/TradeOffersWebSocket.java`
- `src/main/java/org/qortal/api/resource/CrossChainResource.java`

Changes:

- Removed per-trade repeated failed-check query pattern.
- Switched websocket and hidden-offers API paths to batch filtering using one `removeFailedTrades(...)` pass.
- In `TradeBot.removeFailedTrades(...)`:
  - Fetch stale unconfirmed MESSAGE recipients once per call.
  - Added 5-second cache for stale-recipient set to dampen burst callers.

Expected impact:

- Large reduction in repeated:
  - `SELECT ... FROM UnconfirmedTransactions ... type IN (MESSAGE)`
  - `SELECT ... FROM MessageTransactions WHERE signature = ?`
- Less repeated work in trade offer websocket updates and hidden-offer endpoints.

---

### 6) Checkpoint gate lock-wave mitigation

File:

- `src/main/java/org/qortal/repository/hsqldb/HSQLDBRepository.java`

Changes:

- Updated `maybeCheckpoint()`:
  - Fast-path return if no checkpoint requested.
  - Use non-blocking `CHECKPOINT_GATE.writeLock().tryLock()` instead of unconditional blocking `lock()`.
  - Re-check request state under lock before proceeding.

Expected impact:

- Prevents queuing writer lock requests from causing read lock waves under fair lock ordering.
- Reduces "many tiny queries all report same high duration" events caused by lock queueing.

---

### 7) Lock hold/wait instrumentation

Files:

- `src/main/java/org/qortal/transaction/Transaction.java`
- `src/main/java/org/qortal/controller/BlockMinter.java`
- `src/main/java/org/qortal/controller/Synchronizer.java`

Changes:

- Added INFO log in `importAsUnconfirmed()` when blockchain lock wait >= 2000ms.
- Added INFO log for lock hold duration in block minter >= 2000ms.
- Added INFO log for lock hold duration in synchronizer >= 2000ms.

Expected impact:

- Easier root-cause timing analysis for lock contention between minter, sync, and tx import.

---

### 8) Chat validation fallback scope fix (important for spam-heavy mempools)

Files:

- `src/main/java/org/qortal/transaction/Transaction.java`
- `src/main/java/org/qortal/transaction/ChatTransaction.java`

Changes:

- When importer cache is unavailable:
  - `Transaction.countUnconfirmedByCreator(...)` now queries only creator-scoped unconfirmed transactions.
  - `ChatTransaction.countRecentChatTransactionsByCreator(...)` now queries only creator+CHAT scoped unconfirmed transactions.
- Added synchronized iteration around the in-memory list used by those counters.

Why this matters:

- Prior fallback path could scan/hydrate the full unconfirmed pool, amplifying latency on nodes exposed to chat spam.
- This directly explains divergent behavior between nodes that block spam sender(s) and nodes that do not.

Expected impact:

- Outbound chat processing latency less sensitive to global unconfirmed chat volume from other creators.

---

## Operational Notes

- `HSQLDBRepository:1079` entries are statement history dumps for a session after a slow-query trigger, not necessarily independent slow queries themselves.
- If slow bursts persist, check new lock timing logs and compare with heavy metadata sweeps and sync cycles.

---

## Verification Performed

- Build verification after changes:
  - `mvn -q -DskipTests compile`
- Compile completed successfully.

No full integration/perf benchmark suite was run in this pass.
