# Recent QDN transaction query optimization

`performance-db-improvements` consolidates `db-query-improvements` at
`a5d20f639db3e5472d1bd0e29cce77c6c81f9dcd` and `performance-fixes` at
`b9d8816a7d0ae1cc062eed8355211f2b5d768949`, with the new timestamp query
optimization. It is separate from the
migration-free `qdn-performance` XMR/ARRR testing JAR. Do not transfer this
migration alone between branches: their earlier schema histories differ.

## Database change

Both parent branches used schema 54 for different indexes: the database branch
added `ArbitraryResourcesServiceCreatedIndex(service, created_when)` and the
performance branch added `AccountBalancesBalanceIndex(balance)`. The combined
migration from 54 to 55 ensures both indexes exist, then adds
`ArbitraryCreatedIndex` on `ArbitraryTransactions(created_when DESC)`. The earlier
service-index migration also accepts an existing index so interrupted DDL can be
retried. Tests cover each parent layout and neither/both indexes already present. Index creation happens once at
startup and can extend startup on large repositories. Existing transactions,
resource caches, wallets and trade records are preserved by this migration.

Limited recent-transaction reads use HSQLDB's `LIMIT ... USING INDEX` hint.
Adding the index alone still allowed the planner to choose the name-leading
index and read/sort all named rows. The hint selects timestamp order and stops
after the requested number of named transactions; hydration and the full-history
query remain unchanged. See [HSQLDB query optimization](https://www.hsqldb.org/doc/2.0/guide/management-chapt.html).

The November 17 gateway log records a 30.9-second recent-transaction query and
dumps show database disk-cache contention. This optimization addresses that
workload; it is not proof that the persistent QDN loading failure is fixed and
does not optimize the separate AT-state maintenance query.

## Verification and repeatable benchmark

Use JDK 11; enable tests explicitly:

```sh
mvn -Dmaven.gitcommitid.nativegit=true -DskipJUnitTests=false \
  -Dtest=ArbitraryLatestTransactionsTests,ArbitraryResourceSearchTests,HSQLDBCacheUtilsTests,AccountBalanceCleanupTests,PirateChainACCTv3TradeBotTests test
java -Xmx512m --class-path lib/org/hsqldb/hsqldb/2.7.4/hsqldb.jar \
  tools/QdnLatestQueryBenchmark.java 100000
```

Repository tests verify ordered hydrated results, empty/updated sets, the actual
production SQL's query plan and migration from version 54 with/without an existing
index. The benchmark opens and deletes only its own temporary disk-backed
repository, uses Core's HSQLDB 2.7.4 and MySQL syntax, a 2 MiB row cache,
scrambled timestamps and 10% unnamed rows. Local JDK 11 warm medians:

| Rows | Previous query | Timestamp index + hint | Index build |
| --- | --- | --- | --- |
| 100,000 | 396.559 ms | 0.977 ms | 3.865 s |
| 300,000 | 971.707 ms | 0.637 ms | 12.885 s |

Both fixtures returned identical ordered signatures before/after. These are
synthetic local measurements, not gateway timings or production speedup promises.
The command prints plans and timings for reproduction.

The combined branch retains candidate filtering before metadata joins, service-scoped
latest grouping, cached-search work reduction and indexed zero-balance cleanup
from the parent branches. Both parent histories remain reachable; the retired
branch names can be removed after the combined remote tip is verified.

## Rollout boundary

No production rollout or database migration is performed by this task. This
branch already contains other database changes; evaluate its complete history
and migration/rollback requirements before testing it on a real node. Preserve
the old JAR and take a complete stopped-node repository/wallet/recovery backup
before that separately authorized rollout. Allow index creation to finish and
check `/admin/info`, `/admin/status`, query timing and cached/uncached QDN reads.
Never lower `DatabaseInfo.version` or restore financial state independently.

For a simple JAR swap back to the current XMR/ARRR base, use the separate
`qdn-performance` artifact, which does not contain this migration.
