import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/** Standalone HSQLDB fixture; never opens a node's repository. See docs/qdn-performance.md. */
public class QdnLatestQueryBenchmark {
    private static final String QUERY = "SELECT signature FROM ArbitraryTransactions "
            + "WHERE name IS NOT NULL ORDER BY created_when DESC LIMIT 100";

    public static void main(String[] args) throws Exception {
        int rows = args.length == 0 ? 100_000 : Integer.parseInt(args[0]);
        if (rows < 1) throw new IllegalArgumentException("Row count must be positive");
        Path directory = Files.createTempDirectory("qortal-qdn-query-");
        try {
            run(directory, rows);
        } finally {
            try (Stream<Path> paths = Files.walk(directory)) {
                for (Path path : (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private static void run(Path directory, int rows) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:hsqldb:file:" + directory.resolve("fixture"), "SA", "");
             Statement statement = connection.createStatement()) {
            statement.execute("SET DATABASE SQL SYNTAX MYS TRUE");
            // A small database row cache exercises eviction without a large fixture.
            statement.execute("SET FILES CACHE ROWS 2048");
            statement.execute("SET FILES CACHE SIZE 2048");
            statement.execute("CREATE CACHED TABLE ArbitraryTransactions (signature VARBINARY(64) PRIMARY KEY, "
                    + "name VARCHAR(400), created_when BIGINT NOT NULL, data VARBINARY(256))");
            statement.execute("CREATE INDEX ArbitraryNameIndex ON ArbitraryTransactions(name)");
            statement.execute("CREATE INDEX ArbitraryNameCreatedIndex ON ArbitraryTransactions(name,created_when DESC)");
            connection.setAutoCommit(false);
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO ArbitraryTransactions VALUES(?,?,?,?)")) {
                for (int i = 0; i < rows; i++) {
                    byte[] signature = new byte[64];
                    ByteBuffer.wrap(signature).putInt(i);
                    insert.setBytes(1, signature);
                    insert.setString(2, i % 10 == 0 ? null : "name-" + i % 1000);
                    // Unique timestamps, unrelated to name order, keep result comparison exact.
                    insert.setLong(3, ((long) i * 48271) % Integer.MAX_VALUE);
                    insert.setBytes(4, new byte[256]);
                    insert.addBatch();
                    if (i % 1000 == 999) {
                        insert.executeBatch();
                        connection.commit();
                    }
                }
                if (rows % 1000 != 0) insert.executeBatch();
                connection.commit();
            }
            System.out.println("HSQLDB " + connection.getMetaData().getDatabaseProductVersion()
                    + ", cached rows=" + rows + ", unnamed fraction=10%, database row cache=2 MiB");
            List<String> before = measure(statement, "before", QUERY);
            long start = System.nanoTime();
            statement.execute("CREATE INDEX ArbitraryCreatedIndex ON ArbitraryTransactions(created_when DESC)");
            System.out.printf(Locale.ROOT, "Index creation: %.3f ms%n", (System.nanoTime() - start) / 1e6);
            List<String> after = measure(statement, "after", QUERY + " USING INDEX");
            if (!before.equals(after)) throw new AssertionError("Query results changed");
            System.out.println("Identical ordered signatures: " + after.size());
            statement.execute("SHUTDOWN");
        }
    }

    private static List<String> measure(Statement statement, String label, String query) throws Exception {
        statement.execute("CHECKPOINT");
        System.out.println(label + ": " + query);
        if (!statement.execute("EXPLAIN PLAN FOR " + query)) {
            throw new AssertionError("EXPLAIN did not return a query plan");
        }
        try (ResultSet plan = statement.getResultSet()) {
            while (plan.next()) System.out.println(plan.getString(1));
        }
        double[] warm = new double[3];
        List<String> signatures = new ArrayList<>();
        for (int run = 0; run < 4; run++) {
            long start = System.nanoTime();
            signatures.clear();
            try (ResultSet results = statement.executeQuery(query)) {
                while (results.next()) signatures.add(Base64.getEncoder().encodeToString(results.getBytes(1)));
            }
            double elapsed = (System.nanoTime() - start) / 1e6;
            if (run > 0) warm[run - 1] = elapsed;
            System.out.printf(Locale.ROOT, "%s run %d: %.3f ms, rows=%d%n", label, run, elapsed, signatures.size());
        }
        Arrays.sort(warm);
        System.out.printf(Locale.ROOT, "%s warm median: %.3f ms%n", label, warm[1]);
        return signatures;
    }
}
