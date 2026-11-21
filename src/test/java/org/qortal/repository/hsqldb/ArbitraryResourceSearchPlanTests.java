package org.qortal.repository.hsqldb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.qortal.api.SearchMode;
import org.qortal.arbitrary.misc.Service;
import org.qortal.repository.DataException;
import org.qortal.settings.Settings;
import org.qortal.test.common.Common;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

/** Check the engine's access plan, rather than relying on unstable wall-clock thresholds. */
public class ArbitraryResourceSearchPlanTests extends Common {
    private Field cacheEnabled;
    private boolean oldCacheEnabled;

    @Before
    public void beforeTest() throws Exception {
        Common.useDefaultSettings();
        cacheEnabled = Settings.class.getDeclaredField("dbCacheEnabled");
        cacheEnabled.setAccessible(true);
        oldCacheEnabled = cacheEnabled.getBoolean(Settings.getInstance());
        cacheEnabled.setBoolean(Settings.getInstance(), false);
    }

    @After
    public void restoreSettings() throws Exception {
        cacheEnabled.setBoolean(Settings.getInstance(), oldCacheEnabled);
    }

    private static final class RecordingRepository extends HSQLDBRepository {
        String lastQuery;
        RecordingRepository(Connection connection) throws DataException { super(connection); }
        @Override
        public ResultSet checkedExecute(String sql, Object... parameters) throws SQLException {
            lastQuery = sql;
            return super.checkedExecute(sql, parameters);
        }
    }

    @Test
    public void serviceSearchUsesServiceDateRangeBeforeMetadataLookup() throws Exception {
        assertSearchPlan(Service.WEBSITE, "ARBITRARYRESOURCESSERVICECREATEDINDEX");
    }

    @Test
    public void globalSearchKeepsChronologicalIndexPagination() throws Exception {
        assertSearchPlan(null, "ARBITRARYRESOURCESCREATEDINDEX");
    }

    private static void assertSearchPlan(Service service, String expectedIndex) throws Exception {
        Connection connection = DriverManager.getConnection(Common.testConnectionUrlMemory);
        connection.setAutoCommit(false);
        try (RecordingRepository repository = new RecordingRepository(connection)) {
            for (boolean metadata : Arrays.asList(false, true)) {
                for (boolean reverse : Arrays.asList(false, true)) {
                    repository.getArbitraryRepository().searchArbitraryResources(service, null, "post", null,
                            null, "needle", null, false, null, false, SearchMode.ALL, null, false, false,
                            metadata, false, null, null, 20, 1, reverse);
                    String sql = repository.lastQuery;
                    StringBuilder plan = new StringBuilder();
                    try (Statement statement = connection.createStatement()) {
                        // Core enables SQL RESTRICT EXEC, so EXPLAIN must use execute, not executeQuery.
                        assertTrue(statement.execute("EXPLAIN PLAN FOR " + sql));
                        try (ResultSet result = statement.getResultSet()) {
                            while (result.next()) plan.append(result.getString(1)).append('\n');
                        }
                    }
                    String text = plan.toString();
                    int resourcesAt = text.indexOf("table=ARBITRARYRESOURCESCACHE");
                    int metadataAt = text.indexOf("table=ARBITRARYMETADATACACHE");
                    assertTrue(text, resourcesAt >= 0 && metadataAt > resourcesAt);
                    String resourcesPlan = text.substring(resourcesAt, metadataAt);
                    assertTrue(text, resourcesPlan.contains("index=" + expectedIndex));
                    assertTrue("Identifier filter must precede metadata lookup: " + text, resourcesPlan.contains("LIKE"));
                    if (service != null) {
                        assertTrue(text, resourcesPlan.contains("access=INDEX PRED"));
                        assertTrue(text, resourcesPlan.contains("DYNAMIC PARAM"));
                    }
                    String ordering = text.substring(text.indexOf("order by="), text.indexOf("PARAMETERS="));
                    assertTrue("LIMIT must use the ordered index: " + text, ordering.contains("uses index"));
                }
            }
        }
    }

    @Test
    public void version53UpgradePreservesResourcesAndAddsOrderedIndex() throws Exception {
        String url = "jdbc:hsqldb:mem:resource-index-upgrade-" + UUID.randomUUID();
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE DatabaseInfo(version INT NOT NULL)");
                statement.execute("INSERT INTO DatabaseInfo VALUES(53)");
                statement.execute("CREATE TABLE ArbitraryResourcesCache(service SMALLINT, created_when BIGINT)");
                statement.execute("INSERT INTO ArbitraryResourcesCache VALUES(100,10),(200,20),(100,30)");
            }
            HSQLDBDatabaseUpdates.updateDatabase(connection);
            assertEquals(54, HSQLDBDatabaseUpdates.fetchDatabaseVersion(connection));
            SortedMap<Integer, String> columns = new TreeMap<>();
            try (ResultSet indexes = connection.getMetaData().getIndexInfo(null, "PUBLIC", "ARBITRARYRESOURCESCACHE", false, false)) {
                while (indexes.next()) {
                    if ("ARBITRARYRESOURCESSERVICECREATEDINDEX".equals(indexes.getString("INDEX_NAME")))
                        columns.put(indexes.getInt("ORDINAL_POSITION"), indexes.getString("COLUMN_NAME"));
                }
            }
            assertEquals(Arrays.asList("SERVICE", "CREATED_WHEN"), new ArrayList<>(columns.values()));
            try (Statement statement = connection.createStatement();
                 ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM ArbitraryResourcesCache")) {
                assertTrue(rows.next());
                assertEquals(3, rows.getInt(1));
            }
            // Reopening/updating a migrated schema must not attempt to recreate the index.
            HSQLDBDatabaseUpdates.updateDatabase(connection);
            assertEquals(54, HSQLDBDatabaseUpdates.fetchDatabaseVersion(connection));
            try (Statement statement = connection.createStatement()) { statement.execute("SHUTDOWN"); }
        }
    }
}
