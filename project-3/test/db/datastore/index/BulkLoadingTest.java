package db.datastore.index;

import db.DatabaseStructure;
import db.TestUtils;
import db.datastore.Database;
import db.datastore.IndexInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class BulkLoadingTest {

    private Database DB;
    private IndexInfo config;

    @Before
    public void init() throws IOException {
        DatabaseStructure path = new DatabaseStructure(TestUtils.NEW_SAMPLES_PATH);
        DB = Database.loadDatabase(path.db);

        config = new IndexInfo();
    }

    @Test
    public void testUnclusteredBulkLoading() {
        config.tableName = "Boats";
        config.attributeName = "E";
        config.treeOrder = 10;
        config.isClustered = false;

        Path referenceFile = TestUtils.EXPECTED_INDEXES.resolve("Boats.E");

        Path indexFile = BulkLoader.buildIndex(DB, config, TestUtils.NEW_DB_PATH.resolve("indexes"));

        assertEquals(true, compareBinary(referenceFile.toFile(), indexFile.toFile()));
    }

    @Test
    public void testClusteredBulkLoading() {
        config.tableName = "Sailors";
        config.attributeName = "A";
        config.treeOrder = 15;
        config.isClustered = true;

        Path referenceFile = TestUtils.EXPECTED_INDEXES.resolve("Sailors.A");

        Path indexFile = BulkLoader.buildIndex(DB, config, TestUtils.NEW_DB_PATH.resolve("indexes"));

        assertEquals(true, compareBinary(referenceFile.toFile(), indexFile.toFile()));
    }

    private boolean compareBinary(File f1, File f2) {
        if (f1.length() != f2.length()) {
            return false;
        }

        try {
            InputStream a = new BufferedInputStream(new FileInputStream(f1));
            InputStream b = new BufferedInputStream(new FileInputStream(f2));

            int v1 = 0, v2 = 0;
            while (v1 >= 0) {
                v1 = a.read();
                v2 = b.read();

                if (v1 != v2) {
                    return false;
                }
            }

            a.close();
            b.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }
}
