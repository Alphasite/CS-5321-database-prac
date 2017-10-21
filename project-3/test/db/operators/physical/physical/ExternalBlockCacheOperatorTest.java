package db.operators.physical.physical;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

public class ExternalBlockCacheOperatorTest {
    TableInfo table;

    @Before
    public void setUp() throws Exception {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        table = database.getTable("Sailors");
    }

    @Test
    public void seek() throws Exception {
        BinaryTupleReader reader = BinaryTupleReader.get(table.file);

        List<Tuple> tuples = new ArrayList<>();

        Tuple next;
        while ((next = reader.next()) != null) {
            tuples.add(next);
        }

        ScanOperator scan = new ScanOperator(table);

        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan, Files.createTempDirectory("test-external-cache"));

        for (int i = 0; i < tuples.size(); i++) {
            cache.seek(i);
            assertThat("Tuple " + i, cache.getNextTuple(), equalTo(tuples.get(i)));
        }

        reader.seek(0);

        cache.seek(tuples.size() - 1);
        assertThat(cache.getNextTuple(), equalTo(tuples.get(tuples.size() - 1)));
        assertThat(cache.getNextTuple(), is(nullValue()));
        cache.seek(0);
        assertThat(cache.getNextTuple(), equalTo(tuples.get(0)));
        assertThat(cache.getNextTuple(), is(notNullValue()));
        cache.seek(10);
        assertThat(cache.getNextTuple(), equalTo(tuples.get(10)));
        assertThat(cache.getNextTuple(), is(notNullValue()));
        cache.seek(999);
        assertThat(cache.getNextTuple(), equalTo(tuples.get(999)));
        assertThat(cache.getNextTuple(), is(nullValue()));
    }

    @Test
    public void getNextTuple() throws Exception {
        ScanOperator scan = new ScanOperator(table);
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan, Files.createTempDirectory("test-external-cache"));

        ScanOperator refScan = new ScanOperator(table);

        TestUtils.compareTuples(refScan, cache);
    }

    @Test
    public void getHeader() throws Exception {
        ScanOperator scan = new ScanOperator(table);
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan, Files.createTempDirectory("test-external-cache"));

        assertThat(scan.getHeader(), equalTo(cache.getHeader()));
    }

    @Test
    public void reset() throws Exception {
        ScanOperator scan = new ScanOperator(table);
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan, Files.createTempDirectory("test-external-cache"));

        ScanOperator refScan = new ScanOperator(table);

        TestUtils.compareTuples(refScan, cache);

        refScan = new ScanOperator(table);
        cache.reset();

        TestUtils.compareTuples(refScan, cache);
    }

}