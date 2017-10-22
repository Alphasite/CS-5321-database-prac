package db.operators.physical.utility;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.operators.physical.physical.ScanOperator;
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
    private TableInfo table;

    @Before
    public void setUp() throws Exception {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        table = database.getTable("Sailors");
    }

    @Test
    public void writeTupleToBuffer() throws Exception {
        ScanOperator scan = new ScanOperator(table);
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan.getHeader(), Files.createTempDirectory("test-external-cache"));

        int i = 0;
        Tuple next;
        while ((next = scan.getNextTuple()) != null) {
            cache.writeTupleToBuffer(next);
            i++;
        }

        cache.flush();

        System.out.println(i);

        ScanOperator refScan = new ScanOperator(table);
        ScanOperator bufferScan = new ScanOperator(new TableInfo(table.header, cache.getBufferFile(), true));

        TestUtils.compareTuples(refScan, bufferScan);

        refScan.reset();
        TestUtils.compareTuples(refScan, cache);

    }

    @Test
    public void writeSourceToBuffer() throws Exception {
        ScanOperator scan = new ScanOperator(table);
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan.getHeader(), Files.createTempDirectory("test-external-cache"));
        cache.writeSourceToBuffer(scan);
        cache.flush();

        ScanOperator refScan = new ScanOperator(table);
        ScanOperator bufferScan = new ScanOperator(new TableInfo(table.header, cache.getBufferFile(), true));

        TestUtils.compareTuples(refScan, bufferScan);

        refScan.reset();
        TestUtils.compareTuples(refScan, cache);
    }

    @Test
    public void writeSourceToBufferPageLimited() throws Exception {
        for (int i = 1; i <= 3; i++) {
            ExternalBlockCacheOperator cache = null;

            try {
                ScanOperator scan = new ScanOperator(table);
                cache = new ExternalBlockCacheOperator(scan.getHeader(), Files.createTempDirectory("test-external-cache"));
                cache.writeSourceToBuffer(scan, i);
                cache.flush();

                // Check the file directly.
                ScanOperator refScan = new ScanOperator(table);
                BlockCacheOperator refBlock = new BlockCacheOperator(refScan, i * Database.PAGE_SIZE);
                ScanOperator bufferScan = new ScanOperator(new TableInfo(table.header, cache.getBufferFile(), true));

                TestUtils.compareTuples(refBlock, bufferScan);

                // Check the operator
                refBlock.reset();
                cache.reset();
                TestUtils.compareTuples(refBlock, cache);
            } finally {
                if (cache != null) {
                    cache.delete();
                }
            }
        }
    }

    @Test
    public void delete() throws Exception {
        ScanOperator scan = new ScanOperator(table);
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan.getHeader(), Files.createTempDirectory("test-external-cache"));
        cache.writeSourceToBuffer(scan);

        assertThat(cache.getBufferFile(), notNullValue());
        assertThat(Files.exists(cache.getBufferFile()), is(true));
        cache.delete();
        assertThat(Files.exists(cache.getBufferFile()), is(false));
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

        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan.getHeader(), Files.createTempDirectory("test-external-cache"));
        cache.writeSourceToBuffer(scan);

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
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan.getHeader(), Files.createTempDirectory("test-external-cache"));
        cache.writeSourceToBuffer(scan);

        ScanOperator refScan = new ScanOperator(table);

        TestUtils.compareTuples(refScan, cache);
    }

    @Test
    public void getHeader() throws Exception {
        ScanOperator scan = new ScanOperator(table);
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan.getHeader(), Files.createTempDirectory("test-external-cache"));
        cache.writeSourceToBuffer(scan);

        assertThat(scan.getHeader(), equalTo(cache.getHeader()));
    }

    @Test
    public void reset() throws Exception {
        ScanOperator scan = new ScanOperator(table);
        ExternalBlockCacheOperator cache = new ExternalBlockCacheOperator(scan.getHeader(), Files.createTempDirectory("test-external-cache"));
        cache.writeSourceToBuffer(scan);
        cache.flush();

        ScanOperator refScan = new ScanOperator(table);

        TestUtils.compareTuples(refScan, cache);

        refScan = new ScanOperator(table);
        cache.reset();

        TestUtils.compareTuples(refScan, cache);
    }

}