package db.operators.physical.physical;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.IndexInfo;
import db.datastore.TableInfo;
import db.datastore.index.BTree;
import db.datastore.index.BulkLoader;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;

@SuppressWarnings("Duplicates")
public class IndexScanOperatorTest {
    private final Path expectedIndexes = TestUtils.EXPECTED_INDEXES.resolve("Boats.E");
    private final Path inputDir = TestUtils.NEW_DB_PATH;
    private final Path indexesDir = inputDir.resolve("indexes");

    private BTree boatsIndexTree;
    private TableInfo boatsTable;
    private IndexScanOperator boatsOperator;
    private IndexScanOperator boatsOperator2;
    private int numberOfTuples;

    private BTree sailorsIndexTree;
    private TableInfo sailorsTable;
    private IndexScanOperator sailorsOperator;

    private int min = 42;
    private int max = 9994;
    private IndexInfo boatIndex;
    private IndexInfo sailorIndex;

    @Before
    public void setUp() throws Exception {
        Database database = Database.loadDatabase(inputDir);
        boatsTable = database.getTable("Boats");
        boatIndex = boatsTable.indices.get(0);
        boatsIndexTree = BTree.createTree(expectedIndexes);
        boatsOperator = new IndexScanOperator(boatsTable, boatIndex, boatsIndexTree, min, max);
        numberOfTuples = 10000 - 4 - 32;

        Path boatsIndexFile2 = BulkLoader.buildIndex(database, boatIndex, indexesDir);
        BTree boatsIndexTree2 = BTree.createTree((boatsIndexFile2));
        boatsOperator2 = new IndexScanOperator(boatsTable, boatIndex, boatsIndexTree2, 9982, null);

        sailorIndex = database.getTable("Sailors").indices.get(0);
        Path sailorsIndexFile = BulkLoader.buildIndex(database, sailorIndex, indexesDir);
        sailorsTable = database.getTable("Sailors");
        sailorsIndexTree = BTree.createTree(sailorsIndexFile);
        sailorsOperator = new IndexScanOperator(sailorsTable, sailorIndex, sailorsIndexTree, null, 13);
    }

    @After
    public void tearDown() throws Exception {
        boatsOperator.close();
        sailorsOperator.close();
    }

    @Test
    public void fullScan() throws Exception {
        boatsOperator.close();
        boatsOperator = new IndexScanOperator(boatsTable, boatIndex, boatsIndexTree, 0, 10000);

        TestUtils.unorderedCompareTuples(new ScanOperator(boatsTable), boatsOperator);

        boatsOperator.reset();

        while (boatsOperator.hasNextTuple()) {
            Tuple tuple = boatsOperator.getNextTuple();
            assertThat("not null", tuple, notNullValue());
            assertThat("class", tuple, instanceOf(Tuple.class));
            assertThat("size", tuple.fields.size(), is(3));
        }

        boatsOperator.close();
    }

    @Test
    public void number() throws Exception {
        while (boatsOperator.hasNextTuple()) {
            Tuple tuple = boatsOperator.getNextTuple();
            assertThat("not null", tuple, notNullValue());
            assertThat("class", tuple, instanceOf(Tuple.class));
            assertThat("size", tuple.fields.size(), is(3));
        }
    }

    @Test
    public void next() throws Exception {
        assertThat(boatsOperator, notNullValue());

        for (int i = 0; i < numberOfTuples; i++) {
            assertThat("tuple: " + i, boatsOperator.hasNextTuple(), equalTo(true));

            Tuple peek = boatsOperator.peekNextTuple();
            Tuple next = boatsOperator.getNextTuple();

            assertThat(peek, notNullValue());
            assertThat(peek, instanceOf(Tuple.class));
            assertThat(peek.fields.size(), equalTo(boatsTable.header.size()));

            assertThat(next, notNullValue());
            assertThat(next, instanceOf(Tuple.class));
            assertThat(next.fields.size(), equalTo(boatsTable.header.size()));

            assertThat(peek, equalTo(next));
        }

        assertThat(boatsOperator.hasNextTuple(), equalTo(false));
        assertThat(boatsOperator.peekNextTuple(), nullValue());
        assertThat(boatsOperator.getNextTuple(), nullValue());
        assertThat(boatsOperator.hasNextTuple(), equalTo(false));
    }

    @Test
    public void reset() throws Exception {
        List<Tuple> tuples = new ArrayList<>();

        while (boatsOperator.hasNextTuple()) {
            tuples.add(boatsOperator.getNextTuple());
        }

        assertThat(tuples.size(), is(numberOfTuples));

        for (int i = 0; i < 100; i++) {
            System.out.println("Iteration: " + i);

            boatsOperator.reset();
            TestUtils.compareTuples(new DummyOperator(tuples, boatsOperator.getHeader()), boatsOperator);
        }

        boatsOperator.close();
    }

    @Test
    public void getHeader() throws Exception {
        assertThat(boatsOperator.getHeader(), is(boatsTable.header));
    }

    @Test
    public void unclusteredIndexTest() {
        Tuple[] expected = {
                new Tuple(Arrays.asList(7673, 9982, 4067)),
                new Tuple(Arrays.asList(1285, 9984, 2328)),
                new Tuple(Arrays.asList(2292, 9985, 2115)),
                new Tuple(Arrays.asList(7574, 9985, 7989)),
                new Tuple(Arrays.asList(7432, 9986, 5594)),
                new Tuple(Arrays.asList(2016, 9987, 1358)),
                new Tuple(Arrays.asList(391, 9987, 9223)),
                new Tuple(Arrays.asList(8279, 9988, 9012)),
                new Tuple(Arrays.asList(236, 9989, 6496)),
                new Tuple(Arrays.asList(871, 9990, 9051)),
                new Tuple(Arrays.asList(743, 9991, 9305)),
                new Tuple(Arrays.asList(2360, 9991, 1359)),
                new Tuple(Arrays.asList(3836, 9991, 5023)),
                new Tuple(Arrays.asList(8805, 9991, 9155)),
                new Tuple(Arrays.asList(5022, 9994, 7983)),
                new Tuple(Arrays.asList(152, 9994, 7147)),
                new Tuple(Arrays.asList(6437, 9998, 2317)),
                new Tuple(Arrays.asList(8439, 9998, 6378)),
                new Tuple(Arrays.asList(4461, 9999, 4000)),
                new Tuple(Arrays.asList(5317, 9999, 266)),
        };

        TestUtils.compareTuples(new DummyOperator(Arrays.asList(expected), boatsOperator2.getHeader()), boatsOperator2);
    }

    @Test
    public void clusteredIndexTest() {
        Tuple[] expected = {
                new Tuple(Arrays.asList(1, 1517, 5260)),
                new Tuple(Arrays.asList(2, 2956, 3580)),
                new Tuple(Arrays.asList(2, 4052, 6863)),
                new Tuple(Arrays.asList(2, 9310, 7776)),
                new Tuple(Arrays.asList(3, 9406, 1086)),
                new Tuple(Arrays.asList(3, 7984, 781)),
                new Tuple(Arrays.asList(3, 9901, 4977)),
                new Tuple(Arrays.asList(3, 206, 9771)),
                new Tuple(Arrays.asList(4, 1047, 2743)),
                new Tuple(Arrays.asList(4, 4005, 1168)),
                new Tuple(Arrays.asList(5, 2230, 9911)),
                new Tuple(Arrays.asList(7, 1501, 8347)),
                new Tuple(Arrays.asList(8, 4437, 838)),
                new Tuple(Arrays.asList(8, 7075, 8898)),
                new Tuple(Arrays.asList(8, 7260, 262)),
                new Tuple(Arrays.asList(8, 3868, 6975)),
                new Tuple(Arrays.asList(10, 4957, 6258)),
                new Tuple(Arrays.asList(10, 4685, 5770)),
                new Tuple(Arrays.asList(11, 3823, 1524)),
                new Tuple(Arrays.asList(13, 2347, 5827))
        };

        TestUtils.compareTuples(new DummyOperator(Arrays.asList(expected), sailorsOperator.getHeader()), sailorsOperator);
    }
}