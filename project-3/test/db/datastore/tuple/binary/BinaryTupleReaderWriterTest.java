package db.datastore.tuple.binary;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReaderWriter;
import db.operators.physical.physical.ScanOperator;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class BinaryTupleReaderWriterTest {
    TableInfo table;
    Path output;

    @Before
    public void setUp() throws Exception {
        Path rootDir = Files.createTempDirectory("resourceFolder");
        Path inputDir = Paths.get("resources/samples/input/db");

        Database database = Database.loadDatabase(inputDir);
        table = database.getTable("Sailors");

        Path input = inputDir.resolve("data").resolve("Sailors");
        output = rootDir.resolve("Sailors");
        Files.copy(input, output);
    }

    @Test
    public void swap() throws Exception {
        TupleReaderWriter readerWriter = BinaryTupleReaderWriter.get(table.header, output);

        List<Tuple> tuples = new ArrayList<>();
        while (readerWriter.hasNext()) {
            tuples.add(readerWriter.next());
        }

        for (int i = 0; i < 500; i++) {
            int indexOfCurrent = i;
            int indexOfOther = 999 - i;

            Tuple pre = tuples.get(indexOfCurrent);
            Tuple post = tuples.get(indexOfOther);

            readerWriter.seek(indexOfCurrent);
            assertThat("sample: " + i, readerWriter.peek(), equalTo(pre));
            assertThat("sample: " + i, readerWriter.next(), equalTo(pre));

            readerWriter.seek(indexOfOther);
            assertThat("sample: " + i, readerWriter.peek(), equalTo(post));
            assertThat("sample: " + i, readerWriter.next(), equalTo(post));

            readerWriter.seek(indexOfCurrent);
            readerWriter.write(post);
            readerWriter.flush();

            readerWriter.seek(indexOfOther);
            readerWriter.write(pre);
            readerWriter.flush();

            readerWriter.seek(indexOfCurrent);
            assertThat("sample: " + i + " post", readerWriter.peek(), equalTo(post));
            assertThat("sample: " + i + " post", readerWriter.next(), equalTo(post));

            readerWriter.seek(indexOfOther);
            assertThat("sample: " + i + " pre", readerWriter.peek(), equalTo(pre));
            assertThat("sample: " + i + " pre", readerWriter.next(), equalTo(pre));

            tuples.set(indexOfCurrent, post);
            tuples.set(indexOfOther, pre);
        }

        readerWriter.flush();
        readerWriter.seek(0);

        BinaryTupleReaderWriter file = BinaryTupleReaderWriter.get(table.header, output);

        TestUtils.compareTuples(new ScanOperator(readerWriter), new ScanOperator(file));

        readerWriter.close();
        file.close();

        file = BinaryTupleReaderWriter.get(table.header, output);

        for (Tuple tuple : tuples) {
            assertThat(file.next(), equalTo(tuple));
        }

        assertThat(readerWriter.next(), equalTo(null));
        assertThat(file.next(), equalTo(null));
    }

    @Test
    public void readAfterWrite() throws Exception {
        BinaryTupleReaderWriter readerWriter = BinaryTupleReaderWriter.get(table.header, output);

        readerWriter.seek(0);
        readerWriter.write(new Tuple(Arrays.asList(1, 2, 3)));

        byte[] old = new byte[readerWriter.getBb().capacity()];
        readerWriter.getBb().get(old, 0, readerWriter.getBb().capacity());

        readerWriter.flush();

        readerWriter.seek(0);

        byte[] current = new byte[readerWriter.getBb().capacity()];
        readerWriter.getBb().get(current);

        assertThat(Arrays.equals(current, current), equalTo(true));
    }
}
