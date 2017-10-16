package db.datastore.tuple.string;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.operators.DummyOperator;
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
import static org.hamcrest.core.IsNull.notNullValue;

public class StringTupleWriterTest {
    private List<Tuple> tuples;
    private DummyOperator operator;
    private TableHeader header;
    private Path tempFile;
    private TableInfo tableInfo;

    @Before
    public void setUp() throws Exception {
        tuples = new ArrayList<>();
        tuples.add(new Tuple(Arrays.asList(1, 200, 50)));
        tuples.add(new Tuple(Arrays.asList(2, 200, 200)));
        tuples.add(new Tuple(Arrays.asList(3, 100, 105)));
        tuples.add(new Tuple(Arrays.asList(4, 100, 50)));
        tuples.add(new Tuple(Arrays.asList(5, 100, 500)));
        tuples.add(new Tuple(Arrays.asList(6, 300, 400)));
        header = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));
        operator = new DummyOperator(tuples, header);

        tempFile = Files.createTempFile("test", "BinaryWriter");

        tableInfo = new TableInfo(header, tempFile, false);

        BinaryTupleWriter.get(header, tempFile.toFile());
    }

    @Test
    public void write() throws Exception {
        StringTupleWriter writer = StringTupleWriter.get(tempFile.toFile());

        assertThat(writer, notNullValue());

        for (Tuple tuple : tuples) {
            writer.write(tuple);
        }

        writer.flush();
        writer.close();

        ScanOperator binaryInput = new ScanOperator(tableInfo);

        TestUtils.compareTuples(operator, binaryInput);
    }

    @Test
    public void writeLarge() throws Exception {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        TableInfo table = database.getTable("Sailors");

        BinaryTupleReader reader = BinaryTupleReader.get(table);

        StringTupleWriter writer = StringTupleWriter.get(tempFile.toFile());

        assertThat(writer, notNullValue());

        Tuple tuple;
        while ((tuple = reader.next()) != null) {
            writer.write(tuple);
        }

        writer.flush();
        writer.close();

        ScanOperator binaryInput = new ScanOperator(tableInfo);
        ScanOperator refScan = new ScanOperator(table);

        TestUtils.compareTuples(refScan, binaryInput);
    }
}