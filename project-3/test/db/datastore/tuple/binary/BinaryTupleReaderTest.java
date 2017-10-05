package db.datastore.tuple.binary;

import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;

public class BinaryTupleReaderTest {
    @Test
    public void next() throws Exception {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        TableInfo table = database.getTable("Sailors");

        BinaryTupleReader reader = BinaryTupleReader.get(table);

        assertThat(reader, notNullValue());

        for (int i = 0; i < 1000; i++) {
            Tuple tuple = reader.next();
            assertThat(tuple, notNullValue());
            assertThat(tuple, instanceOf(Tuple.class));
            assertThat(tuple.fields.size(), equalTo(table.header.size()));
        }

        assertThat(reader.next(), nullValue());
    }

}