package db.datastore.tuple.string;

import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;

public class StringTupleReaderTest {
    TableInfo table_bin;
    TableInfo table;

    @Before
    public void setUp() throws Exception {
        Path inputDir = Paths.get("resources/samples/input/db").toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        table_bin = database.getTable("Sailors");
        table = new TableInfo(table_bin.header, Paths.get(table_bin.file.toString() + "_humanreadable"), false);
    }

    @Test
    public void next() throws Exception {
        StringTupleReader reader = StringTupleReader.get(table);

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