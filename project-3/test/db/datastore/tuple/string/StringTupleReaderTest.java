package db.datastore.tuple.string;

import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
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
        StringTupleReader reader = StringTupleReader.get(this.table.header, this.table.file);

        assertThat(reader, notNullValue());

        for (int i = 0; i < 1000; i++) {
            Tuple tuple = reader.next();
            assertThat(tuple, notNullValue());
            assertThat(tuple, instanceOf(Tuple.class));
            assertThat(tuple.fields.size(), equalTo(table.header.size()));
        }

        assertThat(reader.next(), nullValue());
        reader.close();
    }

    @Test
    public void seek() throws Exception {
        StringTupleReader reader = StringTupleReader.get(this.table.header, this.table.file);

        List<Tuple> tuples = new ArrayList<>();

        Tuple next;
        while ((next = reader.next()) != null) {
            tuples.add(next);
        }

        reader.seek(tuples.size() - 1);
        assertThat(reader.next(), equalTo(tuples.get(tuples.size() - 1)));
        assertThat(reader.next(), is(nullValue()));
        reader.seek(0);
        assertThat(reader.next(), equalTo(tuples.get(0)));
        assertThat(reader.next(), is(notNullValue()));
        reader.seek(10);
        assertThat(reader.next(), equalTo(tuples.get(10)));
        assertThat(reader.next(), is(notNullValue()));
        reader.seek(tuples.size() - 1);
        assertThat(reader.next(), equalTo(tuples.get(tuples.size() - 1)));
        assertThat(reader.next(), is(nullValue()));

        reader.close();
    }

}
