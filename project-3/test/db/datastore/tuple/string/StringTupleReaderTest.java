package db.datastore.tuple.string;

import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;
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
        TupleReader reader = StringTupleReader.get(this.table.header, this.table.file);

        assertThat(reader, notNullValue());

        for (int i = 0; i < 1000; i++) {
            assertThat("tuple: " + i, reader.hasNext(), equalTo(true));

            Tuple peek = reader.peek();
            Tuple next = reader.next();

            assertThat(peek, notNullValue());
            assertThat(peek, instanceOf(Tuple.class));
            assertThat(peek.fields.size(), equalTo(table.header.size()));

            assertThat(next, notNullValue());
            assertThat(next, instanceOf(Tuple.class));
            assertThat(next.fields.size(), equalTo(table.header.size()));

            assertThat(peek, equalTo(next));
        }

        assertThat(reader.peek(), nullValue());
        assertThat(reader.next(), nullValue());
        assertThat(reader.hasNext(), equalTo(false));


        reader.close();
    }

    @Test
    public void seek() throws Exception {
        TupleReader reader = StringTupleReader.get(this.table.header, this.table.file);

        List<Tuple> tuples = new ArrayList<>();

        Tuple next;
        while ((next = reader.next()) != null) {
            tuples.add(next);
        }

        for (int i = 0; i < tuples.size(); i++) {
            reader.seek(i);
            assertThat("Peek tuple " + i, reader.peek(), equalTo(tuples.get(i)));
            assertThat("Next tuple " + i, reader.next(), equalTo(tuples.get(i)));
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
        reader.seek(999);
        assertThat(reader.next(), equalTo(tuples.get(999)));
        assertThat(reader.next(), is(nullValue()));

        reader.close();
    }

    @Test
    public void randomSeek() throws Exception {
        TupleReader reader = StringTupleReader.get(this.table.header, this.table.file);

        List<Tuple> tuples = new ArrayList<>();

        Tuple next;
        while ((next = reader.next()) != null) {
            tuples.add(next);
        }

        for (int i = 0; i < 5000; i++) {
            int index = (int) (Math.random() * tuples.size());
            reader.seek(index);
            assertThat("Peek tuple " + index, reader.peek(), equalTo(tuples.get(index)));
            assertThat("Next tuple " + index, reader.next(), equalTo(tuples.get(index)));
        }

        for (int i = 0; i < tuples.size(); i++) {
            reader.seek(i);
            assertThat("Scan tuple: " + i, reader.next(), equalTo(tuples.get(i)));
        }

        reader.close();
    }
}
