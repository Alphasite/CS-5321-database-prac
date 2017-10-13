package db;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.datastore.tuple.string.StringTupleWriter;
import db.operators.physical.Operator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.fail;

/**
 * A collection of utility methods for the testing suite.
 */
public class TestUtils {

    /**
     * A method to compare the tuple output of two db.operators, e.g. a reference and processed.
     *
     * @param reference The expected output operator
     * @param tested    The generated operator
     */
    public static void compareTuples(Operator reference, Operator tested) {
        int i = 0;
        while (true) {
            Tuple ref = reference.getNextTuple();
            Tuple test = tested.getNextTuple();

            if (ref == null && test == null) {
                break;
            } else if (test == null) {
                fail("output has fewer tuples (" + i + ") than expected");
            } else if (ref == null) {
                fail("output has more tuples (" + i + ") than expected");
            } else {
                assertThat(test, equalTo(ref));
            }

            i++;
        }

        System.out.println("ALL OKAY: checked " + i + " rows.");
    }

    /**
     * A method to compare the tuple output of two db.operators, e.g. a reference and processed.
     * <p>
     * This maintains an internal buffer to enable unordered comparisons.
     *
     * @param reference The expected output operator
     * @param tested    The generated operator
     */
    public static void unorderedCompareTuples(Operator reference, Operator tested) {
        int referenceTupleCount = 0;
        Set<String> referenceTuples = new HashSet<>();
        List<String> testedTuples = new ArrayList<>();

        Tuple tuple;

        while ((tuple = reference.getNextTuple()) != null) {
            referenceTupleCount += 1;
            referenceTuples.add(tuple.toString());
        }

        while ((tuple = tested.getNextTuple()) != null) {
            testedTuples.add(tuple.toString());
        }

        if (referenceTupleCount > testedTuples.size()) {
            assertThat("output has fewer tuples than expected", testedTuples.size(), equalTo(referenceTuples.size()));
        }

        if (referenceTupleCount < testedTuples.size()) {
            assertThat("output has more tuples than expected", testedTuples.size(), equalTo(referenceTuples.size()));
        }

        int i = 0;
        for (String testedTuple : testedTuples) {
            assertThat("Tuple " + testedTuple + " not found in reference", referenceTuples.contains(testedTuple), equalTo(true));
            i++;
        }

        System.out.println("ALL OKAY: checked " + i + " rows.");
    }

    public static PlainSelect parseQuery(String query) {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
        PlainSelect select;

        try {
            select = (PlainSelect) parser.Select().getSelectBody();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }

        return select;
    }

    public static int countNotNullTuples(Operator op) {
        int i = 0;
        Tuple record;
        while ((record = op.getNextTuple()) != null) {
            i += 1;

            assertThat(record, notNullValue());
        }

        return i;
    }


    public static void generateRandomData(String dbFolderPath, int numRows, int randRange) throws IOException {
        Random rand = new Random();
        List<String> schema = Arrays.asList("Sailors A B C", "Boats D E F", "Reserves G H");
        TableHeader header = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));

        // create db folder if it doesn't already exist
        Path dbFolder = Paths.get(dbFolderPath);
        Files.createDirectories(dbFolder);

        // write schema.txt file
        Path schemaFile = dbFolder.resolve("schema.txt");
        Files.write(schemaFile, schema, Charset.forName("UTF-8"));

        // create data folder if it doesn't already exist
        Path dataFolder = dbFolder.resolve("data");
        Files.createDirectories(dataFolder);

        BinaryTupleWriter binaryWriter;
        StringTupleWriter stringWriter;
        PrintStream sqlWriter;
        String sqlTemplate;

        // generate Sailors
        binaryWriter = BinaryTupleWriter.get(header, dataFolder.resolve("Sailors"));
        stringWriter = StringTupleWriter.get(dataFolder.resolve("Sailors_humanreadable"));
        sqlWriter = new PrintStream(new FileOutputStream(dataFolder.resolve("Sailors_sql.sql").toFile()));
        sqlTemplate = "INSERT INTO Sailors (A, B, C) VALUES (%d, %d, %d);\n";

        sqlWriter.println("DROP TABLE IF EXISTS Sailors;");
        sqlWriter.println("CREATE TABLE Sailors (A integer, B integer, C integer);");
        for (int i = 0; i < numRows; i++) {
            int val1 = rand.nextInt(randRange);
            int val2 = rand.nextInt(randRange);
            int val3 = rand.nextInt(randRange);
            Tuple t = new Tuple(Arrays.asList(val1, val2, val3));

            binaryWriter.write(t);
            stringWriter.write(t);
            sqlWriter.printf(sqlTemplate, val1, val2, val3);
        }
        binaryWriter.flush();
        binaryWriter.close();
        stringWriter.flush();
        stringWriter.close();
        sqlWriter.close();


        // generate Boats
        binaryWriter = BinaryTupleWriter.get(header, dataFolder.resolve("Boats"));
        stringWriter = StringTupleWriter.get(dataFolder.resolve("Boats_humanreadable"));
        sqlWriter = new PrintStream(new FileOutputStream(dataFolder.resolve("Boats_sql.sql").toFile()));
        sqlTemplate = "INSERT INTO Boats (D, E, F) VALUES (%d, %d, %d);\n";

        sqlWriter.println("DROP TABLE IF EXISTS Boats;");
        sqlWriter.println("CREATE TABLE Boats (D integer, E integer, F integer);");
        for (int i = 0; i < numRows; i++) {
            int val1 = rand.nextInt(randRange);
            int val2 = rand.nextInt(randRange);
            int val3 = rand.nextInt(randRange);
            Tuple t = new Tuple(Arrays.asList(val1, val2, val3));

            binaryWriter.write(t);
            stringWriter.write(t);
            sqlWriter.printf(sqlTemplate, val1, val2, val3);
        }
        binaryWriter.flush();
        binaryWriter.close();
        stringWriter.flush();
        stringWriter.close();
        sqlWriter.close();


        // generate Reserves
        binaryWriter = BinaryTupleWriter.get(header, dataFolder.resolve("Reserves"));
        stringWriter = StringTupleWriter.get(dataFolder.resolve("Reserves_humanreadable"));
        sqlWriter = new PrintStream(new FileOutputStream(dataFolder.resolve("Reserves_sql.sql").toFile()));
        sqlTemplate = "INSERT INTO Reserves (G, H) VALUES (%d, %d);\n";

        sqlWriter.println("DROP TABLE IF EXISTS Reserves;");
        sqlWriter.println("CREATE TABLE Reserves (G integer, H integer);");
        for (int i = 0; i < numRows; i++) {
            int val1 = rand.nextInt(randRange);
            int val2 = rand.nextInt(randRange);
            Tuple t = new Tuple(Arrays.asList(val1, val2));

            binaryWriter.write(t);
            stringWriter.write(t);
            sqlWriter.printf(sqlTemplate, val1, val2);
        }
        binaryWriter.flush();
        binaryWriter.close();
        stringWriter.flush();
        stringWriter.close();
        sqlWriter.close();

    }

    public static BufferedReader executeBashCmd(String command) throws IOException, InterruptedException {
        Process proc = new ProcessBuilder("/bin/bash", "-c", command).start();
        proc.waitFor();
        return new BufferedReader(new InputStreamReader(proc.getInputStream()));
    }
}
