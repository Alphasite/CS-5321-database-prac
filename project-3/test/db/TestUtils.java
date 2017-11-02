package db;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;
import db.datastore.tuple.binary.BinaryTupleReaderWriter;
import db.datastore.tuple.string.StringTupleWriter;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.query.QueryBuilder;
import db.query.visitors.PhysicalPlanBuilder;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
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
                assertThat("output: " + i + " does not match.", test, equalTo(ref));
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
    public static Map<String, List<Tuple>> populateDatabase(Path dbFolder, List<String> queries, int numColumns, int randRange) {
        return populateDatabase(dbFolder, queries, numColumns, randRange, true);
    }

    public static Map<String, List<Tuple>> populateDatabase(Path dbFolder, List<String> queries, int numColumns, int randRange, boolean generateSamples) {
        try {
            List<String> schema = Arrays.asList("Sailors A B C", "Boats D E F", "Reserves G H");

            // create db folder if it doesn't already exist
            Files.createDirectories(dbFolder);

            // write schema.txt file
            Path schemaFile = dbFolder.resolve("schema.txt");
            Files.write(schemaFile, schema, Charset.forName("UTF-8"));

            // create data folder if it doesn't already exist
            Path dataFolder = dbFolder.resolve("data");
            Files.createDirectories(dataFolder);

            List<Tuple> sailors = generateTuples(3, numColumns, randRange);
            List<Tuple> boats = generateTuples(3, numColumns, randRange);
            List<Tuple> reserves = generateTuples(2, numColumns, randRange);

            dumpTable(sailors, dataFolder, "Sailors", Arrays.asList("A", "B", "C"));
            dumpTable(boats, dataFolder, "Boats", Arrays.asList("D", "E", "F"));
            dumpTable(reserves, dataFolder, "Reserves", Arrays.asList("G", "H"));

            Map<String, List<Tuple>> results;

            if (generateSamples) {
                return getSampleResult(queries, sailors, boats, reserves);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void dumpTable(List<Tuple> tuples, Path dataFolder, String tableName, List<String> columns) {
        TupleWriter stringWriter = StringTupleWriter.get(dataFolder.resolve(tableName + "_humanreadable"));

        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            aliases.add(tableName);
        }

        TupleWriter binaryWriter = BinaryTupleReaderWriter.get(
                new TableHeader(aliases, columns),
                dataFolder.resolve(tableName)
        );

        for (Tuple tuple : tuples) {
            stringWriter.write(tuple);
            binaryWriter.write(tuple);
        }

        stringWriter.flush();
        stringWriter.close();

        binaryWriter.flush();
        binaryWriter.close();
    }

    public static Operator getQueryPlan(Path dbFolder, String query, PhysicalPlanConfig config) {
        Database DB = Database.loadDatabase(dbFolder);
        QueryBuilder builder = new QueryBuilder(DB);
        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(config, dbFolder);

        try {
            CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
            Statement statement = parser.Statement();

            PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();
            LogicalOperator logicalPlan = builder.buildQuery(select);

            return physicalBuilder.buildFromLogicalTree(logicalPlan);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static Map<String, List<Tuple>> getSampleResult(List<String> queries, List<Tuple> sailors, List<Tuple> boats, List<Tuple> reserves) {
        try {
            Class.forName("org.h2.Driver");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:test");) {
            c.createStatement().execute("CREATE TABLE Sailors (A INT, B INT, C INT)");
            c.createStatement().execute("CREATE TABLE Boats (D INT, E INT, F INT)");
            c.createStatement().execute("CREATE TABLE Reserves (G INT, H INT)");

            populateSailors(c, sailors);
            populateBoats(c, boats);
            populateReserves(c, reserves);

            Map<String, List<Tuple>> queryResults = new HashMap<>();

            for (String query : queries) {
                ResultSet resultSet = c.createStatement().executeQuery(query);

                int columnCount = resultSet.getMetaData().getColumnCount();

                List<Tuple> tuples = new ArrayList<>();

                while (resultSet.next()) {
                    List<Integer> columns = new ArrayList<>(columnCount);

                    for (int i = 0; i < columnCount; i++) {
                        columns.add(resultSet.getInt(i + 1));
                    }

                    tuples.add(new Tuple(columns));
                }

                queryResults.put(query, tuples);
            }

            return queryResults;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void populateSailors(Connection c, List<Tuple> tuples) throws SQLException {
        populateTuples(c, "INSERT INTO Sailors (A, B, C) VALUES (?, ?, ?)", tuples);
    }

    public static void populateBoats(Connection c, List<Tuple> tuples) throws SQLException {
        populateTuples(c, "INSERT INTO Boats (D, E, F) VALUES (?, ?, ?)", tuples);
    }

    public static void populateReserves(Connection c, List<Tuple> tuples) throws SQLException {
        populateTuples(c, "INSERT INTO Reserves (G, H) VALUES (?, ?)", tuples);
    }

    private static void populateTuples(Connection c, String sql, List<Tuple> tuples) throws SQLException {
        PreparedStatement statement = c.prepareStatement(sql);

        for (Tuple tuple : tuples) {
            for (int i = 0; i < tuple.fields.size(); i++) {
                statement.setInt(i + 1, tuple.fields.get(i));
            }

            statement.executeUpdate();
        }
    }

    private static List<Tuple> generateTuples(int numColumns, int numRows, int randRange) {
        Random rand = new Random();

        List<Tuple> tuples = new ArrayList<>(numRows);

        for (int i = 0; i < numRows; i++) {
            List<Integer> columns = new ArrayList<>(numColumns);

            for (int j = 0; j < numColumns; j++) {
                columns.add(rand.nextInt(randRange));
            }

            tuples.add(new Tuple(columns));
        }

        return tuples;
    }
}
