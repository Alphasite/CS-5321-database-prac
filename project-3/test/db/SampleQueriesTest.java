package db;

import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.*;

public class SampleQueriesTest {

    private static String OUTPUT_PATH = "resources/samples/output";
    private static String EXPECTED_PATH = "resources/samples/expected";

    @Test
    public void testSampleQueries() throws Exception {
        Project3.main(new String[0]);

        for (int i = 1; i <= 15; i++) {
            System.out.println("Checking file 'query" + i + "'...");

            File outputFile = new File(OUTPUT_PATH + File.separator + "query" + i);

            assertTrue("Error: file 'query" + i + "' is not in output directory", outputFile.exists());

            File expectedFile = new File(EXPECTED_PATH + File.separator + "query" + i);

            BinaryTupleReader outputReader = new BinaryTupleReader(null, new FileInputStream(outputFile).getChannel());
            BinaryTupleReader expectedReader = new BinaryTupleReader(null, new FileInputStream(expectedFile).getChannel());

            Tuple outputTuple;
            Tuple expectedTuple;

            while (true) {
                outputTuple = outputReader.next();
                expectedTuple = expectedReader.next();

                if (outputTuple == null && expectedTuple == null) {
                    break;
                } else if (outputTuple == null) {
                    fail("Error: output file has fewer pages than expected");
                } else if (expectedTuple == null) {
                    fail("Error: output file has more pages than expected");
                } else {
                    assertEquals(expectedTuple, outputTuple);
                }
            }
        }

    }

}