package db;

import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.operators.physical.physical.ScanOperator;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Project3Test {
    @Before
    public void setUp() throws Exception {
        Project3.DUMP_TO_CONSOLE = false;

        Path tmp = Paths.get("resources/samples/tmp");

        if (!Files.exists(tmp)) {
            Files.createDirectory(tmp);
        }

        Path output = Paths.get("resources/samples/output");

        if (!Files.exists(output)) {
            Files.createDirectory(output);
        }
    }

    @Test
    public void main() throws Exception {
        Project3.main(new String[]{"resources/samples/input/config.txt"});

        int[] numColumns = {
                3, 1, 2, 2, 3,
                1, 3, 5, 8, 8,
                3, 6, 2, 8, 8
        };

        for (int i = 1; i <= 17; i++) {
            Path expected = Paths.get("resources/samples/expected/query" + i);
            Path result = Paths.get("resources/samples/output/query" + i);

            List<String> tables = new ArrayList<>();
            List<String> columns = new ArrayList<>();

            for (int j = 0; j < numColumns.length; j++) {
                tables.add("table");
                columns.add("" + j);
            }

            TableHeader header = new TableHeader(tables, columns);

            System.out.println("Comparing: " + i);

            TestUtils.unorderedCompareTuples(
                    new ScanOperator(new TableInfo(header, expected, true)),
                    new ScanOperator(new TableInfo(header, result, true))
            );
        }
    }
}