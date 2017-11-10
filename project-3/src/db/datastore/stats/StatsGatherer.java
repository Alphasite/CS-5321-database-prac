package db.datastore.stats;

import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.operators.physical.physical.ScanOperator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class StatsGatherer {

    public static List<TableStats> gatherStats(Database database) {
        return database.getTables().stream()
                .map(TableInfo::getStats)
                .collect(Collectors.toList());
    }

    public static TableStats gatherStats(TableInfo table) {
        ScanOperator operator = new ScanOperator(table);

        TableStats stats = new TableStats(table);

        while (operator.hasNextTuple()) {
            stats.count += 1;

            Tuple tuple = operator.getNextTuple();

            for (int i = 0; i < tuple.fields.size(); i++) {
                stats.minimums[i] = Math.min(stats.minimums[i], tuple.fields.get(i));
                stats.maximums[i] = Math.max(stats.maximums[i], tuple.fields.get(i));
            }
        }

        operator.close();

        return stats;
    }

    public static String buildStatsFile(Collection<TableStats> stats) {
        return stats.stream()
                .map(Object::toString)
                .collect(Collectors.joining("\n"));
    }

    public static void writeStatsFile(Path directory, String file) {
        try {
            Files.write(directory.resolve("stats.txt"), file.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
