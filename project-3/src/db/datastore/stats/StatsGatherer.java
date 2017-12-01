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

/**
 * Static methods to generate useful statistics about a relation stored on disk :
 * number of records, range of values for each attribute
 */
public class StatsGatherer {

    /**
     * Generate statistics for each Table tracked by a {@link Database}
     */
    public static List<TableStats> gatherStats(Database database) {
        return database.getTables().stream()
                .map(TableInfo::getStats)
                .collect(Collectors.toList());
    }

    /**
     * Generate statistics for a Table. This operation keeps bounded state.
     */
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

    /**
     * Format a collection of {@link TableStats} as a string.
     *
     * @param stats
     * @return Line separated : tableName count [attribute.name,min,max]
     */
    public static String asString(Collection<TableStats> stats) {
        return stats.stream()
                .map(TableStats::toString)
                .collect(Collectors.joining("\n"));
    }

    /**
     * Write the stats text to the correct file.
     *
     * @param directory the directory which contains the stats file.
     * @param contents  the contents of the stats file.
     */
    public static void writeStatsFile(Path directory, String contents) {
        try {
            Files.write(directory.resolve("stats.txt"), contents.getBytes(), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
