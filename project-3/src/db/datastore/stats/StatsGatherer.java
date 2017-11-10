package db.datastore.stats;

import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.operators.physical.physical.ScanOperator;

import java.util.Collection;
import java.util.stream.Collectors;

public class StatsGatherer {
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
}
