package db.operators.logical;

import java.util.List;

public class LogicalSortOperator {
    private final LogicalOperator source;
    private List<Integer> tupleSortPriorityIndex;

    public LogicalSortOperator(LogicalOperator source, List<Integer> tupleSortPriorityIndex) {
        this.source = source;
        this.tupleSortPriorityIndex = tupleSortPriorityIndex;
    }
}
