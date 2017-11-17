package db.operators.logical;

/**
 * A visitor which visits each of the logical operator types.
 */
public interface LogicalTreeVisitor {
    /**
     * @param node the join operator to visit.
     */
    void visit(LogicalJoinOperator node);

    /**
     * @param node the scan operator to visit.
     */
    void visit(LogicalScanOperator node);

    /**
     * @param node the select operator to visit.
     */
    void visit(LogicalSelectOperator node);

    /**
     * @param node the sort operator to visit.
     */
    void visit(LogicalSortOperator node);

    /**
     * @param node the project operator to visit.
     */
    void visit(LogicalProjectOperator node);

    /**
     * @param node the distinct operator to visit.
     */
    void visit(LogicalDistinctOperator node);
}
