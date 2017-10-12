package db.operators.logical;

public interface LogicalTreeVisitor {
    void visit(LogicalJoinOperator node);

    void visit(LogicalRenameOperator node);

    void visit(LogicalScanOperator node);

    void visit(LogicalSelectOperator node);

    void visit(LogicalSortOperator node);

    void visit(LogicalProjectOperator node);

    void visit(LogicalDistinctOperator node);
}
