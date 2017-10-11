package db.operators.physical;

import db.operators.physical.bag.JoinOperator;
import db.operators.physical.bag.ProjectionOperator;
import db.operators.physical.bag.RenameOperator;
import db.operators.physical.bag.SelectionOperator;
import db.operators.physical.extended.DistinctOperator;
import db.operators.physical.extended.SortOperator;
import db.operators.physical.physical.ScanOperator;

public interface PhysicalTreeVisitor {
    void visit(JoinOperator node);

    void visit(RenameOperator node);

    void visit(SelectionOperator node);

    void visit(ProjectionOperator node);

    void visit(ScanOperator node);

    void visit(DistinctOperator node);

    void visit(SortOperator node);
}
