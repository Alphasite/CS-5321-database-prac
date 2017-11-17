package db.operators.physical;

import db.operators.physical.bag.JoinOperator;
import db.operators.physical.bag.ProjectionOperator;
import db.operators.physical.bag.SelectionOperator;
import db.operators.physical.extended.DistinctOperator;
import db.operators.physical.extended.SortOperator;
import db.operators.physical.physical.IndexScanOperator;
import db.operators.physical.physical.ScanOperator;
import db.operators.physical.utility.BlockCacheOperator;

/**
 * A visitor for the physical operators.
 */
public interface PhysicalTreeVisitor {
    /**
     * @param node the join node to visit.
     */
    void visit(JoinOperator node);

    /**
     * @param node the selection node to visit.
     */
    void visit(SelectionOperator node);

    /**
     * @param node the projection node to visit.
     */
    void visit(ProjectionOperator node);

    /**
     * @param node the scan node to visit.
     */
    void visit(ScanOperator node);

    /**
     * @param node the distinct node to visit.
     */
    void visit(DistinctOperator node);

    /**
     * @param node the sort node to visit.
     */
    void visit(SortOperator node);

    /**
     * @param node the cache node to visit.
     */
    void visit(BlockCacheOperator node);

    /**
     * @param node the index scan node to visit.
     */
    void visit(IndexScanOperator node);
}
