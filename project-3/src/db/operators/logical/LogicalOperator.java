package db.operators.logical;

import db.datastore.TableHeader;

/**
 * The super class for all logical operators.
 */
public interface LogicalOperator {

    /**
     * Get the schema for the tuples produced by this operator.
     *
     * @return The tuples schema.
     */
    TableHeader getHeader();

    /**
     * Accept a visitor and pass it this operator.
     *
     * @param visitor the visitor to visit.
     */
    void accept(LogicalTreeVisitor visitor);
}
