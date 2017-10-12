package db.operators.logical;

import db.datastore.TableHeader;

public interface LogicalOperator {

    /**
     * Get the schema for the tuples produced by this operator.
     *
     * @return The tuples schema.
     */
    TableHeader getHeader();

    void accept(LogicalTreeVisitor visitor);
}
