package db.query.optimizer;

import db.operators.logical.LogicalOperator;

class Relation {
    String name;
    int tupleCount;
    int tupleSize;
    VValues vvalues;
    LogicalOperator op;

    Relation(String name, int tupleCount, VValues vvalues, LogicalOperator op) {
        this.name = name;
        this.tupleCount = tupleCount;
        this.tupleSize = op.getHeader().size();
        this.vvalues = vvalues;
        this.op = op;
    }
}