package db.query.optimizer;

/**
 * Abstract representation of a left-deep join tree, with heuristics to estimate its cost.
 */
class JoinPlan {

    private int estimatedTupleCount;
    private int cost;
    private int tableCount;

    private JoinPlan parentJoin;
    private String joinTable;

    public JoinPlan(JoinOrderOptimizer.Relation baseOp) {
        this.parentJoin = null;
        this.joinTable = baseOp.name;

        this.cost = 0;
        this.tableCount = 1;

        this.estimatedTupleCount = baseOp.tupleCount;
    }

    public JoinPlan(JoinPlan parentJoin, JoinOrderOptimizer.Relation joinWith) {
        this.parentJoin = parentJoin;
        this.joinTable = joinWith.name;

        this.tableCount = parentJoin.tableCount + 1;

        this.estimatedTupleCount;

        if (this.tableCount <= 2) {
            this.cost = 0;
        } else {
            this.cost = parentJoin.cost + parentJoin.estimatedTupleCount;
        }
    }
}
