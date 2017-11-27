package db.query.optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstract representation of a left-deep join tree, with heuristics to estimate its cost.
 */
class JoinPlan {

    int estimatedTupleCount;
    int cost;
    int tableCount;

    private JoinPlan parentJoin;
    private String joinTable;

    /**
     * Create a join plan on a single table
     */
    public JoinPlan(JoinOrderOptimizer.Relation baseOp) {
        this.parentJoin = null;
        this.joinTable = baseOp.name;

        this.cost = 0;
        this.tableCount = 1;

        this.estimatedTupleCount = baseOp.tupleCount;
    }

    /**
     * Create a join plan that uses parent as a left child and the given relation as a right child
     */
    public JoinPlan(JoinPlan parentJoin, JoinOrderOptimizer.Relation joinWith) {
        this.parentJoin = parentJoin;
        this.joinTable = joinWith.name;

        this.tableCount = parentJoin.tableCount + 1;

        // TODO: implement intermediate relation size estimation
//        this.estimatedTupleCount;

        if (parentJoin.tableCount < 2) {
            this.cost = 0;
        } else {
            this.cost = parentJoin.cost + parentJoin.estimatedTupleCount;
        }
    }

    public List<String> getJoins() {
        List<String> joins = new ArrayList<>();
        JoinPlan join = this;

        while (join != null) {
            joins.add(join.joinTable);
            join = join.parentJoin;
        }

        Collections.reverse(joins);
        return joins;
    }
}
