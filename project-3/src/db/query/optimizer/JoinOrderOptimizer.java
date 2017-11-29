package db.query.optimizer;

import db.Utilities.UnionFind;
import db.Utilities.Utilities;
import db.operators.logical.LogicalJoinOperator;
import db.operators.logical.LogicalOperator;
import db.operators.logical.LogicalScanOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinOrderOptimizer {
    /**
     * Map table identifiers to useful info for optimization
     */
    private Map<String, Relation> relationsToJoin;

    private UnionFind constraints;
    private JoinPlan bestPlan;

    public JoinOrderOptimizer(LogicalJoinOperator join) {
        this.constraints = join.getUnionFind();

        this.relationsToJoin = new HashMap<>();

        // Load statistics for base tables
        for (LogicalOperator source : join.getChildren()) {
            LogicalScanOperator scan = Utilities.getLeafScan(source);
            String tableId = scan.getTableAlias();
            int tupleCount = scan.getTable().getStats().count;

            VValues vvalues = new VValues(scan.getHeader(), scan.getTable().getStats());
            vvalues.updateConstraints(constraints);

            this.relationsToJoin.put(tableId, new Relation(tableId, tupleCount, vvalues, source));
        }
    }

    /**
     * Recursively enumerate all possible join orders and evaluate their cost, preserving the minimum one.
     * This implementation is efficient because it keeps previously evaluated deep trees on the stack.
     */
    public JoinPlan computeBestJoinOrder() {
        computeBestPlan(new ArrayList<>(relationsToJoin.keySet()), null);

        return bestPlan;
    }

    private void computeBestPlan(List<String> toJoin, JoinPlan currentPlan) {
        if (toJoin.size() == 0) {
            // Evaluate total plan cost
            if (bestPlan == null || currentPlan.getCost() < bestPlan.getCost()) {
                bestPlan = currentPlan;
            }
        } else {
            // Continue iterating
            for (String relation : toJoin) {
                JoinPlan plan;
                if (currentPlan == null) {
                    plan = new JoinPlan(this.relationsToJoin.get(relation));
                } else {
                    plan = new JoinPlan(currentPlan, this.relationsToJoin.get(relation), this.constraints.getSets());
                }

                List<String> nextIter = new ArrayList<>(toJoin);
                nextIter.remove(relation);

                computeBestPlan(nextIter, plan);
            }
        }
    }
}
