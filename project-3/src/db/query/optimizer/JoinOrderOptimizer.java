package db.query.optimizer;

import db.Utilities.UnionFind;
import db.Utilities.Utilities;
import db.operators.logical.LogicalJoinOperator;
import db.operators.logical.LogicalOperator;
import db.operators.logical.LogicalScanOperator;
import db.operators.physical.Operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinOrderOptimizer {

    private int bestCost;
    private JoinPlan bestPlan;

    private UnionFind constraints;

    /**
     * Map table identifiers to useful info for optimization
     */
    private Map<String, Relation> relationsToJoin;

    class Relation {
        String name;
        int tupleCount;
        VValues vvalues;
        LogicalOperator op;

        Relation(String name, int tupleCount, VValues vvalues, LogicalOperator op) {
            this.name = name;
            this.tupleCount = tupleCount;
            this.vvalues = vvalues;
            this.op = op;
        }
    }

    // Private variables used for recursion

    private int currentRelationSize;
    private int currentPlanCost;

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

    public Operator buildOptimizedPlan(List<Operator> sources) {
        computeBestPlan(new ArrayList<>(), new ArrayList<>(relationsToJoin.keySet()), 0);

        return null;
    }

    private void computeBestPlan(List<String> joined, List<String> toJoin, int planCost) {
        if (toJoin.size() == 0) {
            if (planCost < bestCost) {
                bestCost = planCost;
                bestPlan = new ArrayList<>(joined); // Create a copy to prevent side-effects
            }
        } else {
            // Continue iterating
            for (String relation : toJoin) {
                VValues relationVValues = relationsToJoin.get(relation).vvalues;
                int joinCost = 0;

                // Apply modifications
                joined.add(relation);
                toJoin.remove(relation);

                computeBestPlan(joined, toJoin, planCost + joinCost);

                // Revert modifications
                toJoin.add(relation);
                joined.remove(relation);
            }
        }
    }

}
