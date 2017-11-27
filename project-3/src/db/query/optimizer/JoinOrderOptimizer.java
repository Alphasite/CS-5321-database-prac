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

    /**
     * Map table identifiers to useful info for optimization
     */
    private Map<String, Relation> relationsToJoin;

    private UnionFind constraints;
    private JoinPlan bestPlan;

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
        computeBestPlan(new ArrayList<>(relationsToJoin.keySet()), null);

        List<String> joins = bestPlan.getJoins();

        return null;
    }

    private void computeBestPlan(List<String> toJoin, JoinPlan currentPlan) {
        if (toJoin.size() == 0) {
            if (currentPlan.cost < bestPlan.cost) {
                bestPlan = currentPlan;
            }
        } else {
            // Continue iterating
            for (String relation : toJoin) {
                JoinPlan plan;
                if (currentPlan == null) {
                    plan = new JoinPlan(this.relationsToJoin.get(relation));
                } else {
                    plan = new JoinPlan(currentPlan, this.relationsToJoin.get(relation));
                }

                // Apply modifications
                toJoin.remove(relation);

                computeBestPlan(toJoin, plan);

                // Revert modifications
                toJoin.add(relation);
            }
        }
    }

}
