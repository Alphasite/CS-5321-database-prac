package db.query.optimizer;

import db.Utilities.UnionFind;
import db.operators.UnaryNode;
import db.operators.logical.LogicalJoinOperator;
import db.operators.logical.LogicalOperator;
import db.operators.logical.LogicalScanOperator;
import db.operators.physical.Operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinOrderOptimizer {

    private List<LogicalOperator> relations;
    private UnionFind constraints;

    private List<VValues> vvalues;

    private int bestCost;
    private List<Operator> bestPlan;

    /**
     * Map table identifiers to physical operator that pulls tuples from it and may apply selections
     */
    private Map<String, Operator> sourceRelations;

    public JoinOrderOptimizer(LogicalJoinOperator join) {
        // Make a copy in case we have to mutate list
        this.relations = new ArrayList<>(join.getChildren());

        this.constraints = join.getUnionFind();

        this.vvalues = new ArrayList<>();

        this.sourceRelations = new HashMap<>();
    }

    public Operator buildOptimizedPlan(List<Operator> sources) {
        computeBestPlan(new ArrayList<>(sources), new ArrayList<>());

        // Generate name -> op mappings
        for (Operator op : sources) {
            String tableIdentifier = op.getHeader().tableIdentifiers.get(0);
            this.sourceRelations.put(tableIdentifier, op);
        }

        this.computeSourceVValues();

        return null;
    }

    private void computeBestPlan(List<Operator> joined, List<Operator> toJoin) {
        if (toJoin.size() == 0) {
            int planCost = computeCost(joined);

            if (planCost < bestCost) {
                bestCost = planCost;
                bestPlan = new ArrayList<>(joined); // Create a copy to prevent side-effects
            }
        } else {
            // Continue iterating
            for (Operator op : toJoin) {
                // Apply modifications
                joined.add(op);
                toJoin.remove(op);

                computeBestPlan(joined, toJoin);

                // Revert modifications
                toJoin.add(op);
                joined.remove(op);
            }
        }
    }

    private void computeSourceVValues() {
        for (LogicalOperator source : relations) {
            while (!(source instanceof LogicalScanOperator)) {
                assert source instanceof UnaryNode;

                source = ((UnaryNode<LogicalOperator>) source).getChild();
            }

            VValues vval = new VValues((LogicalScanOperator) source);
            vval.updateConstraints(constraints);
            vvalues.add(vval);
        }
    }

    private int computeCost(List<Operator> joinPlan) {
        if (joinPlan.size() <= 2) {
            return 0;
        }

        int cost = 0;
        int tupleCount = 0;

    }
}
