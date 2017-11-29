package db.query.optimizer;

import db.PhysicalPlanConfig;
import db.PhysicalPlanConfig.JoinImplementation;
import db.datastore.Database;
import db.operators.logical.LogicalOperator;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.lang.Math.ceil;
import static java.lang.Math.log;

/**
 * Abstract representation of a left-deep join tree, with heuristics to estimate its cost.
 */
public class JoinPlan {

    private int tupleSize;
    private int estimatedTupleCount;
    private int cost;
    private int tableCount;

    private JoinPlan parentJoin;
    private Relation joinTable;

    /**
     * Internal vvalue map to simplify attribute lookup
     */
    private Map<String, Integer> vvalues;

    /**
     * Create a join plan on a single table.
     */
    public JoinPlan(Relation baseRelation) {
        this.parentJoin = null;
        this.joinTable = baseRelation;

        this.cost = 0;
        this.tableCount = 1;
        this.estimatedTupleCount = baseRelation.tupleCount;
        this.tupleSize = baseRelation.tupleSize;

        this.vvalues = new HashMap<>(baseRelation.vvalues.getVvalues());
    }

    /**
     * Create a join plan that uses parent as a left child and the given relation as a right child.
     *
     * @param parentJoin Parent left-deep join tree.
     * @param joinWith Relation to join parent with.
     * @param attributeEqualitySets List of sets of attribute names describing attributes which have to be joined.
     */
    public JoinPlan(JoinPlan parentJoin, Relation joinWith, List<Set<String>> attributeEqualitySets) {
        this.parentJoin = parentJoin;
        this.joinTable = joinWith;

        this.vvalues = new HashMap<>(parentJoin.vvalues);
        this.vvalues.putAll(joinWith.vvalues.getVvalues());

        this.tableCount = parentJoin.tableCount + 1;

        this.estimatedTupleCount = estimateJoinSize(attributeEqualitySets);
        this.tupleSize = parentJoin.tupleSize + joinWith.tupleSize;

        if (parentJoin.tableCount < 2) {
            this.cost = 0;
        } else {
            this.cost = parentJoin.cost + parentJoin.estimatedTupleCount;
        }
    }

    private int estimateJoinSize(List<Set<String>> sets) {
        double total = this.parentJoin.estimatedTupleCount * this.joinTable.tupleCount;

        List<String> relationAttributes = this.joinTable.op.getHeader().getQualifiedAttributeNames();

        Set<String> attributesAvailableForJoin = this.parentJoin.vvalues.keySet();

        for (Set<String> set : sets) {
            // Find join conditions in set involving attributes of this relation
            Set<String> intersect = new HashSet<>(set);
            intersect.retainAll(relationAttributes);

            if (!intersect.isEmpty()) {
                // Pick any attribute of this relation and try to join it with another in parent join tree
                String attribute = intersect.iterator().next();

                Set<String> available = new HashSet<>(set);
                available.retainAll(attributesAvailableForJoin);

                if (!available.isEmpty()) {
                    String joinOn = available.iterator().next();

                    // Read VValues and update tuple count estimation accordingly
                    int vvalA = this.vvalues.get(attribute);
                    int vvalB = this.vvalues.get(joinOn);

                    total /= Math.max(vvalA, vvalB);
                }
            }
        }

        return Math.min(1, (int) total);
    }

    public int getCost() {
        return cost;
    }

    public int getEstimatedTupleCount() {
        return estimatedTupleCount;
    }

    public int getTupleSize() {
        return this.tupleSize;
    }

    public List<Relation> getRelations() {
        List<Relation> relations = new ArrayList<>();
        JoinPlan join = this;

        while (join != null) {
            relations.add(join.joinTable);
            join = join.parentJoin;
        }

        Collections.reverse(relations);
        return relations;
    }

    public List<LogicalOperator> getJoinOrder() {
        return this.getRelations().stream()
                .map(r -> r.op)
                .collect(Collectors.toList());
    }

    public List<JoinImplementation> getJoinTypes(PhysicalPlanConfig config) {
        List<JoinImplementation> joinImplementations = new ArrayList<>();

        BiFunction<JoinPlan, Relation, Void> recursiveFunction = (parentJoin, table) -> {
            if (parentJoin != null) {
                int outerTupleSize = parentJoin.getTupleSize();
                int outerTupleCount = parentJoin.getEstimatedTupleCount();
                int outerTuplesPerPage = Database.PAGE_SIZE / 4 / outerTupleSize;
                int outerPages = (int) ceil(1f * outerTupleCount / outerTuplesPerPage);

                int innerTupleSize = table.tupleSize;
                int innerTupleCount = table.tupleCount;
                int innerTuplesPerPage = Database.PAGE_SIZE / 4 / innerTupleSize;
                int innerPages = (int) ceil(1f * innerTupleCount / innerTuplesPerPage);

                int bnljCost = computeBNLJCost(config.joinParameter, outerPages, innerPages);
                int smjCost = computeSMJCost(config.sortParameter, outerPages, innerPages);

                System.out.println("###################################");
                System.out.println("smj Cost:    " + smjCost);
                System.out.println("bnlj Cost:   " + bnljCost);
                System.out.println("sort buffer: " + config.sortParameter);

                if (smjCost > bnljCost && config.sortParameter >= 3) {
                    System.out.println("winner:      BNLJ");
                    joinImplementations.add(JoinImplementation.BNLJ);
                } else {
                    System.out.println("winner:      SMJ");
                    joinImplementations.add(JoinImplementation.SMJ);
                }

                System.out.println("###################################");
            }

            return null;
        };

        this.recursePlan(recursiveFunction);

        return joinImplementations;
    }

    private static int computeBNLJCost(int blockCount, int outerPages, int innerPages) {
        int blockSize = blockCount - 2;
        int outerBlocks = (int) ceil(1f * outerPages / blockSize);
        return outerPages + outerBlocks * innerPages;
    }

    private static int computeSMJCost(int sortBlocks, int outerPages, int innerPages) {
        int innerCost = computeSortCost(sortBlocks, innerPages) + innerPages;
        int outerCost = computeSortCost(sortBlocks, outerPages) + outerPages;
        return innerCost + outerCost;
    }

    private static int computeSortCost(int blocks, int numberOfPages) {
        int numberOfPasses = (int) ceil(log(1f * numberOfPages / blocks) / log(blocks - 1)) + 1;
        int passCost = 2 * numberOfPages;

        return passCost * numberOfPasses;
    }

    private void recursePlan(BiFunction<JoinPlan, Relation, Void> recursiveFunction) {
        if (this.parentJoin != null) {
            this.parentJoin.recursePlan(recursiveFunction);
        }

        recursiveFunction.apply(this.parentJoin, this.joinTable);
    }
}
