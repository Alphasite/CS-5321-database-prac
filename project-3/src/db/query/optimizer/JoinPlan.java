package db.query.optimizer;

import db.PhysicalPlanConfig;
import db.PhysicalPlanConfig.JoinImplementation;
import db.Utilities.Pair;
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

    /**
     * Estimate the join sizes for each equality set from the union find.
     *
     * @param sets the equality sets from the union find.
     * @return the total size of the joins.
     */
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

        return Math.max(1, (int) total);
    }

    /**
     * @return the estimated cost of the plan.
     */
    public int getCost() {
        return cost;
    }

    /**
     * @return an estimate of the number of tuples produced after performing all joins.
     */
    public int getEstimatedTupleCount() {
        return estimatedTupleCount;
    }

    /**
     * @return the size of a tuple after the joins have been performed.
     */
    public int getTupleSize() {
        return this.tupleSize;
    }

    /**
     * @return a list of all relations in a table.
     */
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

    /**
     * @return a list indicating the order in relations should be joined.
     */
    public List<LogicalOperator> getJoinOrder() {
        return this.getRelations().stream()
                .map(r -> r.op)
                .collect(Collectors.toList());
    }

    /**
     * A list indicating the type of join to do for each join.
     *
     * @param config the plan config.
     * @return a list of join types, 1 for each join.
     */
    public List<JoinImplementation> getJoinTypes(PhysicalPlanConfig config) {
        return this.getJoinTypesAndFlips(config).getLeft();
    }

    /**
     * A list indicating the optimal arrangement of relations for the joins.
     *
     * @param config the plan config.
     * @return a list of booleans, 1 for each join, indicating whether or not to flip inner and outer.
     */
    public List<Boolean> getFlipInnerOuter(PhysicalPlanConfig config) {
        return this.getJoinTypesAndFlips(config).getRight();
    }

    /**
     * Compute which join is optimal and whether or not the inner and outer relations should be flipped on a join.
     *
     * @param config the plan config.
     * @return both the list of joins and the list of whether or not to flip.
     */
    private Pair<List<JoinImplementation>, List<Boolean>> getJoinTypesAndFlips(PhysicalPlanConfig config) {
        List<JoinImplementation> joinImplementations = new ArrayList<>();
        List<Boolean> flipInnerOuterRelations = new ArrayList<>();

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

                if (smjCost > bnljCost || config.sortParameter < 3) {
                    System.out.println("winner:      BNLJ");
                    joinImplementations.add(JoinImplementation.BNLJ);
                } else {
                    System.out.println("winner:      SMJ");
                    joinImplementations.add(JoinImplementation.SMJ);
                }

                if (outerPages > innerPages) {
                    flipInnerOuterRelations.add(true);
                } else {
                    flipInnerOuterRelations.add(false);
                }

                System.out.println("###################################");
            }

            return null;
        };

        this.recursePlan(recursiveFunction);

        return new Pair<>(joinImplementations, flipInnerOuterRelations);
    }

    /**
     * Estimate the cost of performing a BNLJ on the relations.
     *
     * @param blockCount the number of buffer pages available to the join operation.
     * @param outerPages the size of the outer relation in pages.
     * @param innerPages the size of the inner relation in pages.
     * @return the estimated cost of the operation.
     */
    private static int computeBNLJCost(int blockCount, int outerPages, int innerPages) {
        int blockSize = blockCount - 2;
        int outerBlocks = (int) ceil(1f * outerPages / blockSize);
        return outerPages + outerBlocks * innerPages;
    }

    /**
     * Estimate the cost of performing a SMJ on the relations.
     *
     * @param sortBlocks the number of buffer pages available to the sort operation.
     * @param outerPages the size of the outer relation in pages.
     * @param innerPages the size of the inner relation in pages.
     * @return the estimated cost of the operation.
     */
    private static int computeSMJCost(int sortBlocks, int outerPages, int innerPages) {
        int innerCost = computeSortCost(sortBlocks, innerPages) + innerPages;
        int outerCost = computeSortCost(sortBlocks, outerPages) + outerPages;
        return innerCost + outerCost;
    }

    /**
     * Compute the cost of performing an external sort on the relation.
     *
     * @param blocks        the number of pages in a block
     * @param numberOfPages the number of page to sort
     * @return the estimated cost of the operation in page IOs
     */
    private static int computeSortCost(int blocks, int numberOfPages) {
        int numberOfPasses = (int) ceil(log(1f * numberOfPages / blocks) / log(blocks - 1)) + 1;
        int passCost = 2 * numberOfPages;

        return passCost * numberOfPasses;
    }

    /**
     * A helper function to recursivly traverse the tree to apply a function to each node, from first join to last.
     *
     * @param recursiveFunction the function called on each node.
     */
    private void recursePlan(BiFunction<JoinPlan, Relation, Void> recursiveFunction) {
        if (this.parentJoin != null) {
            this.parentJoin.recursePlan(recursiveFunction);
        }

        recursiveFunction.apply(this.parentJoin, this.joinTable);
    }
}
