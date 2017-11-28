package db.query.optimizer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Abstract representation of a left-deep join tree, with heuristics to estimate its cost.
 */
class JoinPlan {

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

    public List<String> getJoins() {
        return getRelations().stream()
                .map(r -> r.name)
                .collect(Collectors.toList());
    }
}
