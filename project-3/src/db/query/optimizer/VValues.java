package db.query.optimizer;

import db.Utilities.UnionFind;
import db.datastore.TableHeader;
import db.datastore.stats.TableStats;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to compute and store the attribute V-Values for different kinds of relations.
 * A V-Value V(A, R) is the number of distinct values that attribute A takes in relation R.
 */
public class VValues {

    /**
     * Map a qualified attribute name ('tableAlias'.'columnName') to its V-Value in this relation.
     */
    private Map<String, Integer> vvalues;

    private Map<String, Integer> lowerBounds;
    private Map<String, Integer> upperBounds;

    public VValues() {
        this.vvalues = new HashMap<>();
        this.lowerBounds = new HashMap<>();
        this.upperBounds = new HashMap<>();
    }

    /**
     * Create V-Values for table
     */
    public VValues(TableHeader header, TableStats stats) {
        this();

        int i = 0;
        for (String attribute : header.getQualifiedAttributeNames()) {
            // Estimate with either range of values or tuple count
            int vval = Math.min(stats.maximums[i] - stats.minimums[i] + 1, stats.count);
            vvalues.put(attribute, vval);

            lowerBounds.put(attribute, stats.minimums[i]);
            upperBounds.put(attribute, stats.maximums[i]);
            i++;
        }
    }

    /**
     * Refresh VV values using the current constraints.
     */
    private void recomputeVValues() {
        for (String attribute : this.vvalues.keySet()) {
            int min = lowerBounds.get(attribute);
            int max = upperBounds.get(attribute);

            // VValues can only decrease when performing selections
            int vval = Math.max(max - min + 1, 1);
            vvalues.put(attribute, Math.min(vval, vvalues.get(attribute)));
        }
    }

    /**
     * Adjust V-Values based on attribute range constraints.
     *
     * @param constraints {@link UnionFind} structure containing the bounds for every attribute part of a selection
     */
    public void updateConstraints(UnionFind constraints) {
        for (String attribute : this.vvalues.keySet()) {
            Integer min = constraints.getMinimum(attribute);
            if (min != null) {
                lowerBounds.put(attribute, Math.max(lowerBounds.get(attribute), min));
            }

            Integer max = constraints.getMaximum(attribute);
            if (max != null) {
                upperBounds.put(attribute, Math.min(upperBounds.get(attribute), max));
            }
        }

        this.recomputeVValues();
    }

    public Map<String, Integer> getVvalues() {
        return vvalues;
    }

    public int getAttributeVValue(String attributeName) {
        return vvalues.get(attributeName);
    }
}
