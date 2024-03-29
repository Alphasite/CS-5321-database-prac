package db.Utilities;

import java.util.*;

/**
 * Implementation of a union-find data structure, ie. a partition of a set of elements into disjoint subsets.
 * Provides efficient methods for merging and checking if elements are in the same subset.
 * <p>
 * Used as part of plan optimization to implement pushing selections.
 * </p>
 */
public class UnionFind {
    private Map<String, String> parent;
    private Map<String, Integer> minimum;
    private Map<String, Integer> maximum;

    public UnionFind() {
        this.parent = new HashMap<>();
        this.minimum = new HashMap<>();
        this.maximum = new HashMap<>();
    }

    /**
     * Get the parent for a column
     *
     * @param table the column to search
     * @return the parent of the column
     */
    private String getParent(String table) {
        while (!Objects.equals(table, this.parent.get(table))) {
            table = this.parent.get(table);
        }

        return table;
    }

    /**
     * Add the column to the union find.
     *
     * @param table the column to add
     */
    public void add(String table) {
        if (!parent.containsKey(table)) {
            this.parent.put(table, table);
        }
    }

    /**
     * Try set the minimum value for a column/set
     *
     * @param table   the column to set
     * @param minimum the new candidate minimum value
     */
    public void setMinimum(String table, int minimum) {
        table = this.getParent(table);
        if (this.minimum.containsKey(table)) {
            this.minimum.put(table, Math.max(this.minimum.get(table), minimum));
        } else {
            this.minimum.put(table, minimum);
        }
    }

    /**
     * Try set the maximum value for a column/set
     *
     * @param table the column to set
     * @param maximum the new candidate maximum value
     */
    public void setMaximum(String table, int maximum) {
        table = this.getParent(table);
        if (this.maximum.containsKey(table)) {
            this.maximum.put(table, Math.min(this.maximum.get(table), maximum));
        } else {
            this.maximum.put(table, maximum);
        }
    }

    /**
     * Try set the equal value for a column/set
     *
     * @param table the column to set
     * @param value the new candidate equal value
     */
    public void setEquals(String table, int value) {
        table = this.getParent(table);
        this.setMinimum(table, value);
        this.setMaximum(table, value);
    }

    public Integer getMinimum(String table) {
        return this.minimum.getOrDefault(this.getParent(table), null);
    }

    public Integer getMaximum(String table) {
        return this.maximum.getOrDefault(this.getParent(table), null);
    }

    public Integer getEquals(String table) {
        table = this.getParent(table);
        if (this.getMaximum(table) == this.getMinimum(table)) {
            return this.getMaximum(table);
        } else {
            return null;
        }
    }

    /**
     * Union 2 equal columns, joining min/max/equals
     * @param table1 the first column
     * @param table2 the second column
     */
    public void union(String table1, String table2) {
        this.add(table1);
        this.add(table2);

        table2 = getParent(table2);

        this.parent.put(table2, table1);

        if (this.minimum.containsKey(table2)) {
            this.setMinimum(table1, this.minimum.get(table2));
        }

        if (this.maximum.containsKey(table2)) {
            this.setMaximum(table1, this.maximum.get(table2));
        }
    }

    /**
     * @return the equality set of equal columns
     */
    public List<Set<String>> getSets() {
        Map<String, Set<String>> sets = new HashMap<>();

        for (String table : parent.keySet()) {
            String parent = this.getParent(table);

            Set<String> set = sets.getOrDefault(parent, new HashSet<>());
            set.add(table);

            sets.put(parent, set);
        }

        return new ArrayList<>(sets.values());
    }
}
