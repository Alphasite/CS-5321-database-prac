package db.Utilities;

import java.util.*;

public class UnionFind {
    private Map<String, String> parent;
    private Map<String, Integer> minimum;
    private Map<String, Integer> maximum;

    public UnionFind() {
        this.parent = new HashMap<>();
        this.minimum = new HashMap<>();
        this.maximum = new HashMap<>();
    }

    private String getParent(String table) {
        String parent = table;

        while (!Objects.equals(parent, this.parent.get(parent))) {
            parent = this.parent.get(parent);
        }

        return parent;
    }

    public void add(String table) {
        if (!parent.containsKey(table)) {
            this.parent.put(table, table);
            this.maximum.put(table, Integer.MIN_VALUE);
            this.minimum.put(table, Integer.MAX_VALUE);
        }
    }

    public void setMinimum(String table, int minimum) {
        table = this.getParent(table);
        this.minimum.put(table, Math.min(this.minimum.getOrDefault(table, Integer.MAX_VALUE), minimum));
    }

    public void setMaximum(String table, int maximum) {
        table = this.getParent(table);
        this.maximum.put(table, Math.max(this.maximum.getOrDefault(table, Integer.MIN_VALUE), maximum));
    }

    public void setEquals(String table, int value) {
        table = this.getParent(table);
        this.setMinimum(table, value);
        this.setMaximum(table, value);
    }

    public int getMinimum(String table) {
        return this.minimum.get(this.getParent(table));
    }

    public int getMaximum(String table) {
        return this.maximum.get(this.getParent(table));
    }

    public Integer getEquals(String table) {
        table = this.getParent(table);
        if (this.getMaximum(table) == this.getMinimum(table)) {
            return this.getMaximum(table);
        } else {
            return null;
        }
    }

    public void union(String table1, String table2) {
        this.add(table1);
        this.add(table2);

        table2 = getParent(table2);

        this.parent.put(table2, table1);
        this.setMinimum(table1, this.minimum.get(table2));
        this.setMaximum(table1, this.maximum.get(table2));
    }

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
