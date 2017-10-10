package db.query;

/**
 * A pair class to encapsulate a pair of tables.
 * <p>
 * Implements {@link #equals(Object)} and {@link #hashCode()}
 */
public class TableCouple {
    private String table1;
    private String table2;

    /**
     * This pair class will sort Table1 and table 2 class, by lexical order,
     * to ensure that the tables are always in the same order, regardless of the order in the code.
     *
     * @param table1 The first table
     * @param table2 The second table
     */
    public TableCouple(String table1, String table2) {
        this.table1 = table1;
        this.table2 = table2;

        // This ensures that this always has tables in the same order.
        // This allows equals and hash to work correctly.
        if (table1.compareTo(table2) > 0) {
            this.table1 = table1;
            this.table2 = table2;
        } else {
            this.table1 = table2;
            this.table2 = table1;
        }
    }

    public String getTable1() {
        return table1;
    }

    public String getTable2() {
        return table2;
    }

    /**
     * Compare based on the names of the tables.
     *
     * @param o the object which is being compared too.
     * @return whether or not they are equal.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableCouple)) return false;

        TableCouple that = (TableCouple) o;

        return table1.equals(that.table1) && table2.equals(that.table2);
    }

    /**
     * A hash based on the names of the table.
     *
     * @return The has value.
     */
    @Override
    public int hashCode() {
        int result = table1.hashCode();
        result = 31 * result + table2.hashCode();
        return result;
    }
}
