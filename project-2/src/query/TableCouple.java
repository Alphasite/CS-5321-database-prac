package query;

import db.Utilities;
import net.sf.jsqlparser.schema.Table;

public class TableCouple {
    private Table table1;
    private Table table2;

    public TableCouple(Table table1, Table table2){
        String id1 = Utilities.getIdentifier(table1);
        String id2 = Utilities.getIdentifier(table2);

        // This ensures that this always has tables in the same order.
        // This allows equals and hash to work correctly.
        if (id1.compareTo(id2) > 0) {
            this.table1 = table1;
            this.table2 = table2;
        } else {
            this.table1 = table2;
            this.table2 = table1;
        }
    }

    public Table getTable1() {
        return table1;
    }

    public Table getTable2() {
        return table2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableCouple)) return false;

        TableCouple that = (TableCouple) o;

        String table1Id1 = Utilities.getIdentifier(table1);
        String table1Id2 = Utilities.getIdentifier(table2);

        String table2Id1 = Utilities.getIdentifier(that.table1);
        String table2Id2 = Utilities.getIdentifier(that.table2);

        if (!table1Id1.equals(table2Id1)) return false;
        return table1Id2.equals(table2Id2);
    }

    @Override
    public int hashCode() {
        String id1 = Utilities.getIdentifier(table1);
        String id2 = Utilities.getIdentifier(table2);

        int result = id1.hashCode();
        result = 31 * result + id2.hashCode();
        return result;
    }
}
