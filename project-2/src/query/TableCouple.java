package query;

import net.sf.jsqlparser.schema.Table;

public class TableCouple {
    private Table table1;
    private Table table2;

    public TableCouple(Table table1, Table table2){
        this.table1=table1;
        this.table2=table2;
    }

    public Table getTable1() {
        return table1;
    }

    public Table getTable2() {
        return table2;
    }
}
