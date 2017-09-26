package query;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

// TODO: What is this ?

public class FromVisitor implements FromItemVisitor {

    @Override
    public void visit(Table table) {

    }

    @Override
    public void visit(SubSelect subSelect) {

    }

    @Override
    public void visit(SubJoin subJoin) {

    }
}
