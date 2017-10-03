package db.query;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

// TODO: What is this ?

public class FromVisitor implements FromItemVisitor {

    @Override
    public void visit(Table table) {

    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(SubJoin subJoin) {
        throw new NotImplementedException();
    }

}
