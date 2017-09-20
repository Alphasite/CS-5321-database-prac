package query;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SelectVisitorColumnFinder implements SelectVisitor {

    @Override
    public void visit(PlainSelect plainSelect) {

    }

    @Override
    public void visit(SetOperationList setOpList) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(WithItem withItem) {
        throw new NotImplementedException();
    }

}
