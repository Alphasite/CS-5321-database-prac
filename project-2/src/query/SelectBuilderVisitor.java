package query;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

// TODO: What is this ? Builder for the projection operator ?

public class SelectBuilderVisitor implements SelectVisitor {
    @Override
    public void visit(PlainSelect plainSelect) {

    }

    @Override
    public void visit(Union union) {
        throw new NotImplementedException();
    }

}
