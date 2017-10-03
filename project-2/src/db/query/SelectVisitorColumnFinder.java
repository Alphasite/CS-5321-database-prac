package db.query;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

// TODO: even less sure what this does...

public class SelectVisitorColumnFinder implements SelectVisitor {

    @Override
    public void visit(PlainSelect plainSelect) {

    }

    @Override
    public void visit(Union union) {
        throw new NotImplementedException();
    }
}
