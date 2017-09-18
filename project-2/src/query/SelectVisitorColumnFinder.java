package query;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

public class SelectVisitorColumnFinder implements SelectVisitor {

    @Override
    public void visit(PlainSelect plainSelect) {

    }

    @Override
    public void visit(Union union) {

    }
}
