package query;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(ValuesList valuesList) {
        throw new NotImplementedException();
    }

}
