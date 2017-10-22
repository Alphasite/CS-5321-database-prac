package db.operators.physical.extended;

import db.datastore.TableHeader;
import db.operators.UnaryNode;
import db.operators.physical.Operator;

public interface SortOperator extends Operator, UnaryNode<Operator> {
    TableHeader getSortHeader();
}
