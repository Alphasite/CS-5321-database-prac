package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.physical.Operator;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class LogicalJoinOperator implements LogicalOperator {
    private final LogicalOperator left;
    private final LogicalOperator right;
    private final Expression joinCondition;

    public LogicalJoinOperator(LogicalOperator left, LogicalOperator right, Expression joinCondition) {
        this.left = left;
        this.right = right;
        this.joinCondition = joinCondition;
    }

    public static TableHeader computeHeader(Operator left, Operator right) {
        int tableWidth = left.getHeader().size() + right.getHeader().size();

        List<String> headings = new ArrayList<>(tableWidth);
        List<String> aliases = new ArrayList<>(tableWidth);

        headings.addAll(left.getHeader().columnHeaders);
        headings.addAll(right.getHeader().columnHeaders);

        aliases.addAll(left.getHeader().columnAliases);
        aliases.addAll(right.getHeader().columnAliases);

        return new TableHeader(aliases, headings);
    }
}
