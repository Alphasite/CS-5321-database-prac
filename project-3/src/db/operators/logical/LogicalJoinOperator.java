package db.operators.logical;

import db.Utilities.Pair;
import db.Utilities.UnionFind;
import db.datastore.TableHeader;
import db.operators.NaryNode;
import db.query.TablePair;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Logical operator for join handling : keeps track of left and right tuple sources and optional join condition.
 */
public class LogicalJoinOperator implements LogicalOperator, NaryNode<LogicalScanOperator> {
    private final List<LogicalScanOperator> children;
    private UnionFind unionFind;
    private List<Pair<TablePair, Expression>> unusedExpressions;
    private TableHeader outputSchema;

    public LogicalJoinOperator(List<LogicalScanOperator> children, UnionFind unionFind, List<Pair<TablePair, Expression>> unusedExpressions) {
        this.children = children;
        this.unionFind = unionFind;
        this.unusedExpressions = unusedExpressions;

        if (this.children.size() == 0) {
            this.outputSchema = new TableHeader();
        } else {
            this.outputSchema = this.children.get(0).getHeader();

            for (int i = 1; i < this.children.size(); i++) {
                this.outputSchema = computeHeader(this.outputSchema, this.children.get(i).getHeader());
            }
        }
    }

    /**
     * Compute the result header for this join.
     *
     * @param left  the left node.
     * @param right the right node.
     * @return The resultant header.
     */
    public static TableHeader computeHeader(TableHeader left, TableHeader right) {
        int tableWidth = left.size() + right.size();

        List<String> headings = new ArrayList<>(tableWidth);
        List<String> aliases = new ArrayList<>(tableWidth);

        headings.addAll(left.columnHeaders);
        headings.addAll(right.columnHeaders);

        aliases.addAll(left.columnAliases);
        aliases.addAll(right.columnAliases);

        return new TableHeader(aliases, headings);
    }

    /**
     * @return the union find with all of the expressions
     */
    public UnionFind getUnionFind() {
        return unionFind;
    }

    /**
     * @return the list of unused expressions
     */
    public List<Pair<TablePair, Expression>> getUnusedExpressions() {
        return unusedExpressions;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return outputSchema;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public List<LogicalScanOperator> getChildren() {
        return this.children;
    }
}
