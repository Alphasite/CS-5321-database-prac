package db.query.visitors;

import db.Utilities.Pair;
import db.Utilities.UnionFind;
import db.Utilities.Utilities;
import db.operators.logical.*;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Print a logical tree to a string.
 */
public class LogicalTreePrinter implements LogicalTreeVisitor {
    private int depth;
    private List<String> lines;

    private LogicalTreePrinter() {
        this.depth = 0;
        this.lines = new ArrayList<>();
    }

    /**
     * Print the node to std out
     *
     * @param node the node to dump.
     */
    public static void printTree(LogicalOperator node) {
        LogicalTreePrinter printer = new LogicalTreePrinter();
        node.accept(printer);
        System.out.println(String.join("\n", printer.lines));
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalJoinOperator node) {
        StringBuilder line = new StringBuilder();

        Optional<Expression> expression = node.getUnusedExpressions().stream()
                .map(Pair::getRight)
                .reduce(Utilities::joinExpression);


        line.append("Join");

        if (expression.isPresent()) {
            line.append("[");
            line.append(expression.get());
            line.append("]");
        }

        this.lines.add(pad(line.toString()));

        UnionFind unionFind = node.getUnionFind();
        for (Set<String> set : unionFind.getSets()) {
            if (set.size() > 0) {
                line = new StringBuilder();
                line.append("[");

                List<String> names = new ArrayList<>(set);
                line.append("[").append(String.join(", ", names)).append("], ");

                String column = names.get(0);
                line.append("equals ").append(unionFind.getEquals(column)).append(", ");
                line.append("min ").append(unionFind.getMinimum(column)).append(", ");
                line.append("max ").append(unionFind.getMaximum(column));

                line.append("]");

                this.lines.add(line.toString());
            }
        }


        this.depth += 1;
        for (LogicalOperator operator : node.getChildren()) {
            operator.accept(this);
        }
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalScanOperator node) {
        lines.add(pad("Leaf[" + node.getTable().file.getFileName() + "]"));
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalSelectOperator node) {
        lines.add(pad("Select[" + node.getPredicate() + "]"));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalSortOperator node) {
        List<String> columns = node.getSortHeader().getQualifiedAttributeNames();

        lines.add(pad("Sort[" + String.join(", ", columns) + "]"));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalProjectOperator node) {
        List<String> columns = node.getHeader().getQualifiedAttributeNames();

        lines.add(pad("Project[" + String.join(", ", columns) + "]"));


        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalDistinctOperator node) {
        lines.add(pad("DupElim"));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * Pad the string with the corrent number of spaces.
     * @param lineBody the line to pad
     * @return the padded line
     */
    private String pad(String lineBody) {
        StringBuilder line = new StringBuilder();

        for (int i = 0; i < this.depth; i++) {
            line.append("-");
        }

        line.append(lineBody);

        return line.toString();
    }
}
