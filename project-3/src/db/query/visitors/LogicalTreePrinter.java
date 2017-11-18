package db.query.visitors;

import db.operators.logical.*;

import java.util.ArrayList;
import java.util.List;

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

        line.append("Join");

        this.lines.add(pad(line.toString()));

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
        lines.add(pad("Scan " + node.getTable().file.getFileName()));
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalSelectOperator node) {
        lines.add(pad("Select on " + node.getPredicate()));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalSortOperator node) {
        lines.add(pad("Sort on " + node.getSortHeader()));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalProjectOperator node) {
        lines.add(pad("Project to " + node.getHeader()));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalDistinctOperator node) {
        lines.add(pad("Distinct"));

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
            line.append("  ");
        }

        line.append(lineBody);

        return line.toString();
    }
}
