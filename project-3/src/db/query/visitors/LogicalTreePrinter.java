package db.query.visitors;

import db.operators.logical.*;

import java.util.ArrayList;
import java.util.List;

public class LogicalTreePrinter implements LogicalTreeVisitor {
    private int depth;
    private List<String> lines;

    private LogicalTreePrinter() {
        this.depth = 0;
        this.lines = new ArrayList<>();
    }

    public static void printTree(LogicalOperator node) {
        LogicalTreePrinter printer = new LogicalTreePrinter();
        node.accept(printer);
        System.out.println(String.join("\n", printer.lines));
    }

    @Override
    public void visit(LogicalJoinOperator node) {
        StringBuilder line = new StringBuilder();

        line.append("Join");

        if (node.getJoinCondition() != null) {
            line.append(" on ");
            line.append(node.getJoinCondition());
        }

        this.lines.add(pad(line.toString()));

        this.depth += 1;
        node.getRight().accept(this);
        node.getLeft().accept(this);
        this.depth -= 1;
    }

    @Override
    public void visit(LogicalRenameOperator node) {
        lines.add(pad("Reaname " + node.getNewTableName()));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    @Override
    public void visit(LogicalScanOperator node) {
        lines.add(pad("Scan " + node.getTable().file.getFileName()));
    }

    @Override
    public void visit(LogicalSelectOperator node) {
        lines.add(pad("Select on " + node.getPredicate()));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    @Override
    public void visit(LogicalSortOperator node) {
        lines.add(pad("Sort on " + node.getSortHeader()));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    @Override
    public void visit(LogicalProjectOperator node) {
        lines.add(pad("Project to " + node.getHeader()));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    @Override
    public void visit(LogicalDistinctOperator node) {
        lines.add(pad("Distinct"));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    private String pad(String lineBody) {
        StringBuilder line = new StringBuilder();

        for (int i = 0; i < this.depth; i++) {
            line.append("  ");
        }

        line.append(lineBody);

        return line.toString();
    }
}
