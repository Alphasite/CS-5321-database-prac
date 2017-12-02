package db.query.visitors;

import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.operators.physical.bag.JoinOperator;
import db.operators.physical.bag.ProjectionOperator;
import db.operators.physical.bag.SelectionOperator;
import db.operators.physical.extended.DistinctOperator;
import db.operators.physical.extended.SortOperator;
import db.operators.physical.physical.IndexScanOperator;
import db.operators.physical.physical.ScanOperator;
import db.operators.physical.utility.BlockCacheOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Print the physical tree structure.
 *
 * @inheritDoc
 */
public class PhysicalTreePrinter implements PhysicalTreeVisitor {
    private int depth;
    private List<String> lines;

    private PhysicalTreePrinter() {
        this.depth = 0;
        this.lines = new ArrayList<>();
    }

    /**
     * Print the tree to stdout.
     *
     * @param node the node to visit.
     */
    public static void printTree(Operator node) {
        System.out.println(getTree(node));
    }

    /**
     * @param node the root node to print
     * @return the tree as a string.
     */
    public static String getTree(Operator node) {
        PhysicalTreePrinter printer = new PhysicalTreePrinter();
        node.accept(printer);
        return String.join("\n", printer.lines);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(JoinOperator node) {
        StringBuilder line = new StringBuilder();

        line.append(node.getJoinType());
        line.append("[");
        if (node.getPredicate() != null) {
            line.append(node.getPredicate());
        }
        line.append("]");

        this.lines.add(pad(line.toString()));

        this.depth += 1;
        node.getRight().accept(this);
        node.getLeft().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(SortOperator node) {
        List<String> columns = node.getSortHeader().getQualifiedAttributeNames();

        lines.add(pad(node.getSortType() + "[" + String.join(", ", columns) + "]"));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(ScanOperator node) {
        lines.add(pad("TableScan[" + node.getTable().file.getFileName() + "]"));
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(SelectionOperator node) {
        lines.add(pad("Select[" + node.getPredicate() + "]"));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(BlockCacheOperator node) {
        node.getChild().accept(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(IndexScanOperator node) {
        lines.add(pad(
                "IndexScan[" +
                        node.getIndex().tableName + "," +
                        node.getIndex().attributeName + "," +
                        node.getLowVal() + "," +
                        node.getHighVal() +
                        "]"
        ));
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(ProjectionOperator node) {
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
    public void visit(DistinctOperator node) {
        lines.add(pad("DupElim"));

        this.depth += 1;
        node.getChild().accept(this);
        this.depth -= 1;
    }

    /**
     * Pad the line to the correct depth.
     *
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
