package db.query.visitors;

import db.PhysicalPlanConfig;
import db.Project3;
import db.datastore.TableHeader;
import db.operators.logical.*;
import db.operators.physical.Operator;
import db.operators.physical.bag.*;
import db.operators.physical.extended.DistinctOperator;
import db.operators.physical.extended.ExternalSortOperator;
import db.operators.physical.extended.InMemorySortOperator;
import db.operators.physical.extended.SortOperator;
import db.operators.physical.physical.ScanOperator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;

import static db.PhysicalPlanConfig.SortImplementation.IN_MEMORY;

/**
 * Physical query plan builder, implemented as a Logical operator tree visitor
 *
 * For now convert each logical op to the corresponding physical one, later will implement proper query optimization
 */
public class PhysicalPlanBuilder implements LogicalTreeVisitor {

    /**
     * Stack of operators currently being processed
     *
     * Unary operators pop their operand, binary ones pop right op then left one
     */
    private Deque<Operator> operators;

    private Path temporaryFolder;

    private PhysicalPlanConfig config;

    public PhysicalPlanBuilder() {
        this(Paths.get(Project3.TEMP_PATH));
    }

    public PhysicalPlanBuilder(Path temporaryFolder) {
        this(PhysicalPlanConfig.DEFAULT_CONFIG, temporaryFolder);
    }

    public PhysicalPlanBuilder(PhysicalPlanConfig config, Path temporaryFolder) {
        this.operators = new ArrayDeque<>();
        this.config = config;
        this.temporaryFolder = temporaryFolder;
    }

    public Operator buildFromLogicalTree(LogicalOperator root) {
        root.accept(this);

        return operators.pollLast();
    }

    @Override
    public void visit(LogicalJoinOperator node) {
        node.getLeft().accept(this);
        node.getRight().accept(this);

        Operator rightOp = operators.pollLast();
        Operator leftOp = operators.pollLast();

        Operator join;

        switch (config.joinImplementation) {
            case TNLJ:
                join = new TupleNestedJoinOperator(leftOp, rightOp, node.getJoinCondition());
                break;
            case BNLJ:
                join = new BlockNestedJoinOperator(leftOp, rightOp, node.getJoinCondition(), config.joinParameter);
                break;
            case SMJ:
                SMJHeaderEvaluator smjEval = new SMJHeaderEvaluator(leftOp.getHeader(), rightOp.getHeader());
                node.getJoinCondition().accept(smjEval);

                TableHeader leftSortHeader = smjEval.getLeftSortHeader();
                TableHeader rightSortHeader = smjEval.getRightSortHeader();

                SortOperator leftOpSorted, rightOpSorted;

                if (config.sortImplementation == IN_MEMORY) {
                    leftOpSorted = new InMemorySortOperator(leftOp, leftSortHeader);
                    rightOpSorted = new InMemorySortOperator(rightOp, rightSortHeader);
                } else /* EXTERNAL */ {
                    leftOpSorted = new ExternalSortOperator(leftOp, leftSortHeader, config.sortParameter, temporaryFolder);
                    rightOpSorted = new ExternalSortOperator(rightOp, rightSortHeader, config.sortParameter, temporaryFolder);
                }

                join = new SortMergeJoinOperator(leftOpSorted, rightOpSorted, node.getJoinCondition());
                break;
            default:
                throw new NotImplementedException();
        }

        operators.add(join);
    }

    @Override
    public void visit(LogicalRenameOperator node) {
        node.getChild().accept(this);

        Operator rename = new RenameOperator(operators.pollLast(), node.getNewTableName());
        operators.add(rename);
    }

    @Override
    public void visit(LogicalScanOperator node) {
        Operator scan = new ScanOperator(node.getTable());
        operators.add(scan);
    }

    @Override
    public void visit(LogicalSelectOperator node) {
        node.getChild().accept(this);

        Operator select = new SelectionOperator(operators.pollLast(), node.getPredicate());
        operators.add(select);
    }

    @Override
    public void visit(LogicalSortOperator node) {
        node.getChild().accept(this);

        Operator sort;

        switch (config.sortImplementation) {

            case IN_MEMORY:
                sort = new InMemorySortOperator(operators.pollLast(), node.getSortHeader());
                break;
            case EXTERNAL:
                sort = new ExternalSortOperator(operators.pollLast(), node.getSortHeader(), config.sortParameter, temporaryFolder);
                break;
            default:
                throw new NotImplementedException();
        }

        operators.add(sort);
    }

    @Override
    public void visit(LogicalProjectOperator node) {
        node.getChild().accept(this);

        Operator project = new ProjectionOperator(operators.pollLast(), node.getHeader());
        operators.add(project);

    }

    @Override
    public void visit(LogicalDistinctOperator node) {
        node.getChild().accept(this);

        Operator distinct = new DistinctOperator(operators.pollLast());
        operators.add(distinct);
    }
}
