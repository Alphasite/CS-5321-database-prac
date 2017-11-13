package db.query.visitors;

import db.PhysicalPlanConfig;
import db.datastore.IndexInfo;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.index.BTree;
import db.operators.logical.*;
import db.operators.physical.Operator;
import db.operators.physical.bag.*;
import db.operators.physical.extended.DistinctOperator;
import db.operators.physical.extended.ExternalSortOperator;
import db.operators.physical.extended.InMemorySortOperator;
import db.operators.physical.extended.SortOperator;
import db.operators.physical.physical.IndexScanOperator;
import db.operators.physical.physical.ScanOperator;
import net.sf.jsqlparser.expression.Expression;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.file.Path;
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

    /**
     * Last table referenced by a leaf node (ie. all operators on current path reference this table).
     * Set to null when encountering a join.
     */
    private TableInfo currentTable;

    /**
     * Alias used when referencing current source table, or null if none present.
     */
    private String currentTableAlias;

    private Path temporaryFolder;
    private Path indexesFolder;

    private PhysicalPlanConfig config;

    public PhysicalPlanBuilder(Path temporaryFolder, Path indexesFolder) {
        this(PhysicalPlanConfig.DEFAULT_CONFIG, temporaryFolder, indexesFolder);
    }

    public PhysicalPlanBuilder(PhysicalPlanConfig config, Path temporaryFolder, Path indexesFolder) {
        this.config = config;
        this.temporaryFolder = temporaryFolder;
        this.indexesFolder = indexesFolder;

        this.operators = new ArrayDeque<>();
        this.currentTable = null;
        this.currentTableAlias = null;
    }

    public Operator buildFromLogicalTree(LogicalOperator root) {
        root.accept(this);

        return operators.pollLast();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalJoinOperator node) {
        node.getLeft().accept(this);
        node.getRight().accept(this);

        // We are no longer on a single path, remove references to leaf node
        this.currentTable = null;
        this.currentTableAlias = null;

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
                Expression leftoverJoinCondition = smjEval.getLeftoverExpression();

                if (leftSortHeader.size() > 0 && rightSortHeader.size() > 0) {
                    SortOperator leftOpSorted, rightOpSorted;

                    if (config.sortImplementation == IN_MEMORY) {
                        leftOpSorted = new InMemorySortOperator(leftOp, leftSortHeader);
                        rightOpSorted = new InMemorySortOperator(rightOp, rightSortHeader);
                    } else /* EXTERNAL */ {
                        leftOpSorted = new ExternalSortOperator(leftOp, leftSortHeader, config.sortParameter, temporaryFolder);
                        rightOpSorted = new ExternalSortOperator(rightOp, rightSortHeader, config.sortParameter, temporaryFolder);
                    }

                    join = new SortMergeJoinOperator(leftOpSorted, rightOpSorted, leftoverJoinCondition);
                } else {
                    // when no equijoins, just use BNLJ
                    join = new BlockNestedJoinOperator(leftOp, rightOp, node.getJoinCondition(), config.joinParameter);
                }
                break;
            default:
                throw new NotImplementedException();
        }

        operators.add(join);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalRenameOperator node) {
        node.getChild().accept(this);

        Operator rename = new RenameOperator(operators.pollLast(), node.getNewTableName());
        operators.add(rename);

        this.currentTableAlias = node.getNewTableName();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalScanOperator node) {
        Operator scan = new ScanOperator(node.getTable());
        operators.add(scan);

        // Update leaf node info for future uses
        this.currentTable = node.getTable();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalSelectOperator node) {
        node.getChild().accept(this);

        LogicalOperator source = node.getChild();
        boolean singleSource = (source instanceof LogicalScanOperator || source instanceof LogicalRenameOperator);

        // If we don't or can't use indices just add a selection operator
        if (!config.useIndices || !singleSource) {
            Operator select = new SelectionOperator(operators.pollLast(), node.getPredicate());
            operators.add(select);
            return;
        }

        // Handle index scan and renaming
        // Preconditions : source is either Scan or Scan + Rename, anything else is likely to break

        IndexInfo indexInfo = this.currentTable.indices.get(0);
        IndexScanEvaluator scanEval = new IndexScanEvaluator(this.currentTable, indexInfo, indexesFolder);
        node.getPredicate().accept(scanEval);

        BTree treeIndex = scanEval.getIndexTree();
        Expression leftovers = scanEval.getLeftoverExpression();

        Operator currentOp = operators.pollLast();

        if (treeIndex == null) {
            // Regular scan -> (rename) -> select
            Operator select = new SelectionOperator(currentOp, node.getPredicate());
            operators.add(select);
        } else {
            // Replace scan with indexed scan, add rename if needed
            Operator op = new IndexScanOperator(this.currentTable, indexInfo, treeIndex, scanEval.getLow(), scanEval.getHigh());
            if (this.currentTableAlias != null) {
                op = new RenameOperator(op, this.currentTableAlias);
            }

            if (leftovers != null) {
                // Add a selection operator to handle leftovers
                op = new SelectionOperator(op, leftovers);
            }

            operators.add(op);
        }
    }

    /**
     * @inheritDoc
     */
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

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalProjectOperator node) {
        node.getChild().accept(this);

        Operator project = new ProjectionOperator(operators.pollLast(), node.getHeader());
        operators.add(project);

    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalDistinctOperator node) {
        node.getChild().accept(this);

        Operator distinct = new DistinctOperator(operators.pollLast());
        operators.add(distinct);
    }
}
