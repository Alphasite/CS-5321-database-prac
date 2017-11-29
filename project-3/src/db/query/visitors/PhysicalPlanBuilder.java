package db.query.visitors;

import db.PhysicalPlanConfig;
import db.Utilities.Pair;
import db.Utilities.Utilities;
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
import db.query.TablePair;
import db.query.optimizer.JoinOrderOptimizer;
import net.sf.jsqlparser.expression.Expression;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.file.Path;
import java.util.*;

import static db.PhysicalPlanConfig.SortImplementation.IN_MEMORY;
import static db.Utilities.Utilities.joinExpression;

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

        Map<String, Set<String>> columnToEqualitySetMapping = new HashMap<>();
        Map<Set<String>, Set<String>> equalitySetToUsedSetMap = new HashMap<>();

        for (Set<String> equalitySet : node.getUnionFind().getSets()) {
            for (String column : equalitySet) {
                columnToEqualitySetMapping.put(column, equalitySet);
            }

            equalitySetToUsedSetMap.put(equalitySet, new HashSet<>());
        }

        // BEGIN NEW CODE

        JoinOrderOptimizer optimizer = new JoinOrderOptimizer(node);
        List<LogicalOperator> joinPlan = optimizer.computeBestJoinOrder();

        for (LogicalOperator source : joinPlan) {
            source.accept(this);
        }

        // Operators are appended at the end of the queue so we have to retrieve them from the front (FIFO order)

        Operator root = this.operators.remove();

        while (!this.operators.isEmpty()) {
            Operator joinWith = this.operators.remove();

            root = createJoin(root, joinWith, columnToEqualitySetMapping, equalitySetToUsedSetMap);
        }

        this.operators.offer(root);

        // END OF NEW CODE

        // We are no longer on a single path, remove references to leaf node
        this.currentTable = null;

        Expression unusedExpression = null;
        for (Pair<TablePair, Expression> tablePairExpressionPair : node.getUnusedExpressions()) {
            unusedExpression = joinExpression(unusedExpression, tablePairExpressionPair.getRight());
        }

        if (unusedExpression != null) {
            operators.push(new SelectionOperator(operators.pollLast(), unusedExpression));
        }
    }

    private Operator createJoin(Operator inner, Operator outer, Map<String, Set<String>> c2e, Map<Set<String>, Set<String>> e2s) {
        Operator join;
        Expression joinCondition = null;
        TableHeader header = LogicalJoinOperator.computeHeader(inner.getHeader(), outer.getHeader());

        for (String attribute : header.getQualifiedAttributeNames()) {
            Set<String> equalitySet = c2e.get(attribute);
            Set<String> usedEqualitySet = e2s.get(equalitySet);

            if (equalitySet.size() > 1 && usedEqualitySet.size() > 0 && !usedEqualitySet.contains(attribute)) {
                String equalColumn = usedEqualitySet.stream()
                        .findAny()
                        .get();

                joinCondition = Utilities.joinExpression(joinCondition, Utilities.equalPairToExpression(attribute, equalColumn));
            }

            usedEqualitySet.add(attribute);
        }

        switch (config.joinImplementation) {
            // TODO: find another method of choosing joins
            case TNLJ:
                join = new TupleNestedJoinOperator(inner, outer, joinCondition);
                break;
            case BNLJ:
                join = new BlockNestedJoinOperator(inner, outer, joinCondition, config.joinParameter);
                break;
            case SMJ:
                SMJHeaderEvaluator smjEval = new SMJHeaderEvaluator(inner.getHeader(), outer.getHeader());
                if (joinCondition != null) {
                    joinCondition.accept(smjEval);

                    TableHeader leftSortHeader = smjEval.getLeftSortHeader();
                    TableHeader rightSortHeader = smjEval.getRightSortHeader();

                    if (leftSortHeader.size() > 0 && rightSortHeader.size() > 0) {
                        SortOperator leftOpSorted, rightOpSorted;

                        if (config.sortImplementation == IN_MEMORY) {
                            leftOpSorted = new InMemorySortOperator(inner, leftSortHeader);
                            rightOpSorted = new InMemorySortOperator(outer, rightSortHeader);
                        } else /* EXTERNAL */ {
                            leftOpSorted = new ExternalSortOperator(inner, leftSortHeader, config.sortParameter, temporaryFolder);
                            rightOpSorted = new ExternalSortOperator(outer, rightSortHeader, config.sortParameter, temporaryFolder);
                        }

                        join = new SortMergeJoinOperator(leftOpSorted, rightOpSorted, joinCondition);
                    } else {
                        // when no equijoins, just use BNLJ
                        join = new BlockNestedJoinOperator(inner, outer, joinCondition, config.joinParameter);
                    }
                } else {
                    join = new BlockNestedJoinOperator(inner, outer, null, config.joinParameter);
                }
                break;
            default:
                throw new NotImplementedException();
        }

        return join;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalScanOperator node) {
        // Update leaf node info for future uses
        this.currentTable = node.getTable();

        operators.add(new ScanOperator(node.getTable(), node.getTableAlias()));
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalSelectOperator node) {
        node.getChild().accept(this);

        LogicalOperator source = node.getChild();
        boolean singleSource = (source instanceof LogicalScanOperator);

        // If we don't or can't use indices just add a selection operator
        if (!config.useIndices || !singleSource) {
            Operator select = new SelectionOperator(operators.pollLast(), node.getPredicate());
            operators.add(select);
            return;
        }

        // Handle index scan and renaming
        // Preconditions : source is either Scan, anything else is likely to break

        LogicalScanOperator sourceScan = (LogicalScanOperator) source;

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
            Operator op = new IndexScanOperator(this.currentTable, sourceScan.getTableAlias(), indexInfo, treeIndex, scanEval.getLow(), scanEval.getHigh());

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
