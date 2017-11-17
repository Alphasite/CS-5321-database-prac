package db.query.visitors;

import db.PhysicalPlanConfig;
import db.Utilities.Pair;
import db.Utilities.UnionFind;
import db.Utilities.Utilities;
import db.datastore.IndexInfo;
import db.datastore.TableHeader;
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
import net.sf.jsqlparser.expression.Expression;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.file.Path;
import java.util.*;

import static db.PhysicalPlanConfig.SortImplementation.IN_MEMORY;
import static db.Utilities.Utilities.*;

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

    private UnionFind unionFind;

    private Path temporaryFolder;
    private Path indexesFolder;

    private PhysicalPlanConfig config;

    public PhysicalPlanBuilder(Path temporaryFolder, Path indexesFolder) {
        this(PhysicalPlanConfig.DEFAULT_CONFIG, temporaryFolder, indexesFolder);
    }

    public PhysicalPlanBuilder(PhysicalPlanConfig config, Path temporaryFolder, Path indexesFolder) {
        this.operators = new ArrayDeque<>();
        this.config = config;
        this.temporaryFolder = temporaryFolder;
        this.indexesFolder = indexesFolder;
        this.unionFind = new UnionFind();
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
        this.unionFind = node.getUnionFind();

        Map<String, Set<String>> columnToEqualitySetMapping = new HashMap<>();
        Map<Set<String>, Set<String>> equalitySetToUsedSetMap = new HashMap<>();

        for (Set<String> equalitySet : this.unionFind.getSets()) {
            for (String column : equalitySet) {
                columnToEqualitySetMapping.put(column, equalitySet);
            }

            equalitySetToUsedSetMap.put(equalitySet, new HashSet<>());
        }

        List<LogicalScanOperator> children = node.getChildren();

        for (LogicalScanOperator scan : children) {
            scan.accept(this);
        }

        Operator join = operators.poll();


        for (int i = 1; i < children.size(); i++) {
            Expression condition = null;
            Operator joined = operators.poll();

            TableHeader header = LogicalJoinOperator.computeHeader(join.getHeader(), joined.getHeader());
            for (int j = 0; j < header.size(); j++) {
                String column = header.columnAliases.get(j) + "." + header.columnHeaders.get(j);
                Set<String> equalitySet = columnToEqualitySetMapping.get(column);
                Set<String> usedEqualitySet = equalitySetToUsedSetMap.get(equalitySet);

                if (equalitySet.size() > 1 && usedEqualitySet.size() > 0 && !usedEqualitySet.contains(column)) {
                    String equalColumn = usedEqualitySet.stream()
                            .findAny()
                            .get();

                    condition = Utilities.joinExpression(condition, Utilities.equalPairToExpression(column, equalColumn));
                }

                usedEqualitySet.add(column);
            }

            // TODO expand unused expressions here

            switch (config.joinImplementation) {
                case TNLJ:
                    join = new TupleNestedJoinOperator(join, joined, condition);
                    break;
                case BNLJ:
                    join = new BlockNestedJoinOperator(join, joined, condition, config.joinParameter);
                    break;
                case SMJ:
                    SMJHeaderEvaluator smjEval = new SMJHeaderEvaluator(join.getHeader(), joined.getHeader());
                    if (condition != null) {
                        condition.accept(smjEval);

                        TableHeader leftSortHeader = smjEval.getLeftSortHeader();
                        TableHeader rightSortHeader = smjEval.getRightSortHeader();
                        Expression leftoverJoinCondition = smjEval.getLeftoverExpression();

                        if (leftSortHeader.size() > 0 && rightSortHeader.size() > 0) {
                            SortOperator leftOpSorted, rightOpSorted;

                            if (config.sortImplementation == IN_MEMORY) {
                                leftOpSorted = new InMemorySortOperator(join, leftSortHeader);
                                rightOpSorted = new InMemorySortOperator(joined, rightSortHeader);
                            } else /* EXTERNAL */ {
                                leftOpSorted = new ExternalSortOperator(join, leftSortHeader, config.sortParameter, temporaryFolder);
                                rightOpSorted = new ExternalSortOperator(joined, rightSortHeader, config.sortParameter, temporaryFolder);
                            }

                            join = new SortMergeJoinOperator(leftOpSorted, rightOpSorted, leftoverJoinCondition);
                        } else {
                            // when no equijoins, just use BNLJ
                            join = new BlockNestedJoinOperator(join, joined, condition, config.joinParameter);
                        }
                    } else {
                        join = new BlockNestedJoinOperator(join, joined, null, config.joinParameter);
                    }
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        operators.push(join);

        Expression unusedExpression = null;
        for (Pair<TablePair, Expression> tablePairExpressionPair : node.getUnusedExpressions()) {
            unusedExpression = joinExpression(unusedExpression, tablePairExpressionPair.getRight());
        }

        if (unusedExpression != null) {
            operators.push(new SelectionOperator(operators.poll(), unusedExpression));
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalScanOperator scan) {
        TableHeader header = scan.getHeader();

        ScanOperator scanOperator = new ScanOperator(scan.getTable(), scan.getTableName());

        Expression expression = null;

        for (int i = 0; i < header.size(); i++) {
            String path = header.columnAliases.get(i) + "." + header.columnHeaders.get(i);

            if (unionFind.getMinimum(path) != null) {
                expression = joinExpression(expression, greaterThanColumn(path, unionFind.getMinimum(path)));
            }

            if (unionFind.getMaximum(path) != null) {
                expression = joinExpression(expression, lessThanColumn(path, unionFind.getMaximum(path)));
            }

            for (Set<String> equalitySet : unionFind.getSets()) {
                List<String> equalHeaders = new ArrayList<>();

                for (String column : equalitySet) {
                    Pair<String, String> splitColumn = splitLongFormColumn(column);
                    if (splitColumn.getLeft().equals(scan.getTableName())) {
                        equalHeaders.add(splitColumn.getRight());
                    }
                }

                if (equalHeaders.size() > 1) {
                    for (int j = 1; j < equalHeaders.size(); j++) {
                        Expression equalityExpression = Utilities.equalPairToExpression(
                                scan.getTableName() + "." + equalHeaders.get(j - 1),
                                scan.getTableName() + "." + equalHeaders.get(j)
                        );

                        expression = joinExpression(expression, equalityExpression);
                    }
                }
            }
        }

        if (expression != null) {
            operators.add(new SelectionOperator(scanOperator, expression));
        } else {
            operators.add(scanOperator);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(LogicalSelectOperator node) {
        node.getChild().accept(this);

        ScanOperator child = (ScanOperator) operators.pollLast();

        if (config.useIndices) {
            IndexInfo indexInfo = child.getTable().indices.get(0);
            IndexScanEvaluator scanEval = new IndexScanEvaluator(child.getTable(), indexInfo, indexesFolder);
            node.getPredicate().accept(scanEval);

            BTree treeIndex = scanEval.getIndexTree();
            Expression leftovers = scanEval.getLeftoverExpression();
            if (treeIndex != null && leftovers != null) {
                IndexScanOperator scanOp = new IndexScanOperator(child.getTable(), indexInfo, treeIndex, scanEval.getLow(), scanEval.getHigh());
                SelectionOperator select = new SelectionOperator(scanOp, leftovers);
                operators.add(select);
            } else if (treeIndex != null && leftovers == null) {
                IndexScanOperator scanOp = new IndexScanOperator(child.getTable(), indexInfo, treeIndex, scanEval.getLow(), scanEval.getHigh());
                operators.add(scanOp);
            } else /* treeIndex == null, essentially just a regular scan and select */ {
                Operator select = new SelectionOperator(child, node.getPredicate());
                operators.add(select);
            }
        } else {
            Operator select = new SelectionOperator(child, node.getPredicate());
            operators.add(select);
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
