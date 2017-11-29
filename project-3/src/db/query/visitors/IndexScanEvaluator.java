package db.query.visitors;

import db.Utilities.Pair;
import db.Utilities.Utilities;
import db.datastore.IndexInfo;
import db.datastore.TableInfo;
import db.datastore.index.BTree;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.file.Path;
import java.util.HashMap;

import static db.datastore.Database.PAGE_SIZE;

/**
 * Expression visitor used to collect the selection expressions that may be
 * optimized by an Index Scan and any leftover expressions
 */
public class IndexScanEvaluator implements ExpressionVisitor{
    private TableInfo tableInfo;
    private Expression leftoverExpression;
    private HashMap<String, Expression> leftoverExpressionCols;
    private Integer expressionVal;
    private Path indexesFolder;
    private String indexedColName;
    private HashMap<String, Integer> indicesLow;
    private HashMap<String, Integer> indicesHigh;
    private Integer bestLow;
    private Integer bestHigh;
    private IndexInfo bestIndexInfo;

    /**
     * Setup evaluator
     * @param tableInfo The tableInfo for the table we are doing an index scan on
     * @param indexesFolder The path to the folder containing our indexes
     */
    public IndexScanEvaluator(TableInfo tableInfo, Path indexesFolder) {
        this.tableInfo = tableInfo;
        this.leftoverExpression = null;
        leftoverExpressionCols = new HashMap<>();
        this.expressionVal = null;
        this.indexedColName = null;
        this.indexesFolder = indexesFolder;
        this.indicesLow = new HashMap<>();
        this.indicesHigh = new HashMap<>();
    }

    /**
     * Returns num2 if num1 is null, num1 if num2 is null, and the minimum value of
     * the two if neither is null.
     * @param num1
     * @param num2
     * @return The minimum value of num1 and num2, or null if both are null
     */
    private Integer minNullCheck(Integer num1, Integer num2) {
        if (num1 == null) {
            return num2;
        }
        if (num2 == null) {
            return num1;
        }

        return Math.min(num1, num2);
    }

    /**
     * Returns num2 if num1 is null, num1 if num2 is null, and the maximum value of
     * the two if neither is null.
     * @param num1
     * @param num2
     * @return The maximum value of num1 and num2, or null if both are null
     */
    private Integer maxNullCheck(Integer num1, Integer num2) {
        if (num1 == null) {
            return num2;
        }
        if (num2 == null) {
            return num1;
        }

        return Math.max(num1, num2);
    }

    /**
     * Visits this BinaryExpression's children and if
     * one of the children is an indexed column of this table AND the other
     * child is a numeric (Long) value, return the name of that column,
     * otherwise null.
     * Side effect: Sets indexColName to null
     *
     * @param binop The BinaryExpression we are currently looking at
     * @return true if the conditions above hold, false otherwise
     */
    private String getIndexedColName(BinaryExpression binop) {
        Expression left = binop.getLeftExpression();
        Expression right = binop.getRightExpression();

        if (left instanceof LongValue || right instanceof LongValue) {
            assert !(left instanceof LongValue && right instanceof LongValue); // only one should be numeric

            binop.getLeftExpression().accept(this);
            binop.getRightExpression().accept(this);

            String temp = indexedColName;
            indexedColName = null;
            return temp;
        } else {
            return null;
        }
    }

    /**
     * Adds the given expression to the leftoverExpression field.
     * @param e The expression to add to the leftoverExpression field
     */
    private void addToLeftover(Expression e) {
        if (leftoverExpression == null) {
            leftoverExpression = e;
        } else {
            leftoverExpression = new AndExpression(leftoverExpression, e);
        }
    }

    private void addToColumnExpression(Expression e, String colName) {
        if (leftoverExpressionCols.containsKey(colName)) {
            leftoverExpressionCols.put(colName, new AndExpression(leftoverExpressionCols.get(colName), e));
        } else {
            leftoverExpressionCols.put(colName, e);
        }
    }

    /**
     * Gets any remaining expressions, i.e. conditions involving columns other
     * than indexed column and not-equals-to conditions, which
     * cannot be optimized by the Sort-Merge Join
     * @return An expression that consists of all non-equijoin conditions used
     *         for this Sort-Merge Join, or null if all of the expressions are
     *         equijoins
     */
    public Expression getLeftoverExpression() {
        return this.leftoverExpression;
    }

    /**
     * Gets a Pair of BTree and Expression representing the tree index that can be
     * used to scan this table and the expressions not handled by this tree, or null
     * if no such tree exists.
     * Side effect: Sets the values of bestLow, bestHigh, and bestIndexInfo
     *
     * @return A Pair of BTree and Expression if this table can be optimized with an
     *         IndexScan, otherwise null
     */
    public Pair<BTree, Expression> getBestIndexTree() {

        // get number of pages/tuples in relation
        int numPages = (int) (tableInfo.file.toFile().length() / PAGE_SIZE);
        int numTuples = tableInfo.getStats().count;

        int tupleSize = tableInfo.header.size() * 4;

        // set the initial minimum to the cost of the scan
        IndexInfo minimumIndex = null;
        double minimumCost = numTuples * tupleSize / PAGE_SIZE;

        for (IndexInfo info : tableInfo.indices) {

            // calculate reduction factor
            int colIndex = tableInfo.header.columnNames.indexOf(info.attributeName);
            int minimum = tableInfo.getStats().minimums[colIndex];
            int maximum = tableInfo.getStats().maximums[colIndex];
            int low = indicesLow.getOrDefault(info.attributeName, minimum);
            int high = indicesHigh.getOrDefault(info.attributeName, maximum);
            double reductionFactor = ((double) (high - low + 1)) / ((double) maximum - minimum + 1);

            // get number of leaves in index tree
            Path indexFile = indexesFolder.resolve(info.tableName + "." + info.attributeName);
            assert indexFile.toFile().exists();
            int numLeaves = BTree.createTree(indexFile).getNbLeaves();

            // calculate cost
            double cost;
            if (info.isClustered) {
                cost = 3 + numPages * reductionFactor;
            } else {
                cost = 3 + numLeaves * reductionFactor + numTuples * reductionFactor;
            }

            if (cost < minimumCost) {
                minimumCost = cost;
                minimumIndex = info;
            }

        }

        if (minimumIndex == null) {
            // most efficient is full scan
            return null;
        } else {
            bestLow = indicesLow.getOrDefault(minimumIndex.attributeName, null);
            bestHigh = indicesHigh.getOrDefault(minimumIndex.attributeName, null);
            bestIndexInfo = minimumIndex;

            Expression leftover = leftoverExpression;
            for (String col : leftoverExpressionCols.keySet()) {
                if (!col.equals(minimumIndex.attributeName)) {
                    leftover = Utilities.joinExpression(leftover, leftoverExpressionCols.get(col));
                }
            }
            return new Pair<>(
                    BTree.createTree(indexesFolder.resolve(minimumIndex.tableName + "." + minimumIndex.attributeName)),
                    leftover
            );
        }
    }

    /**
     * Gets the value of indicesLow.
     *
     * @return the value of indicesLow
     */
    public HashMap<String, Integer> getIndicesLow() {
        return indicesLow;
    }

    /**
     * Gets the value of indicesHigh.
     *
     * @return the value of indicesHigh
     */
    public HashMap<String, Integer> getIndicesHigh() {
        return indicesHigh;
    }

    /**
     * Gets the value of bestLow.
     *
     * @return the value of bestLow
     */
    public Integer getBestLow() {
        return this.bestLow;
    }

    /**
     * Gets the value of bestHigh.
     *
     * @return the value of bestHigh
     */
    public Integer getBestHigh() {
        return this.bestHigh;
    }

    public IndexInfo getBestIndexInfo() {
        return bestIndexInfo;
    }

    /*
     * ExpressionVisitor methods
     */

    @Override
    public void visit(LongValue longValue) {
        expressionVal = (int) longValue.getValue();
    }

    @Override
    public void visit(Column column) {
        for (IndexInfo info : tableInfo.indices) {
            if (info.attributeName.equals(column.getColumnName())) {
                indexedColName = column.getColumnName();
                break;
            }
        }
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        String childIndexedCol = getIndexedColName(equalsTo);

        if (childIndexedCol != null) {
            indicesLow.put(childIndexedCol, expressionVal);
            indicesHigh.put(childIndexedCol, expressionVal);
            addToColumnExpression(equalsTo, childIndexedCol);
        } else {
            addToLeftover(equalsTo);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        String childIndexedCol = getIndexedColName(greaterThan);

        if (childIndexedCol != null) {
            if (greaterThan.getLeftExpression() instanceof LongValue) {
                Integer oldHigh = indicesHigh.getOrDefault(childIndexedCol, null);
                indicesHigh.put(childIndexedCol, minNullCheck(oldHigh, expressionVal - 1));
            } else {
                Integer oldLow = indicesLow.getOrDefault(childIndexedCol, null);
                indicesLow.put(childIndexedCol, maxNullCheck(oldLow, expressionVal + 1));
            }
            addToColumnExpression(greaterThan, childIndexedCol);
        } else {
            addToLeftover(greaterThan);
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        String childIndexedCol = getIndexedColName(greaterThanEquals);

        if (childIndexedCol != null) {
            if (greaterThanEquals.getLeftExpression() instanceof LongValue) {
                Integer oldHigh = indicesHigh.getOrDefault(childIndexedCol, null);
                indicesHigh.put(childIndexedCol, minNullCheck(oldHigh, expressionVal));
            } else {
                Integer oldLow = indicesLow.getOrDefault(childIndexedCol, null);
                indicesLow.put(childIndexedCol, maxNullCheck(oldLow, expressionVal));
            }
            addToColumnExpression(greaterThanEquals, childIndexedCol);
        } else {
            addToLeftover(greaterThanEquals);
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        String childIndexedCol = getIndexedColName(minorThan);

        if (childIndexedCol != null) {
            if (minorThan.getLeftExpression() instanceof LongValue) {
                Integer oldLow = indicesLow.getOrDefault(childIndexedCol, null);
                indicesLow.put(childIndexedCol, maxNullCheck(oldLow, expressionVal + 1));
            } else {
                Integer oldHigh = indicesHigh.getOrDefault(childIndexedCol, null);
                indicesHigh.put(childIndexedCol, minNullCheck(oldHigh, expressionVal - 1));
            }
            addToColumnExpression(minorThan, childIndexedCol);
        } else {
            addToLeftover(minorThan);
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        String childIndexedCol = getIndexedColName(minorThanEquals);

        if (childIndexedCol != null) {
            if (minorThanEquals.getLeftExpression() instanceof LongValue) {
                Integer oldLow = indicesLow.getOrDefault(childIndexedCol, null);
                indicesLow.put(childIndexedCol, maxNullCheck(oldLow, expressionVal));
            } else {
                Integer oldHigh = indicesHigh.getOrDefault(childIndexedCol, null);
                indicesHigh.put(childIndexedCol, minNullCheck(oldHigh, expressionVal));
            }
            addToColumnExpression(minorThanEquals, childIndexedCol);
        } else {
            addToLeftover(minorThanEquals);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        addToLeftover(notEqualsTo);
    }

    @Override
    public void visit(Addition addition) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Division division) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Multiplication multiplication) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Subtraction subtraction) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(OrExpression orExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Between between) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(InExpression inExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(NullValue nullValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Function function) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(InverseExpression inverseExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(StringValue stringValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(DateValue dateValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(TimeValue timeValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(WhenClause whenClause) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Concat concat) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Matches matches) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        throw new NotImplementedException();
    }
}
