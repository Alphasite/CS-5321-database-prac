package db.query.visitors;

import db.datastore.TableHeader;
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

public class IndexScanEvaluator implements ExpressionVisitor{
    private TableInfo tableInfo;
    private Expression leftoverExpression;
    private Integer low, high;
    private Integer expressionVal;
    private Path indexesFolder;
    private boolean isIndexedCol;

    /**
     * Setup evaluator
     * @param header The header for the table we are doing an index scan on
     */
    public IndexScanEvaluator(TableHeader header, TableInfo tableInfo, Path indexesFolder) {
        this.tableInfo = tableInfo;
        this.leftoverExpression = null;
        this.low = null;
        this.high = null;
        this.expressionVal = null;
        this.isIndexedCol = false;
    }

    private Integer minNullCheck(Integer num1, Integer num2) {
        if (num1 == null) {
            return num2;
        }
        if (num2 == null) {
            return num1;
        }

        return Math.min(num1, num2);
    }

    private Integer maxNullCheck(Integer num1, Integer num2) {
        if (num1 == null) {
            return num2;
        }
        if (num2 == null) {
            return num1;
        }

        return Math.max(num1, num2);
    }

    private boolean usesIndexedCol(BinaryExpression binop) {
        Expression left = binop.getLeftExpression();
        Expression right = binop.getRightExpression();

        if (left instanceof LongValue || right instanceof LongValue) {
            assert !(left instanceof LongValue && right instanceof LongValue); // only one should be numeric

            binop.getLeftExpression().accept(this);
            binop.getRightExpression().accept(this);

            boolean temp = isIndexedCol;
            isIndexedCol = false;
            return temp;
        } else {
            return false;
        }
    }

    private void addToLeftover(Expression e) {
        if (leftoverExpression == null) {
            leftoverExpression = e;
        } else {
            leftoverExpression = new AndExpression(leftoverExpression, e);
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

    public BTree getIndexTree() {
        if (low == null && high == null) {
            // index cannot be used
            return null;
        } else {
            return BTree.createTree(indexesFolder.resolve(tableInfo.index.tableName + "." + tableInfo.index.attributeName));
        }
    }

    public Integer getLow() {
        return low;
    }

    public Integer getHigh() {
        return high;
    }

    @Override
    public void visit(LongValue longValue) {
        expressionVal = (int) longValue.getValue();
    }


    @Override
    public void visit(Column column) {
        if (column.getColumnName().equals(tableInfo.index.attributeName)) {
            isIndexedCol = true;
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
        if (usesIndexedCol(equalsTo)) {
            low = expressionVal;
            high = expressionVal;
        } else {
            addToLeftover(equalsTo);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        if (usesIndexedCol(greaterThan)) {
            if (greaterThan.getLeftExpression() instanceof LongValue) {
                high = minNullCheck(high, expressionVal - 1);
            } else {
                low = maxNullCheck(low, expressionVal + 1);
            }
        } else {
            addToLeftover(greaterThan);
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        if (usesIndexedCol(greaterThanEquals)) {
            if (greaterThanEquals.getLeftExpression() instanceof LongValue) {
                high = minNullCheck(high, expressionVal);
            } else {
                low = maxNullCheck(low, expressionVal);
            }
        } else {
            addToLeftover(greaterThanEquals);
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        if (usesIndexedCol(minorThan)) {
            if (minorThan.getLeftExpression() instanceof LongValue) {
                low = maxNullCheck(low, expressionVal + 1);
            } else {
                high = minNullCheck(high, expressionVal - 1);
            }
        } else {
            addToLeftover(minorThan);
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        if (usesIndexedCol(minorThanEquals)) {
            if (minorThanEquals.getLeftExpression() instanceof LongValue) {
                low = maxNullCheck(low, expressionVal);
            } else {
                high = minNullCheck(high, expressionVal);
            }
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
