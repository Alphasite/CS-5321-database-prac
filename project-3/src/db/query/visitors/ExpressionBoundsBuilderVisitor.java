package db.query.visitors;

import db.Utilities.UnionFind;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ExpressionBoundsBuilderVisitor implements ExpressionVisitor {
    private UnionFind unionFind;

    private boolean valueLeft;
    private boolean valueRight;

    private Integer value;
    private String column;

    public ExpressionBoundsBuilderVisitor(UnionFind unionFind) {
        this.unionFind = unionFind;
        this.value = null;
    }

    public static void progressivelyBuildUnionBounds(UnionFind unionFind, Expression expression) {
        ExpressionBoundsBuilderVisitor visitor = new ExpressionBoundsBuilderVisitor(unionFind);
        expression.accept(visitor);
    }

    private void tryGetValue(BinaryExpression expression) {
        this.valueLeft = false;
        this.valueRight = false;
        this.value = null;

        expression.getLeftExpression().accept(this);
        if (this.value != null) {
            this.valueLeft = true;
        }

        expression.getRightExpression().accept(this);
        if (this.value != null && !this.valueLeft) {
            this.valueRight = true;
        }
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        tryGetValue(equalsTo);

        if (value != null && column != null) {
            unionFind.setEquals(column, value);
        } else {
            throw new RuntimeException("Expression has no value or no column.");
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        tryGetValue(greaterThan);

        if (value != null && column != null) {
            if (this.valueLeft) {
                // 0 > column
                unionFind.setMaximum(column, value - 1);
            }

            if (this.valueRight) {
                // column > 0
                unionFind.setMinimum(column, value + 1);
            }
        } else {
            throw new RuntimeException("Expression has no value or no column.");
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        tryGetValue(greaterThanEquals);

        if (value != null && column != null) {
            if (this.valueLeft) {
                // 0 >= column
                unionFind.setMaximum(column, value);
            }

            if (this.valueRight) {
                // column >= 0
                unionFind.setMinimum(column, value);
            }
        } else {
            throw new RuntimeException("Expression has no value or no column.");
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        tryGetValue(minorThan);

        if (value != null && column != null) {
            if (this.valueLeft) {
                // 0 < column
                unionFind.setMinimum(column, value + 1);
            }

            if (this.valueRight) {
                // column < 0
                unionFind.setMaximum(column, value - 1);
            }
        } else {
            throw new RuntimeException("Expression has no value or no column: " + minorThan.getStringExpression());
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        tryGetValue(minorThanEquals);

        if (value != null && column != null) {
            if (this.valueLeft) {
                // 0 <= column
                unionFind.setMinimum(column, value);
            }

            if (this.valueRight) {
                // column <= 0
                unionFind.setMaximum(column, value);
            }
        } else {
            throw new RuntimeException("Expression has no value or no column.");
        }
    }

    @Override
    public void visit(Column column) {
        this.column = column.getWholeColumnName();
        this.unionFind.add(this.column);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(LongValue longValue) {
        this.value = (int) longValue.getValue();
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(Function function) {

    }

    @Override
    public void visit(InverseExpression inverseExpression) {

    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {

    }

    @Override
    public void visit(DoubleValue doubleValue) {

    }

    @Override
    public void visit(DateValue dateValue) {

    }

    @Override
    public void visit(TimeValue timeValue) {

    }

    @Override
    public void visit(TimestampValue timestampValue) {

    }

    @Override
    public void visit(StringValue stringValue) {

    }

    @Override
    public void visit(Addition addition) {

    }

    @Override
    public void visit(Division division) {

    }

    @Override
    public void visit(Multiplication multiplication) {

    }

    @Override
    public void visit(Subtraction subtraction) {

    }

    @Override
    public void visit(OrExpression orExpression) {

    }

    @Override
    public void visit(Between between) {

    }

    @Override
    public void visit(InExpression inExpression) {

    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    @Override
    public void visit(LikeExpression likeExpression) {

    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {

    }

    @Override
    public void visit(SubSelect subSelect) {

    }

    @Override
    public void visit(CaseExpression caseExpression) {

    }

    @Override
    public void visit(WhenClause whenClause) {

    }

    @Override
    public void visit(ExistsExpression existsExpression) {

    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    public void visit(Concat concat) {

    }

    @Override
    public void visit(Matches matches) {

    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {

    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {

    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {

    }
}
