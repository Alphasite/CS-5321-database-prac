package query;

import datastore.TableHeader;
import datastore.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ExpressionEvaluator implements ExpressionVisitor {

    private boolean result;
    private long value;

    private Expression expressionRoot;
    private Tuple tuple;
    private TableHeader schema;

    public ExpressionEvaluator(Expression expression, TableHeader schema) {
        this.result = false;
        this.value = 0;
        this.expressionRoot = expression;
        this.schema = schema;
    }

    public boolean matches(Tuple tuple) {
        this.result = false;
        this.value = 0;
        this.tuple = tuple;

        expressionRoot.accept(this);

        return this.result;
    }

    public Expression getExpression() {
        return this.expressionRoot;
    }

    @Override
    public void visit(Column column) {
        // TODO: implement alias/column name resolution
        // TODO: move this computation to object construction

        for (int i = 0; i < schema.columnHeaders.size(); i++) {
            if (schema.columnAliases.get(i).equals(column.getTable().getName()) && schema.columnHeaders.get(i).equals(column.getColumnName())) {
                this.value = tuple.fields.get(i);
                return;
            }
        }

        System.out.println(new Exception("Invalid column reference : " + column).getMessage());
    }

    @Override
    public void visit(LongValue longValue) {
        this.value = longValue.getValue();
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(Addition addition) {
        addition.getLeftExpression().accept(this);
        long lhs = this.value;
        addition.getRightExpression().accept(this);
        long rhs = this.value;

        this.value = lhs + rhs;
    }

    @Override
    public void visit(Division division) {
        division.getLeftExpression().accept(this);
        long lhs = this.value;
        division.getRightExpression().accept(this);
        long rhs = this.value;

        this.value = lhs / rhs;
    }

    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        long lhs = this.value;
        multiplication.getRightExpression().accept(this);
        long rhs = this.value;

        this.value = lhs * rhs;
    }

    @Override
    public void visit(Subtraction subtraction) {
        subtraction.getLeftExpression().accept(this);
        long lhs = this.value;
        subtraction.getRightExpression().accept(this);
        long rhs = this.value;

        this.value = lhs - rhs;
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean lhs = this.result;
        andExpression.getRightExpression().accept(this);
        boolean rhs = this.result;

        this.result = lhs && rhs;
    }

    @Override
    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        boolean lhs = this.result;
        orExpression.getRightExpression().accept(this);
        boolean rhs = this.result;

        this.result = lhs || rhs;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        long lhs = this.value;
        equalsTo.getRightExpression().accept(this);
        long rhs = this.value;

        this.result = (lhs == rhs);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        long lhs = this.value;
        greaterThan.getRightExpression().accept(this);
        long rhs = this.value;

        this.result = (lhs > rhs);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        long lhs = this.value;
        greaterThanEquals.getRightExpression().accept(this);
        long rhs = this.value;

        this.result = (lhs >= rhs);
    }

    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        long lhs = this.value;
        minorThan.getRightExpression().accept(this);
        long rhs = this.value;

        this.result = (lhs < rhs);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        long lhs = this.value;
        minorThanEquals.getRightExpression().accept(this);
        long rhs = this.value;

        this.result = (lhs <= rhs);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        long lhs = this.value;
        notEqualsTo.getRightExpression().accept(this);
        long rhs = this.value;

        this.result = (lhs != rhs);
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
