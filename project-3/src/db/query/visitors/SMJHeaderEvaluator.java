package db.query.visitors;

import db.datastore.TableHeader;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Expression visitor used to collect the sort conditions and leftover join
 * expressions for a Sort-Merge Join
 */
public class SMJHeaderEvaluator implements ExpressionVisitor {
    private TableHeader leftHeader, rightHeader;
    private TableHeader leftSortHeader, rightSortHeader;
    private Expression leftoverExpression;

    /**
     * Setup evaluator
     * @param leftHeader The header for the left operator of the Sort-Merge Join
     * @param rightHeader The header for the right operator of the Sort-Merge Join
     */
    public SMJHeaderEvaluator(TableHeader leftHeader, TableHeader rightHeader) {
        this.leftHeader = leftHeader;
        this.rightHeader = rightHeader;
        this.leftSortHeader = new TableHeader();
        this.rightSortHeader = new TableHeader();
        this.leftoverExpression = null;
    }

    /**
     * Gets the sort header for the left operator
     * @return The sort header for the left operator
     */
    public TableHeader getLeftSortHeader() {
        return this.leftSortHeader;
    }

    /**
     * Gets the sort header for the right operator
     * @return The sort header for the right operator
     */
    public TableHeader getRightSortHeader() {
        return this.rightSortHeader;
    }

    /**
     * Gets any remaining expressions, i.e. non-equijoin conditions, which
     * cannot be optimized by the Sort-Merge Join
     * @return An expression that consists of all non-equijoin conditions used
     *         for this Sort-Merge Join, or null if all of the expressions are
     *         equijoins
     */
    public Expression getLeftoverExpression() {
        return this.leftoverExpression;
    }

    @Override
    public void visit(Column column) {
        String columnAlias = column.getTable().getName();
        String columnName = column.getColumnName();

        for (int i = 0; i < leftHeader.columnHeaders.size(); i++) {
            if (leftHeader.columnAliases.get(i).equals(columnAlias) && leftHeader.columnHeaders.get(i).equals(columnName)) {
                this.leftSortHeader.columnAliases.add(columnAlias);
                this.leftSortHeader.columnHeaders.add(columnName);
                return;
            }
        }

        for (int i = 0; i < rightHeader.columnHeaders.size(); i++) {
            if (rightHeader.columnAliases.get(i).equals(columnAlias) && rightHeader.columnHeaders.get(i).equals(columnName)) {
                this.rightSortHeader.columnAliases.add(columnAlias);
                this.rightSortHeader.columnHeaders.add(columnName);
                return;
            }
        }

        System.out.println(new Exception("Invalid column reference : " + column).getMessage());
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
        equalsTo.getLeftExpression().accept(this);
        equalsTo.getRightExpression().accept(this);

        assert this.leftSortHeader.size() == this.rightSortHeader.size();
    }

    public void handleNonEquijoin(Expression expression) {
        if (leftoverExpression == null) {
            leftoverExpression = expression;
        } else {
            leftoverExpression = new AndExpression(leftoverExpression, expression);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        handleNonEquijoin(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        handleNonEquijoin(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        handleNonEquijoin(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        handleNonEquijoin(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        handleNonEquijoin(notEqualsTo);
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
    public void visit(LongValue longValue) {
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
