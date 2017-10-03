package query;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;

public class BreakWhereBuilder implements ExpressionVisitor {
    HashMap<Table, Expression> hashSelection;
    HashMap<TableCouple, Expression> hashJoin;


    public BreakWhereBuilder(Expression expression) {
        this.hashSelection = new HashMap<>();
        this.hashJoin = new HashMap<>();
        expression.accept(this);


    }

    public HashMap<Table, Expression> getHashSelection() {
        return hashSelection;
    }

    public HashMap<TableCouple, Expression> getHashJoin() {
        return hashJoin;
    }

    @Override
    public void visit(Column column) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(LongValue longValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
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
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(OrExpression orExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        comparisonOperator(equalsTo);
    }

    private void comparisonOperator(BinaryExpression comparator) {
        if (comparator.getLeftExpression() instanceof Column && comparator.getRightExpression() instanceof Column) {
            Table table1 = ((Column) comparator.getLeftExpression()).getTable();
            Table table2 = ((Column) comparator.getRightExpression()).getTable();
            hashJoin.put(new TableCouple(table1, table2), comparator);
        } else {
            if (comparator.getLeftExpression() instanceof Column) {
                Table table = ((Column) comparator.getLeftExpression()).getTable();
                if (hashSelection.containsKey(table)) {
                    AndExpression andExpression = new AndExpression(comparator, hashSelection.get(table));
                    hashSelection.put(table, andExpression);
                } else {
                    hashSelection.put(table, comparator);
                }
            } else {
                if (comparator.getRightExpression() instanceof Column) {
                    Table table = ((Column) comparator.getRightExpression()).getTable();
                    if (hashSelection.containsKey(table)) {
                        AndExpression andExpression = new AndExpression(comparator, hashSelection.get(table));
                        hashSelection.put(table, andExpression);
                    } else {
                        hashSelection.put(table, comparator);
                    }
                } else throw new NotImplementedException();
            }
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        comparisonOperator(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        comparisonOperator(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        comparisonOperator(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        comparisonOperator(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        comparisonOperator(notEqualsTo);
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
