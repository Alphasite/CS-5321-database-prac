package db.query;

import db.Utilities;
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
import java.util.Map;

/**
 * Another expression visitor, handles breaking the WHERE clause into multiple tokens and classify them based
 * on which tables they reference
 */
public class WhereDecomposer implements ExpressionVisitor {
    private Map<String, Expression> selectionExpressions;
    private Map<TableCouple, Expression> joinExpressions;

    public WhereDecomposer(Expression expression) {
        this.selectionExpressions = new HashMap<>();
        this.joinExpressions = new HashMap<>();
        expression.accept(this);
    }

    public Map<String, Expression> getSelectionExpressions() {
        return selectionExpressions;
    }

    public Map<TableCouple, Expression> getJoinExpressions() {
        return joinExpressions;
    }

    private void processComparator(BinaryExpression comparator) {
        // Handle join predicates
        if (comparator.getLeftExpression() instanceof Column && comparator.getRightExpression() instanceof Column) {
            Table table1 = ((Column) comparator.getLeftExpression()).getTable();
            Table table2 = ((Column) comparator.getRightExpression()).getTable();

            TableCouple key = new TableCouple(table1, table2);
            if (joinExpressions.containsKey(key)) {
                // If a condition already exists for this key, compose it with an AND
                AndExpression andExpression = new AndExpression(comparator, joinExpressions.get(key));
                joinExpressions.put(key, andExpression);
            } else {
                joinExpressions.put(key, comparator);
            }
        } else {
            // Handle selection predicates
            Table table;

            if (comparator.getLeftExpression() instanceof Column) {
                table = ((Column) comparator.getLeftExpression()).getTable();
            } else if (comparator.getRightExpression() instanceof Column) {
                table = ((Column) comparator.getRightExpression()).getTable();
            } else {
                throw new NotImplementedException();
            }

            String identifier = Utilities.getIdentifier(table);

            if (selectionExpressions.containsKey(identifier)) {
                // If a condition already exists for this key, compose it with an AND
                AndExpression andExpression = new AndExpression(comparator, selectionExpressions.get(identifier));
                selectionExpressions.put(identifier, andExpression);
            } else {
                selectionExpressions.put(identifier, comparator);
            }
        }
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
        processComparator(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        processComparator(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        processComparator(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        processComparator(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        processComparator(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        processComparator(notEqualsTo);
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
