package db.query.visitors;

import db.Utilities.UnionFind;
import db.query.TablePair;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Another expression visitor, handles breaking the WHERE clause into multiple tokens and classify them based
 * on which tables they reference
 */
public class WhereDecomposer implements ExpressionVisitor {
    private Map<String, Expression> selectionExpressions;
    private Map<TablePair, Expression> joinExpressions;

    private Expression nakedExpression;

    private Queue<String> referencedTables;

    public WhereDecomposer() {
        this.selectionExpressions = new HashMap<>();
        this.joinExpressions = new HashMap<>();
        this.nakedExpression = null;

        this.referencedTables = new ArrayDeque<>();
    }

    public static WhereDecomposer decompose(Expression expression, UnionFind unionFind) {
        WhereDecomposer decomposer = new WhereDecomposer();
        expression.accept(decomposer);

        for (Expression joinExpression : decomposer.joinExpressions.values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.selectionExpressions.values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        return decomposer;
    }

    /**
     * @return the filter expressions
     */
    public Map<String, Expression> getSelectionExpressions() {
        return selectionExpressions;
    }

    /**
     * @return the join conditions
     */
    public Map<TablePair, Expression> getJoinExpressions() {
        return joinExpressions;
    }

    /**
     * @return the where expression without breaking it down in to join and filter expressions
     */
    public Expression getNakedExpression() {
        return nakedExpression;
    }

    /**
     * Add a selection expression to the internal list.
     *
     * @param tableId    the table which it operates on
     * @param comparison the expression
     */
    private void addSelection(String tableId, Expression comparison) {
        if (selectionExpressions.containsKey(tableId)) {
            // If a condition already exists for this key, compose it with an AND
            AndExpression andExpression = new AndExpression(comparison, selectionExpressions.get(tableId));
            selectionExpressions.put(tableId, andExpression);
        } else {
            selectionExpressions.put(tableId, comparison);
        }
    }

    /**
     * Add a join expression to the list
     * @param leftId the left relation
     * @param rightId the right relation
     * @param comparison the join expression
     */
    private void addJoin(String leftId, String rightId, Expression comparison) {
        TablePair key = new TablePair(leftId, rightId);

        if (joinExpressions.containsKey(key)) {
            // If a condition already exists for this key, compose it with an AND
            AndExpression andExpression = new AndExpression(comparison, joinExpressions.get(key));
            joinExpressions.put(key, andExpression);
        } else {
            joinExpressions.put(key, comparison);
        }
    }

    /**
     * Process a comparison node by retrieving referenced tables from the stack
     * @param comparator
     */
    private void processComparator(BinaryExpression comparator) {
        comparator.getLeftExpression().accept(this);
        comparator.getRightExpression().accept(this);

        // Retrieve child tables
        String tableLeft = referencedTables.poll();
        String tableRight = referencedTables.poll();

        // Handle constant expressions
        if (tableLeft == null) {
            if (nakedExpression != null) {
                nakedExpression = new AndExpression(nakedExpression, comparator);
            } else {
                nakedExpression = comparator;
            }
            return;
        }

        if (tableRight == null) {
            addSelection(tableLeft, comparator);
            return;
        }

        if (tableLeft.equals(tableRight)) {
            addSelection(tableLeft, comparator);
        } else {
            addJoin(tableLeft, tableRight, comparator);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(Column column) {
        this.referencedTables.offer(column.getTable().getName());
    }

    /**
     * @inheritDoc
     */
    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    // Not used.

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
    public void visit(LongValue longValue) {
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
