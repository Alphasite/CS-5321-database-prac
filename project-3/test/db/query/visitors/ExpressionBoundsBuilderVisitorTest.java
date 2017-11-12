package db.query.visitors;

import db.TestUtils;
import db.Utilities.UnionFind;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@SuppressWarnings("Duplicates")
public class ExpressionBoundsBuilderVisitorTest {
    @Test
    public void twoColumn() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4 AND Boats.D = Sailors.A;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A"));
        assertThat(unionFind.getMaximum("Boats.D"), is(4));
        assertThat(unionFind.getMaximum("Sailors.A"), is(4));
        assertThat(unionFind.getMinimum("Boats.D"), is(4));
        assertThat(unionFind.getMinimum("Sailors.A"), is(4));
        assertThat(unionFind.getEquals("Boats.D"), is(4));
        assertThat(unionFind.getEquals("Sailors.A"), is(4));
    }

    @Test
    public void twoColumnLessEqualThan() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D < 4 AND Boats.D = Sailors.A;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A"));
        assertThat(unionFind.getMaximum("Boats.D"), is(3));
        assertThat(unionFind.getMaximum("Sailors.A"), is(3));
        assertThat(unionFind.getMinimum("Boats.D"), nullValue());
        assertThat(unionFind.getMinimum("Sailors.A"), nullValue());
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());
    }

    @Test
    public void twoColumnLessThan() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D <= 4 AND Boats.D = Sailors.A;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A"));
        assertThat(unionFind.getMaximum("Boats.D"), is(4));
        assertThat(unionFind.getMaximum("Sailors.A"), is(4));
        assertThat(unionFind.getMinimum("Boats.D"), nullValue());
        assertThat(unionFind.getMinimum("Sailors.A"), nullValue());
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());
    }


    @Test
    public void twoColumnGreaterThan() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D > 4 AND Boats.D = Sailors.A;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A"));
        assertThat(unionFind.getMaximum("Boats.D"), nullValue());
        assertThat(unionFind.getMaximum("Sailors.A"), nullValue());
        assertThat(unionFind.getMinimum("Boats.D"), is(5));
        assertThat(unionFind.getMinimum("Sailors.A"), is(5));
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());
    }

    @Test
    public void twoColumnGreaterEqualThan() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D >= 4 AND Boats.D = Sailors.A;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A"));
        assertThat(unionFind.getMaximum("Boats.D"), nullValue());
        assertThat(unionFind.getMaximum("Sailors.A"), nullValue());
        assertThat(unionFind.getMinimum("Boats.D"), is(4));
        assertThat(unionFind.getMinimum("Sailors.A"), is(4));
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());
    }

    @Test
    public void twoColumnGreaterEqualThanFlipped() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE 4 < Boats.D AND Boats.D = Sailors.A;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A"));
        assertThat(unionFind.getMaximum("Boats.D"), nullValue());
        assertThat(unionFind.getMaximum("Sailors.A"), nullValue());
        assertThat(unionFind.getMinimum("Boats.D"), is(5));
        assertThat(unionFind.getMinimum("Sailors.A"), is(5));
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());
    }

    @Test
    public void twoColumnGreaterEqualAndLessEqualThan() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D < 100 AND Sailors.A > 4 AND Boats.D = Sailors.A;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A"));
        assertThat(unionFind.getMaximum("Boats.D"), is(99));
        assertThat(unionFind.getMaximum("Sailors.A"), is(99));
        assertThat(unionFind.getMinimum("Boats.D"), is(5));
        assertThat(unionFind.getMinimum("Sailors.A"), is(5));
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());
    }

    @Test
    public void threeColumn() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4 AND Boats.D = Sailors.A AND Sailors.A = Boats.E");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Boats.E");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A", "Boats.E"));

        assertThat(unionFind.getMaximum("Boats.D"), is(4));
        assertThat(unionFind.getMaximum("Boats.E"), is(4));
        assertThat(unionFind.getMaximum("Sailors.A"), is(4));
        assertThat(unionFind.getMinimum("Boats.D"), is(4));
        assertThat(unionFind.getMinimum("Boats.E"), is(4));
        assertThat(unionFind.getMinimum("Sailors.A"), is(4));
        assertThat(unionFind.getEquals("Boats.D"), is(4));
        assertThat(unionFind.getEquals("Boats.E"), is(4));
        assertThat(unionFind.getEquals("Sailors.A"), is(4));
    }

    @Test
    public void threeColumnRangeEqual() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4 AND Boats.D < 100 AND Boats.D > 2 AND Boats.D = Sailors.A AND Sailors.A = Boats.E");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Boats.E");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A", "Boats.E"));

        assertThat(unionFind.getMaximum("Boats.D"), is(4));
        assertThat(unionFind.getMaximum("Boats.E"), is(4));
        assertThat(unionFind.getMaximum("Sailors.A"), is(4));
        assertThat(unionFind.getMinimum("Boats.D"), is(4));
        assertThat(unionFind.getMinimum("Boats.E"), is(4));
        assertThat(unionFind.getMinimum("Sailors.A"), is(4));
        assertThat(unionFind.getEquals("Boats.D"), is(4));
        assertThat(unionFind.getEquals("Boats.E"), is(4));
        assertThat(unionFind.getEquals("Sailors.A"), is(4));
    }

    @Test
    public void threeColumnRange() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D < 4 AND Boats.D = Sailors.A AND Sailors.A = Boats.E");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Boats.E");
        unionFind.add("Sailors.A");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A", "Boats.E"));

        assertThat(unionFind.getMaximum("Boats.D"), is(3));
        assertThat(unionFind.getMaximum("Boats.E"), is(3));
        assertThat(unionFind.getMaximum("Sailors.A"), is(3));
        assertThat(unionFind.getMinimum("Boats.D"), nullValue());
        assertThat(unionFind.getMinimum("Boats.E"), nullValue());
        assertThat(unionFind.getMinimum("Sailors.A"), nullValue());
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Boats.E"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());
    }

    @Test
    public void independentCase() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4 AND Boats.D = Sailors.A AND Sailors.A = Boats.E And Sailors.B = Boats.F And Boats.F = 6;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Boats.E");
        unionFind.add("Boats.F");
        unionFind.add("Sailors.A");
        unionFind.add("Sailors.B");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(2));

        Set<String> set2 = sets.stream()
                .filter(set -> set.size() == 2)
                .findAny()
                .get();

        Set<String> set3 = sets.stream()
                .filter(set -> set.size() == 3)
                .findAny()
                .get();

        assertThat(set2, hasItems("Boats.F", "Sailors.B"));
        assertThat(set3, hasItems("Boats.D", "Sailors.A", "Boats.E"));

        assertThat(unionFind.getMaximum("Boats.D"), is(4));
        assertThat(unionFind.getMaximum("Boats.E"), is(4));
        assertThat(unionFind.getMaximum("Sailors.A"), is(4));
        assertThat(unionFind.getMinimum("Boats.D"), is(4));
        assertThat(unionFind.getMinimum("Boats.E"), is(4));
        assertThat(unionFind.getMinimum("Sailors.A"), is(4));
        assertThat(unionFind.getEquals("Boats.D"), is(4));
        assertThat(unionFind.getEquals("Boats.E"), is(4));
        assertThat(unionFind.getEquals("Sailors.A"), is(4));

        assertThat(unionFind.getMaximum("Boats.F"), is(6));
        assertThat(unionFind.getMaximum("Sailors.B"), is(6));
        assertThat(unionFind.getMinimum("Boats.F"), is(6));
        assertThat(unionFind.getMinimum("Sailors.B"), is(6));
        assertThat(unionFind.getEquals("Boats.F"), is(6));
        assertThat(unionFind.getEquals("Sailors.B"), is(6));
    }

    @Test
    public void independentCaseRange() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D < 4 AND Boats.D = Sailors.A AND Sailors.A = Boats.E And Sailors.B = Boats.F And Boats.F > 6 AND Boats.F <= 95;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        UnionFind unionFind = new UnionFind();
        unionFind.add("Boats.D");
        unionFind.add("Boats.E");
        unionFind.add("Boats.F");
        unionFind.add("Sailors.A");
        unionFind.add("Sailors.B");

        for (Expression joinExpression : decomposer.getJoinExpressions().values()) {
            ExpressionUnionBuilderVisitor.progressivelyBuildUnionFind(unionFind, joinExpression);
        }

        for (Expression selectionExpression : decomposer.getSelectionExpressions().values()) {
            ExpressionBoundsBuilderVisitor.progressivelyBuildUnionBounds(unionFind, selectionExpression);
        }

        List<Set<String>> sets = unionFind.getSets();

        assertThat(sets.size(), equalTo(2));

        Set<String> set2 = sets.stream()
                .filter(set -> set.size() == 2)
                .findAny()
                .get();

        Set<String> set3 = sets.stream()
                .filter(set -> set.size() == 3)
                .findAny()
                .get();

        assertThat(set2, hasItems("Boats.F", "Sailors.B"));
        assertThat(set3, hasItems("Boats.D", "Sailors.A", "Boats.E"));

        assertThat(unionFind.getMaximum("Boats.D"), is(3));
        assertThat(unionFind.getMaximum("Boats.E"), is(3));
        assertThat(unionFind.getMaximum("Sailors.A"), is(3));
        assertThat(unionFind.getMinimum("Boats.D"), nullValue());
        assertThat(unionFind.getMinimum("Boats.E"), nullValue());
        assertThat(unionFind.getMinimum("Sailors.A"), nullValue());
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Boats.E"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());

        assertThat(unionFind.getMaximum("Boats.F"), is(95));
        assertThat(unionFind.getMaximum("Sailors.B"), is(95));
        assertThat(unionFind.getMinimum("Boats.F"), is(7));
        assertThat(unionFind.getMinimum("Sailors.B"), is(7));
        assertThat(unionFind.getEquals("Boats.F"), nullValue());
        assertThat(unionFind.getEquals("Sailors.B"), nullValue());
    }
}