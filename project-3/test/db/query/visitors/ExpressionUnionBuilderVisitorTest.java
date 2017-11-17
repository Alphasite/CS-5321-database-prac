package db.query.visitors;

import db.TestUtils;
import db.datastore.Database;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class ExpressionUnionBuilderVisitorTest {
    private Database DB;

    @Before
    public void init() {
        this.DB = Database.loadDatabase(TestUtils.DB_PATH);
    }

    @Test
    public void twoColumn() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4 AND Boats.D = Sailors.A;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        List<Set<String>> sets = decomposer.getUnionFind().getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A"));
    }

    @Test
    public void threeColumn() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4 AND Boats.D = Sailors.A AND Sailors.A = Boats.E");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        List<Set<String>> sets = decomposer.getUnionFind().getSets();

        assertThat(sets.size(), equalTo(1));
        assertThat(sets.get(0), hasItems("Boats.D", "Sailors.A", "Boats.E"));
    }

    @Test
    public void independantCase() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4 AND Boats.D = Sailors.A AND Sailors.A = Boats.E And Sailors.B = Boats.F And Boats.F = 6;");
        WhereDecomposer decomposer = new WhereDecomposer();

        Expression expression = tokens.getWhere();

        expression.accept(decomposer);

        List<Set<String>> sets = decomposer.getUnionFind().getSets();

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
    }
}