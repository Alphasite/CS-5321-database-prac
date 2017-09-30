import datastore.Tuple;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.BeforeClass;
import org.junit.Test;
import query.ExpressionEvaluator;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExpressionEvaluatorTest {

	private static List<Tuple> tuples;

	@BeforeClass
	public static void loadData() {
		tuples = new ArrayList<>();
		tuples.add(new Tuple(Arrays.asList(1, 200, 50)));
		tuples.add(new Tuple(Arrays.asList(2, 200, 200)));
		tuples.add(new Tuple(Arrays.asList(3, 100, 105)));
		tuples.add(new Tuple(Arrays.asList(4, 100, 50)));
		tuples.add(new Tuple(Arrays.asList(5, 100, 500)));
		tuples.add(new Tuple(Arrays.asList(6, 300, 400)));
	}

	private static ExpressionEvaluator buildEvaluator(String query) {
		CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
		PlainSelect select;
		ExpressionEvaluator evaluator;

		try {
			select = (PlainSelect) parser.Select().getSelectBody();
			evaluator = new ExpressionEvaluator(select.getWhere());
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return evaluator;
	}

	/**
	 * Check that simple arithmetic and logic expressions are evaluated properly
	 */
	@Test
	public void testNakedCondition() {
		ExpressionEvaluator whereEvaluator = buildEvaluator("SELECT * FROM a WHERE 2 * 3 = 6 OR 2 = 6 - 3;");
		for (Tuple T : tuples) {
			assertTrue(whereEvaluator.matches(T));
		}

		whereEvaluator = buildEvaluator("SELECT * FROM a WHERE 1 + 2 = 3 AND 6 = 5 - 1;");
		for (Tuple T : tuples) {
			assertFalse(whereEvaluator.matches(T));
		}
	}


	@Test
	public void testSingleColumnReferences() {
		ExpressionEvaluator e = buildEvaluator("SELECT * FROM Sailors WHERE Sailors.B = 100;");
		assertFalse(e.matches(tuples.get(0)));
		assertFalse(e.matches(tuples.get(1)));
		assertTrue(e.matches(tuples.get(2)));
		assertTrue(e.matches(tuples.get(3)));
		assertTrue(e.matches(tuples.get(4)));
		assertFalse(e.matches(tuples.get(5)));

		e = buildEvaluator("SELECT * FROM Sailors WHERE Sailors.C > 100 AND Sailors.B <= 200;");
		assertFalse(e.matches(tuples.get(0)));
		assertTrue(e.matches(tuples.get(1)));
		assertTrue(e.matches(tuples.get(2)));
		assertFalse(e.matches(tuples.get(3)));
		assertTrue(e.matches(tuples.get(4)));
		assertFalse(e.matches(tuples.get(5)));
	}

	@Test
	public void testMultipleColumnReferences() {
		ExpressionEvaluator e = buildEvaluator("SELECT * FROM Sailors WHERE Sailors.B < Sailors.C;");
		assertFalse(e.matches(tuples.get(0)));
		assertFalse(e.matches(tuples.get(1)));
		assertTrue(e.matches(tuples.get(2)));
		assertFalse(e.matches(tuples.get(3)));
		assertFalse(e.matches(tuples.get(4)));
		assertTrue(e.matches(tuples.get(5)));
	}
}
