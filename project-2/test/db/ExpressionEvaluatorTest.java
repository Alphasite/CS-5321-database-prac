package db;

import db.datastore.TableHeader;
import db.datastore.Tuple;
import db.query.ExpressionEvaluator;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExpressionEvaluatorTest {

	private static List<Tuple> tuples;
	private static TableHeader header;

	@BeforeClass
	public static void loadData() {
		tuples = new ArrayList<>();
		tuples.add(new Tuple(Arrays.asList(1, 200, 50)));
		tuples.add(new Tuple(Arrays.asList(2, 200, 200)));
		tuples.add(new Tuple(Arrays.asList(3, 100, 105)));
		tuples.add(new Tuple(Arrays.asList(4, 100, 50)));
		tuples.add(new Tuple(Arrays.asList(5, 100, 500)));
		tuples.add(new Tuple(Arrays.asList(6, 300, 400)));

		List<String> tableNames = Arrays.asList("Sailors", "Sailors", "Sailors");
		List<String> columnNames = Arrays.asList("A", "B", "C");
		header = new TableHeader(tableNames, columnNames);
	}

	/**
	 * Check that simple arithmetic and logic expressions are evaluated properly
	 */
	@Test
	public void testNakedCondition() {
		PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM a WHERE 2 * 3 = 6 OR 2 = 6 - 3;");
		ExpressionEvaluator whereEvaluator = new ExpressionEvaluator(tokens.getWhere(), header);
		for (Tuple T : tuples) {
			assertTrue(whereEvaluator.matches(T));
		}

		tokens = TestUtils.parseQuery("SELECT * FROM a WHERE 1 + 2 = 3 AND 6 = 5 - 1;");
		whereEvaluator = new ExpressionEvaluator(tokens.getWhere(), header);
		for (Tuple T : tuples) {
			assertFalse(whereEvaluator.matches(T));
		}
	}

	/**
	 * Check that column identifier resolution works properly
	 */
	@Test
	public void testSingleColumnReferences() {
		PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors WHERE Sailors.B = 100;");
		ExpressionEvaluator e = new ExpressionEvaluator(tokens.getWhere(), header);
		assertFalse(e.matches(tuples.get(0)));
		assertFalse(e.matches(tuples.get(1)));
		assertTrue(e.matches(tuples.get(2)));
		assertTrue(e.matches(tuples.get(3)));
		assertTrue(e.matches(tuples.get(4)));
		assertFalse(e.matches(tuples.get(5)));

		tokens = TestUtils.parseQuery("SELECT * FROM Sailors WHERE Sailors.C > 100 AND Sailors.B <= 200;");
		e = new ExpressionEvaluator(tokens.getWhere(), header);
		assertFalse(e.matches(tuples.get(0)));
		assertTrue(e.matches(tuples.get(1)));
		assertTrue(e.matches(tuples.get(2)));
		assertFalse(e.matches(tuples.get(3)));
		assertTrue(e.matches(tuples.get(4)));
		assertFalse(e.matches(tuples.get(5)));
	}

	/**
	 * Check that column identifier resolution works properly on both operands
	 */
	@Test
	public void testMultipleColumnReferences() {
		PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors WHERE Sailors.B < Sailors.C;");
		ExpressionEvaluator e = new ExpressionEvaluator(tokens.getWhere(), header);
		assertFalse(e.matches(tuples.get(0)));
		assertFalse(e.matches(tuples.get(1)));
		assertTrue(e.matches(tuples.get(2)));
		assertFalse(e.matches(tuples.get(3)));
		assertTrue(e.matches(tuples.get(4)));
		assertTrue(e.matches(tuples.get(5)));
	}
}
