package cs4321.project1;

import cs4321.project1.list.ListNode;
import cs4321.project1.tree.TreeNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FullSystemTest {
	
	
	private static final double DELTA = 1e-15;
	
	@Test
	public void testSingleNumber() {
		Parser p1 = new Parser("1.0");
		TreeNode parseResult1 = p1.parse();

		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		parseResult1.accept(v1);
		double treeEvaluationResult = v1.getResult();
		
		BuildPrefixExpressionTreeVisitor v2 = new BuildPrefixExpressionTreeVisitor();
		parseResult1.accept(v2);
		ListNode prefixRepresentation = v2.getResult();
		EvaluatePrefixListVisitor v3 = new EvaluatePrefixListVisitor();
		prefixRepresentation.accept(v3);
		double prefixEvaluationResult = v3.getResult();
		
		BuildPostfixExpressionTreeVisitor v4 = new BuildPostfixExpressionTreeVisitor();
		parseResult1.accept(v4);
		ListNode postfixRepresentation = v4.getResult();
		EvaluatePostfixListVisitor v5 = new EvaluatePostfixListVisitor();
		postfixRepresentation.accept(v5);
		double postfixEvaluationResult = v5.getResult();
		
		assertEquals(1.0, treeEvaluationResult, DELTA);
		assertEquals(1.0, prefixEvaluationResult, DELTA);
		assertEquals(1.0, postfixEvaluationResult, DELTA);

	}
	
	@Test
	public void testAdditionSimple() {
		Parser p1 = new Parser("1.0 + 3.0");
		TreeNode parseResult1 = p1.parse();

		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		parseResult1.accept(v1);
		double treeEvaluationResult = v1.getResult();
		
		BuildPrefixExpressionTreeVisitor v2 = new BuildPrefixExpressionTreeVisitor();
		parseResult1.accept(v2);
		ListNode prefixRepresentation = v2.getResult();
		EvaluatePrefixListVisitor v3 = new EvaluatePrefixListVisitor();
		prefixRepresentation.accept(v3);
		double prefixEvaluationResult = v3.getResult();
		
		BuildPostfixExpressionTreeVisitor v4 = new BuildPostfixExpressionTreeVisitor();
		parseResult1.accept(v4);
		ListNode postfixRepresentation = v4.getResult();
		EvaluatePostfixListVisitor v5 = new EvaluatePostfixListVisitor();
		postfixRepresentation.accept(v5);
		double postfixEvaluationResult = v5.getResult();
		
		assertEquals(4.0, treeEvaluationResult, DELTA);
		assertEquals(4.0, prefixEvaluationResult, DELTA);
		assertEquals(4.0, postfixEvaluationResult, DELTA);

	}

	
	@Test
	public void testComplexExpression1() {
		Parser p1 = new Parser("4.0 * 2.0 + 3.0");
		TreeNode parseResult1 = p1.parse();

		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		parseResult1.accept(v1);
		double treeEvaluationResult = v1.getResult();
		
		BuildPrefixExpressionTreeVisitor v2 = new BuildPrefixExpressionTreeVisitor();
		parseResult1.accept(v2);
		ListNode prefixRepresentation = v2.getResult();
		EvaluatePrefixListVisitor v3 = new EvaluatePrefixListVisitor();
		prefixRepresentation.accept(v3);
		double prefixEvaluationResult = v3.getResult();
		
		BuildPostfixExpressionTreeVisitor v4 = new BuildPostfixExpressionTreeVisitor();
		parseResult1.accept(v4);
		ListNode postfixRepresentation = v4.getResult();
		EvaluatePostfixListVisitor v5 = new EvaluatePostfixListVisitor();
		postfixRepresentation.accept(v5);
		double postfixEvaluationResult = v5.getResult();

		PrintListVisitor printListVisitor = new PrintListVisitor();
		prefixRepresentation.accept(printListVisitor);
		System.out.println(printListVisitor.result);

		assertEquals(11.0, treeEvaluationResult, DELTA);
		assertEquals(11.0, prefixEvaluationResult, DELTA);
		assertEquals(11.0, postfixEvaluationResult, DELTA);

	}

	//ADDED TEST, testing the example provided in the instructions of the assignment
	@Test
	public void testExampleProvidedInInstructions() {
		Parser p = new Parser("- 2.0 * ( 3.0 + 1.0 )");
		TreeNode parseResult = p.parse();

		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		parseResult.accept(v1);
		double treeEvaluationResult = v1.getResult();

		BuildPrefixExpressionTreeVisitor v2 = new BuildPrefixExpressionTreeVisitor();
		parseResult.accept(v2);
		ListNode prefixRepresentation = v2.getResult();
		EvaluatePrefixListVisitor v3 = new EvaluatePrefixListVisitor();
		prefixRepresentation.accept(v3);
		double prefixEvaluationResult = v3.getResult();

		BuildPostfixExpressionTreeVisitor v4 = new BuildPostfixExpressionTreeVisitor();
		parseResult.accept(v4);
		ListNode postfixRepresentation = v4.getResult();
		EvaluatePostfixListVisitor v5 = new EvaluatePostfixListVisitor();
		postfixRepresentation.accept(v5);
		double postfixEvaluationResult = v5.getResult();

		PrintListVisitor printListVisitor = new PrintListVisitor();
		prefixRepresentation.accept(printListVisitor);
		System.out.println(printListVisitor.result);

		assertEquals(-8.0, treeEvaluationResult, DELTA);
		assertEquals(-8.0, prefixEvaluationResult, DELTA);
		assertEquals(-8.0, postfixEvaluationResult, DELTA);
	}

	//ADDED TEST, testing the example called "Example Complex A"
	@Test
	public void testExampleComplexA() {
		Parser p = new Parser("- ( - 5.0 ) * 3.0 - ( 1.0 + 4.0 / 2.0 )");
		TreeNode parseResult = p.parse();

		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		parseResult.accept(v1);
		double treeEvaluationResult = v1.getResult();

		BuildPrefixExpressionTreeVisitor v2 = new BuildPrefixExpressionTreeVisitor();
		parseResult.accept(v2);
		ListNode prefixRepresentation = v2.getResult();
		EvaluatePrefixListVisitor v3 = new EvaluatePrefixListVisitor();
		prefixRepresentation.accept(v3);
		double prefixEvaluationResult = v3.getResult();

		BuildPostfixExpressionTreeVisitor v4 = new BuildPostfixExpressionTreeVisitor();
		parseResult.accept(v4);
		ListNode postfixRepresentation = v4.getResult();
		EvaluatePostfixListVisitor v5 = new EvaluatePostfixListVisitor();
		postfixRepresentation.accept(v5);
		double postfixEvaluationResult = v5.getResult();

		PrintListVisitor printListVisitor = new PrintListVisitor();
		prefixRepresentation.accept(printListVisitor);
		System.out.println(printListVisitor.result);

		assertEquals(12.0, treeEvaluationResult, DELTA);
		assertEquals(12.0, prefixEvaluationResult, DELTA);
		assertEquals(12.0, postfixEvaluationResult, DELTA);
	}
	
}
