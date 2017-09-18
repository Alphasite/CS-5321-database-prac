package cs4321.provided;

import cs4321.project1.*;
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
		Parser p1 = new Parser("4.0 * 2.0 + 5.0");
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
		
		assertEquals(13.0, treeEvaluationResult, DELTA);
		assertEquals(13.0, prefixEvaluationResult, DELTA);
		assertEquals(13.0, postfixEvaluationResult, DELTA);

	}
	
	@Test
	public void testComplexExpression2() {
		Parser p1 = new Parser("4.0 * ( 2.0 + 5.0 )");
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
		
		assertEquals(28.0, treeEvaluationResult, DELTA);
		assertEquals(28.0, prefixEvaluationResult, DELTA);
		assertEquals(28.0, postfixEvaluationResult, DELTA);

	}
	
	@Test
	public void testComplexExpression3() {
		Parser p1 = new Parser("4.0 + 2.0 * 3.0");
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
		
		assertEquals(10.0, treeEvaluationResult, DELTA);
		assertEquals(10.0, prefixEvaluationResult, DELTA);
		assertEquals(10.0, postfixEvaluationResult, DELTA);

	}
	
	@Test
	public void testComplexExpression4() {
		Parser p1 = new Parser("5.0 + ( - 12.0 / ( 2.0 * 3.0 ) )");
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
		
		assertEquals(3.0, treeEvaluationResult, DELTA);
		assertEquals(3.0, prefixEvaluationResult, DELTA);
		assertEquals(3.0, postfixEvaluationResult, DELTA);

	}
	
	@Test
	public void testComplexExpression5() {
		Parser p1 = new Parser("3.0 - 2.0 - 4.0");
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
		
		assertEquals(-3.0, treeEvaluationResult, DELTA);
		assertEquals(-3.0, prefixEvaluationResult, DELTA);
		assertEquals(-3.0, postfixEvaluationResult, DELTA);

	}
	
	@Test
	public void testComplexExpression6() {
		Parser p1 = new Parser("3.0 - ( 2.0 - 4.0 )");
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
		
		assertEquals(5.0, treeEvaluationResult, DELTA);
		assertEquals(5.0, prefixEvaluationResult, DELTA);
		assertEquals(5.0, postfixEvaluationResult, DELTA);

	}
	@Test
	public void testExampleFromInstructions() {
		Parser p1 = new Parser("- 2.0 * ( 3.0 + 1.0 )");
		TreeNode parseResult1 = p1.parse();

		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		parseResult1.accept(v1);
		double treeEvaluationResult = v1.getResult();
		assertEquals(-8.0, treeEvaluationResult, DELTA);
		
		BuildPrefixExpressionTreeVisitor v2 = new BuildPrefixExpressionTreeVisitor();
		parseResult1.accept(v2);
		ListNode prefixRepresentation = v2.getResult();
		EvaluatePrefixListVisitor v3 = new EvaluatePrefixListVisitor();
		prefixRepresentation.accept(v3);
		double prefixEvaluationResult = v3.getResult();
		assertEquals(-8.0, prefixEvaluationResult, DELTA);
		
		PrintListVisitor pv1 = new PrintListVisitor();
		prefixRepresentation.accept(pv1);
		assertEquals( "* ~ 2.0 + 3.0 1.0", pv1.getResult());
		
		BuildPostfixExpressionTreeVisitor v4 = new BuildPostfixExpressionTreeVisitor();
		parseResult1.accept(v4);
		ListNode postfixRepresentation = v4.getResult();
		EvaluatePostfixListVisitor v5 = new EvaluatePostfixListVisitor();
		postfixRepresentation.accept(v5);
		double postfixEvaluationResult = v5.getResult();
		assertEquals(-8.0, postfixEvaluationResult, DELTA);
		
		PrintListVisitor pv2 = new PrintListVisitor();
		postfixRepresentation.accept(pv2);
		assertEquals("2.0 ~ 3.0 1.0 + *", pv2.getResult());

	}
}
