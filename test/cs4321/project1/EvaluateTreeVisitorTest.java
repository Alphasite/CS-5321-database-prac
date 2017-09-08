package cs4321.project1;

import cs4321.project1.tree.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EvaluateTreeVisitorTest {

	private static final double DELTA = 1e-15;

	@Test
	public void testSingleLeafNode() {
		TreeNode n1 = new LeafTreeNode(1.0);
        EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		n1.accept(v1);
		assertEquals(1.0, v1.getResult(), DELTA);
	}
	
	@Test
	public void testAdditionNode() {
		TreeNode n1 = new LeafTreeNode(1.0);
		TreeNode n2 = new LeafTreeNode(2.0);
		TreeNode n3 = new AdditionTreeNode(n1, n2);
		TreeNode n4 = new AdditionTreeNode(n2, n1);
        EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		n3.accept(v1);
		assertEquals(3.0, v1.getResult(), DELTA);
        EvaluateTreeVisitor v2 = new EvaluateTreeVisitor();
		n4.accept(v2);
		assertEquals(3.0, v2.getResult(), DELTA);
	}
	
	@Test
	public void testMultiplicationNode() {
		TreeNode n1 = new LeafTreeNode(1.0);
		TreeNode n2 = new LeafTreeNode(2.0);
		TreeNode n3 = new MultiplicationTreeNode(n1, n2);
		TreeNode n4 = new MultiplicationTreeNode(n2, n1);
        EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		n3.accept(v1);
		assertEquals(2.0, v1.getResult(), DELTA);
        EvaluateTreeVisitor v2 = new EvaluateTreeVisitor();
		n4.accept(v2);
		assertEquals(2.0, v2.getResult(), DELTA);
	}

	//ADDED TEST, testing a simple UnaryMinus TreeNode
	@Test
	public void UnaryMinusTreeNode() {
		TreeNode n1 = new LeafTreeNode(3.0);
		TreeNode n2 = new UnaryMinusTreeNode(n1);
		TreeNode n3 = new UnaryMinusTreeNode(n2);
		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		n2.accept(v1);
		assertEquals(-3.0, v1.getResult(), 1.0E-15);
		EvaluateTreeVisitor v2 = new EvaluateTreeVisitor();
		n3.accept(v2);
		assertEquals(3.0, v2.getResult(), 1.0E-15);
	}

	//ADDED TEST, testing the example provided in the instructions of the assignment
	@Test
	public void testExampleProvidedInInstructions() {
		TreeNode n1 = new LeafTreeNode(1.0);
		TreeNode n2 = new LeafTreeNode(2.0);
		TreeNode n3 = new LeafTreeNode(3.0);
		TreeNode n4 = new AdditionTreeNode(n1,n3);
		TreeNode n5 = new UnaryMinusTreeNode(n2);
		TreeNode n6 = new MultiplicationTreeNode(n4, n5);
		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		n6.accept(v1);
		assertEquals(-8.0, v1.getResult(), DELTA);
	}

	//ADDED TEST, testing the example called "Example Complex A"
	@Test
	public void testExampleComplexA() {
		TreeNode n1 = new LeafTreeNode(1.0);
		TreeNode n2 = new LeafTreeNode(2.0);
		TreeNode n3 = new LeafTreeNode(3.0);
		TreeNode n4 = new LeafTreeNode(4.0);
		TreeNode n5 = new LeafTreeNode(5.0);
		TreeNode n6 = new UnaryMinusTreeNode(n5);
		TreeNode n7 = new UnaryMinusTreeNode(n6);
		TreeNode n8 = new MultiplicationTreeNode(n7, n3);
		TreeNode n9 = new DivisionTreeNode(n4, n2);
		TreeNode n10 = new AdditionTreeNode(n1, n9);
		TreeNode n11 = new SubtractionTreeNode(n8, n10);

		EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		n11.accept(v1);
		assertEquals(12.0, v1.getResult(), DELTA);
	}
}
