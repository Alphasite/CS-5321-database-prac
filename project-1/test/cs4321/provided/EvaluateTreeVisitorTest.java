package cs4321.provided;

import cs4321.project1.EvaluateTreeVisitor;
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
	
	@Test
	public void testSubtractionNode() {
		TreeNode n1 = new LeafTreeNode(1.0);
		TreeNode n2 = new LeafTreeNode(2.0);
		TreeNode n3 = new SubtractionTreeNode(n1, n2);
		TreeNode n4 = new SubtractionTreeNode(n2, n1);
        EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		n3.accept(v1);
		assertEquals(-1.0, v1.getResult(), DELTA);
        EvaluateTreeVisitor v2 = new EvaluateTreeVisitor();
		n4.accept(v2);
		assertEquals(1.0, v2.getResult(), DELTA);
	}
	
	@Test
	public void testDivisionNode() {
		TreeNode n1 = new LeafTreeNode(3.0);
		TreeNode n2 = new LeafTreeNode(6.0);
		TreeNode n3 = new DivisionTreeNode(n1, n2);
		TreeNode n4 = new DivisionTreeNode(n2, n1);
        EvaluateTreeVisitor v1 = new EvaluateTreeVisitor();
		n3.accept(v1);
		assertEquals(0.5, v1.getResult(), DELTA);
        EvaluateTreeVisitor v2 = new EvaluateTreeVisitor();
		n4.accept(v2);
		assertEquals(2.0, v2.getResult(), DELTA);
	}
	
	@Test
	public void testComplexExpressionTree(){
		TreeNode n1 = new LeafTreeNode(12.0); 
		TreeNode n2 = new LeafTreeNode(2.0); 
		TreeNode n3 = new LeafTreeNode(3.0); 
		TreeNode n4 = new UnaryMinusTreeNode(n1); 
		TreeNode n5 = new MultiplicationTreeNode(n2, n3);
        TreeNode n6 = new DivisionTreeNode(n4, n5);
        TreeNode n7 = new LeafTreeNode(5.0);
        TreeNode n8 = new AdditionTreeNode(n7, n6);
        EvaluateTreeVisitor v1 = new EvaluateTreeVisitor(); 
        n8.accept(v1);
		assertEquals(3.0, v1.getResult(), DELTA);
	}

}
