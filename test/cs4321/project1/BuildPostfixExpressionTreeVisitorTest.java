package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.tree.*;
import org.junit.Test;

import static org.junit.Assert.*;

public class BuildPostfixExpressionTreeVisitorTest {

	private static final double DELTA = 1e-15;

	@Test
	public void testSingleLeafNode() {
		TreeNode n1 = new LeafTreeNode(1.0);
		BuildPostfixExpressionTreeVisitor v1 = new BuildPostfixExpressionTreeVisitor();
		n1.accept(v1);
		ListNode result = v1.getResult();
		assertNull(result.getNext());
		assertTrue(result instanceof NumberListNode);
	}

	@Test
	public void testAdditionNode() {
		TreeNode n1 = new LeafTreeNode(1.0);
		TreeNode n2 = new LeafTreeNode(2.0);
		TreeNode n3 = new AdditionTreeNode(n1, n2);
		TreeNode n4 = new AdditionTreeNode(n2, n1);

		BuildPostfixExpressionTreeVisitor v1 = new BuildPostfixExpressionTreeVisitor();
		n3.accept(v1);
		ListNode result = v1.getResult();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 1.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 2.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof AdditionListNode);
		assertNull(result.getNext());

		BuildPostfixExpressionTreeVisitor v2 = new BuildPostfixExpressionTreeVisitor();
		n4.accept(v2);
		result = v2.getResult();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 2.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 1.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof AdditionListNode);
		assertNull(result.getNext());
	}

    @Test
	public void testUnaryMinusNode() {
		TreeNode n1 = new LeafTreeNode(1.0);
		TreeNode n2 = new UnaryMinusTreeNode(n1);

		BuildPostfixExpressionTreeVisitor v1 = new BuildPostfixExpressionTreeVisitor();
		n2.accept(v1);
		ListNode result = v1.getResult();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 1.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof UnaryMinusListNode);
		assertNull(result.getNext());

	}

	//ADDED TEST, testing a Division Node
	@Test
	public void testDivisionNode() {
		TreeNode n1 = new LeafTreeNode(4.0);
		TreeNode n2 = new LeafTreeNode(2.0);
		TreeNode n3 = new DivisionTreeNode(n1, n2);
		TreeNode n4 = new DivisionTreeNode(n2, n1);

		BuildPostfixExpressionTreeVisitor v1 = new BuildPostfixExpressionTreeVisitor();
		n3.accept(v1);
		ListNode result = v1.getResult();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 4.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 2.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof DivisionListNode);
		assertNull(result.getNext());

		BuildPostfixExpressionTreeVisitor v2 = new BuildPostfixExpressionTreeVisitor();
		n4.accept(v2);
		result = v2.getResult();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 2.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode) result).getData(), 4.0, DELTA);
		result = result.getNext();
		assertTrue(result instanceof DivisionListNode);
		assertNull(result.getNext());
	}

    //ADDED TEST, testing the example provided in the instructions of the assignment
	@Test
	public void testExampleProvidedInInstructions() {
		TreeNode n1 = new LeafTreeNode(2.0);
		TreeNode n2 = new UnaryMinusTreeNode(n1);
		TreeNode n3 = new LeafTreeNode(3.0);
		TreeNode n4 = new LeafTreeNode(1.0);
		TreeNode n5 = new AdditionTreeNode(n3, n4);
		TreeNode n6 = new MultiplicationTreeNode(n2, n5);
		BuildPostfixExpressionTreeVisitor v1 = new BuildPostfixExpressionTreeVisitor();
		n6.accept(v1);
		ListNode result = v1.getResult();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode)result).getData(), 2.0, 1.0E-15);
		result = result.getNext();
		assertTrue(result instanceof UnaryMinusListNode);
		result = result.getNext();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode)result).getData(), 3.0, 1.0E-15);
		result = result.getNext();
		assertTrue(result instanceof NumberListNode);
		assertEquals(((NumberListNode)result).getData(), 1.0, 1.0E-15);
		result = result.getNext();
		assertTrue(result instanceof AdditionListNode);
		result = result.getNext();
		assertTrue(result instanceof MultiplicationListNode);
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
        BuildPostfixExpressionTreeVisitor v1 = new BuildPostfixExpressionTreeVisitor();
        n11.accept(v1);

        ListNode result = v1.getResult();
        assertTrue(result instanceof NumberListNode);
        assertEquals(((NumberListNode)result).getData(), 5.0, 1.0E-15);
        result = result.getNext();
        assertTrue(result instanceof UnaryMinusListNode);
        result = result.getNext();
        assertTrue(result instanceof UnaryMinusListNode);
        result = result.getNext();
        assertTrue(result instanceof NumberListNode);
        assertEquals(((NumberListNode)result).getData(), 3.0, 1.0E-15);
        result = result.getNext();
        assertTrue(result instanceof MultiplicationListNode);
        result = result.getNext();
        assertTrue(result instanceof NumberListNode);
        assertEquals(((NumberListNode)result).getData(), 1.0, 1.0E-15);
        result = result.getNext();
        assertTrue(result instanceof NumberListNode);
        assertEquals(((NumberListNode)result).getData(), 4.0, 1.0E-15);
        result = result.getNext();
        assertTrue(result instanceof NumberListNode);
        assertEquals(((NumberListNode)result).getData(), 2.0, 1.0E-15);
        result = result.getNext();
        assertTrue(result instanceof DivisionListNode);
        result = result.getNext();
        assertTrue(result instanceof AdditionListNode);
        result = result.getNext();
        assertTrue(result instanceof SubtractionListNode);
    }

}
