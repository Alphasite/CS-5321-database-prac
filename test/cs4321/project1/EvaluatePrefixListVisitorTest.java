package cs4321.project1;

import cs4321.project1.list.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EvaluatePrefixListVisitorTest {
	
	private static final double DELTA = 1e-15;

	@Test
	public void testSingleNumberNode() {
		ListNode n1 = new NumberListNode(1.0);
		EvaluatePrefixListVisitor v1 = new EvaluatePrefixListVisitor();
		n1.accept(v1);
		assertEquals(1.0, v1.getResult(), DELTA);
	}

	@Test
	public void testAdditionSimple() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new AdditionListNode();
		n3.setNext(n2);
		n2.setNext(n1);
		EvaluatePrefixListVisitor v1 = new EvaluatePrefixListVisitor();
		n3.accept(v1);
		assertEquals(3.0, v1.getResult(), DELTA);
		
		ListNode n4 = new NumberListNode(1.0);
		ListNode n5 = new NumberListNode(2.0);
		ListNode n6 = new AdditionListNode();
		n6.setNext(n5);
		n5.setNext(n4);
		EvaluatePrefixListVisitor v2 = new EvaluatePrefixListVisitor();
		n6.accept(v2);
		assertEquals(3.0, v2.getResult(), DELTA);
	}
	
	@Test
	public void testAdditionMultipleInstances() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new AdditionListNode();
		ListNode n4 = new NumberListNode(3.0);
		ListNode n5 = new AdditionListNode();
		n5.setNext(n4);
		n4.setNext(n3);
		n3.setNext(n2);
		n2.setNext(n1);
		EvaluatePrefixListVisitor v1 = new EvaluatePrefixListVisitor();
		n5.accept(v1);
		assertEquals(6.0, v1.getResult(), DELTA);
	}

	//ADDED TEST, testing the example provided in the instructions of the assignment
	@Test
	public void testExampleProvidedInInstructions() {
		ListNode n1 = new MultiplicationListNode();
		ListNode n2 = new UnaryMinusListNode();
		ListNode n3 = new NumberListNode(2.0D);
		ListNode n4 = new AdditionListNode();
		ListNode n5 = new NumberListNode(3.0D);
		ListNode n6 = new NumberListNode(1.0D);
		n1.setNext(n2);
		n2.setNext(n3);
		n3.setNext(n4);
		n4.setNext(n5);
		n5.setNext(n6);
		EvaluatePrefixListVisitor v1 = new EvaluatePrefixListVisitor();
		n1.accept(v1);
		assertEquals(-8.0D, v1.getResult(), 1.0E-15D);
	}

}
