package cs4321.project1;

import cs4321.project1.list.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrintListVisitorTest {

	@Test
	public void testSingleNumberNode() {
		ListNode n1 = new NumberListNode(1.0);
		PrintListVisitor pv1 = new PrintListVisitor();
		n1.accept(pv1);
		assertEquals("1.0", pv1.getResult());
	}
	
	@Test
	public void testAdditionSimplePrefix() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new AdditionListNode();
		n3.setNext(n2);
		n2.setNext(n1);
		PrintListVisitor pv1 = new PrintListVisitor();
		n3.accept(pv1);
		assertEquals("+ 2.0 1.0", pv1.getResult());
	}
	
	@Test
	public void testAdditionSimplePostfix() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new AdditionListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		PrintListVisitor pv1 = new PrintListVisitor();
		n1.accept(pv1);
		assertEquals("1.0 2.0 +", pv1.getResult());
	}

	//ADDED TEST, testing the example provided in the instructions of the assignment for PREFIX
	@Test
	public void testExampleProvidedInInstructionsPrefix() {
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
		PrintListVisitor pv1 = new PrintListVisitor();
		n1.accept(pv1);
		assertEquals("* ~ 2.0 + 3.0 1.0", pv1.getResult());
	}

	//ADDED TEST, testing the example provided in the instructions of the assignment for POSTFIX
	@Test
	public void testExampleProvidedInInstructionsPostfix() {
		ListNode n1 = new NumberListNode(2.0D);
		ListNode n2 = new UnaryMinusListNode();
		ListNode n3 = new NumberListNode(3.0D);
		ListNode n4 = new NumberListNode(1.0D);
		ListNode n5 = new AdditionListNode();
		ListNode n6 = new MultiplicationListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		n3.setNext(n4);
		n4.setNext(n5);
		n5.setNext(n6);
		PrintListVisitor pv1 = new PrintListVisitor();
		n1.accept(pv1);
		assertEquals("2.0 ~ 3.0 1.0 + *", pv1.getResult());
	}
}
