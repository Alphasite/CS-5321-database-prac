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

	//ADDED TEST, testing the example provided in the instructions of the assignment for a PrefixList
	@Test
	public void testExampleProvidedInInstructionsPrefix() {
		ListNode n1 = new MultiplicationListNode();
		ListNode n2 = new UnaryMinusListNode();
		ListNode n3 = new NumberListNode(2.0);
		ListNode n4 = new AdditionListNode();
		ListNode n5 = new NumberListNode(3.0);
		ListNode n6 = new NumberListNode(1.0);
		n1.setNext(n2);
		n2.setNext(n3);
		n3.setNext(n4);
		n4.setNext(n5);
		n5.setNext(n6);
		PrintListVisitor pv1 = new PrintListVisitor();
		n1.accept(pv1);
		assertEquals("* ~ 2.0 + 3.0 1.0", pv1.getResult());
	}

	//ADDED TEST, testing the example provided in the instructions of the assignment for a PostfixList
	@Test
	public void testExampleProvidedInInstructionsPostfix() {
		ListNode n1 = new NumberListNode(2.0);
		ListNode n2 = new UnaryMinusListNode();
		ListNode n3 = new NumberListNode(3.0);
		ListNode n4 = new NumberListNode(1.0);
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

    //ADDED TEST, testing the example called "Example Complex A" for a PrefixList
    @Test
    public void testExampleComplexAPrefix() {
        ListNode n1 = new SubtractionListNode();
        ListNode n2 = new MultiplicationListNode();
        ListNode n3 = new UnaryMinusListNode();
        ListNode n4 = new UnaryMinusListNode();
        ListNode n5 = new NumberListNode(5.0);
        ListNode n6 = new NumberListNode(3.0);
        ListNode n7 = new AdditionListNode();
        ListNode n8 = new NumberListNode(1.0);
        ListNode n9 = new DivisionListNode();
        ListNode n10 = new NumberListNode(4.0);
        ListNode n11 = new NumberListNode(2.0);
        n1.setNext(n2);
        n2.setNext(n3);
        n3.setNext(n4);
        n4.setNext(n5);
        n5.setNext(n6);
        n6.setNext(n7);
        n7.setNext(n8);
        n8.setNext(n9);
        n9.setNext(n10);
        n10.setNext(n11);

        PrintListVisitor pv1 = new PrintListVisitor();
        n1.accept(pv1);
        assertEquals("- * ~ ~ 5.0 3.0 + 1.0 / 4.0 2.0", pv1.getResult());
    }

    //ADDED TEST, testing the example called "Example Complex A" for a PostfixList
    @Test
    public void testExampleComplexAPostfix() {
        ListNode n1 = new NumberListNode(5.0);
        ListNode n2 = new UnaryMinusListNode();
        ListNode n3 = new UnaryMinusListNode();
        ListNode n4 = new NumberListNode(3.0);
        ListNode n5 = new MultiplicationListNode();
        ListNode n6 = new NumberListNode(1.0);
        ListNode n7 = new NumberListNode(4.0);
        ListNode n8 = new NumberListNode(2.0);
        ListNode n9 = new DivisionListNode();
        ListNode n10 = new AdditionListNode();
        ListNode n11 = new SubtractionListNode();
        n1.setNext(n2);
        n2.setNext(n3);
        n3.setNext(n4);
        n4.setNext(n5);
        n5.setNext(n6);
        n6.setNext(n7);
        n7.setNext(n8);
        n8.setNext(n9);
        n9.setNext(n10);
        n10.setNext(n11);

        PrintListVisitor pv1 = new PrintListVisitor();
        n1.accept(pv1);
        assertEquals("5.0 ~ ~ 3.0 * 1.0 4.0 2.0 / + -", pv1.getResult());

    }
}
