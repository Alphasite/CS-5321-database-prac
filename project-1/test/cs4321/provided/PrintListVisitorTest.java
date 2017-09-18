package cs4321.provided;

import cs4321.project1.PrintListVisitor;
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
	
	@Test
	public void testMultiplicationSimplePrefix() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new MultiplicationListNode();
		n3.setNext(n2);
		n2.setNext(n1);
		PrintListVisitor pv1 = new PrintListVisitor();
		n3.accept(pv1);
		assertEquals("* 2.0 1.0", pv1.getResult());
	}
	
	@Test
	public void testMultiplicationSimplePostfix() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new MultiplicationListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		PrintListVisitor pv1 = new PrintListVisitor();
		n1.accept(pv1);
		assertEquals("1.0 2.0 *", pv1.getResult());
	}
	@Test
	public void testSubtractionSimplePrefix() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new SubtractionListNode();
		n3.setNext(n2);
		n2.setNext(n1);
		PrintListVisitor pv1 = new PrintListVisitor();
		n3.accept(pv1);
		assertEquals("- 2.0 1.0", pv1.getResult());
	}
	
	@Test
	public void testSubtractionSimplePostfix() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new SubtractionListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		PrintListVisitor pv1 = new PrintListVisitor();
		n1.accept(pv1);
		assertEquals("1.0 2.0 -", pv1.getResult());
	}

	@Test
	public void testDivisionSimplePrefix() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(4.0);
		ListNode n3 = new DivisionListNode();
		n3.setNext(n2);
		n2.setNext(n1);
		PrintListVisitor pv1 = new PrintListVisitor();
		n3.accept(pv1);
		assertEquals("/ 4.0 1.0", pv1.getResult());
	}
	
	@Test
	public void testDivisionSimplePostfix() {
		ListNode n1 = new NumberListNode(4.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new DivisionListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		PrintListVisitor pv1 = new PrintListVisitor();
		n1.accept(pv1);
		assertEquals("4.0 2.0 /", pv1.getResult());
	}

	@Test
	public void testComplexExpressionList(){
		ListNode n1 = new NumberListNode(12.0); 
		ListNode n2 = new NumberListNode(2.0); 
		ListNode n3 = new NumberListNode(3.0); 
		ListNode n4 = new UnaryMinusListNode();
		ListNode n5 = new AdditionListNode();
		ListNode n6 = new MultiplicationListNode();
		ListNode n7 = new DivisionListNode();
		ListNode n8 = new NumberListNode(5.0);
		n8.setNext(n1);
		n1.setNext(n4);
		n4.setNext(n2);
		n2.setNext(n3);
		n3.setNext(n6);
		n6.setNext(n7);
		n7.setNext(n5);
        PrintListVisitor pv1 = new PrintListVisitor(); 
        n8.accept(pv1);
		assertEquals("5.0 12.0 ~ 2.0 3.0 * / +", pv1.getResult());
	}

}
