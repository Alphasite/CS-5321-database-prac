package cs4321.provided;

import cs4321.project1.EvaluatePostfixListVisitor;
import cs4321.project1.list.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EvaluatePostfixListVisitorTest {

	private static final double DELTA = 1e-15;

	@Test
	public void testSingleNumberNode() {
		ListNode n1 = new NumberListNode(1.0);
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(1.0, v1.getResult(), DELTA);
	}
	
	@Test
	public void testAdditionSimple() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new AdditionListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(3.0, v1.getResult(), DELTA);
		
		ListNode n4 = new NumberListNode(1.0);
		ListNode n5 = new NumberListNode(2.0);
		ListNode n6 = new AdditionListNode();
		n5.setNext(n4);
		n4.setNext(n6);
		EvaluatePostfixListVisitor v2 = new EvaluatePostfixListVisitor();
		n5.accept(v2);
		assertEquals(3.0, v2.getResult(), DELTA);
	}
	
	@Test
	public void testAdditionMultipleInstances() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new AdditionListNode();
		ListNode n4 = new NumberListNode(3.0);
		ListNode n5 = new AdditionListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		n3.setNext(n4);
		n4.setNext(n5); //expression is 1 2 + 3 + 
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(6.0, v1.getResult(), DELTA);
	}
	
	@Test
	public void testMultiplicationSimple() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new MultiplicationListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(2.0, v1.getResult(), DELTA);
		
		ListNode n4 = new NumberListNode(1.0);
		ListNode n5 = new NumberListNode(2.0);
		ListNode n6 = new MultiplicationListNode();
		n5.setNext(n4);
		n4.setNext(n6);
		EvaluatePostfixListVisitor v2 = new EvaluatePostfixListVisitor();
		n5.accept(v2);
		assertEquals(2.0, v2.getResult(), DELTA);
	}
	
	@Test
	public void testMultiplicationMultipleInstances() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new MultiplicationListNode();
		ListNode n4 = new NumberListNode(3.0);
		ListNode n5 = new MultiplicationListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		n3.setNext(n4);
		n4.setNext(n5); //expression is 1 2 * 3 * 
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(6.0, v1.getResult(), DELTA);
	}
	
	@Test
	public void testSubtractionSimple() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new SubtractionListNode();
		n1.setNext(n2);
		n2.setNext(n3);		
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(-1.0, v1.getResult(), DELTA);

		
		ListNode n4 = new NumberListNode(1.0);
		ListNode n5 = new NumberListNode(2.0);
		ListNode n6 = new SubtractionListNode();
		n5.setNext(n4);
		n4.setNext(n6);
		EvaluatePostfixListVisitor v2 = new EvaluatePostfixListVisitor();
		n5.accept(v2);
		assertEquals(1.0, v2.getResult(), DELTA);
	}

	@Test
	public void testSubtractionMultipleInstances() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new SubtractionListNode();
		ListNode n4 = new NumberListNode(3.0);
		ListNode n5 = new SubtractionListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		n3.setNext(n4);
		n4.setNext(n5); //expression is 1 2 - 3 - 
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(-4.0, v1.getResult(), DELTA);
	}
	
	@Test
	public void testDivisionSimple() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new DivisionListNode();
		n1.setNext(n2);
		n2.setNext(n3);		
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(0.5, v1.getResult(), DELTA);

		
		ListNode n4 = new NumberListNode(1.0);
		ListNode n5 = new NumberListNode(2.0);
		ListNode n6 = new DivisionListNode();
		n5.setNext(n4);
		n4.setNext(n6);
		EvaluatePostfixListVisitor v2 = new EvaluatePostfixListVisitor();
		n5.accept(v2);
		assertEquals(2.0, v2.getResult(), DELTA);
	}

	@Test
	public void testDivisionMultipleInstances() {
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new NumberListNode(2.0);
		ListNode n3 = new DivisionListNode();
		ListNode n4 = new NumberListNode(3.0);
		ListNode n5 = new DivisionListNode();
		n1.setNext(n2);
		n2.setNext(n3);
		n3.setNext(n4);
		n4.setNext(n5); //expression is 1 2 / 3 / 
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(0.5/3.0, v1.getResult(), DELTA);
	}
	
	@Test
	public void testUnaryMinusNode() {
		
		ListNode n1 = new NumberListNode(1.0);
		ListNode n2 = new UnaryMinusListNode();
		n1.setNext(n2);
		
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n1.accept(v1);
		assertEquals(-1.0, v1.getResult(), DELTA);
		
		ListNode n3 = new NumberListNode(1.0);
		ListNode n4 = new UnaryMinusListNode();
		ListNode n5 = new UnaryMinusListNode();
		n3.setNext(n4);
		n4.setNext(n5);
		
		EvaluatePostfixListVisitor v2 = new EvaluatePostfixListVisitor();
		n3.accept(v2);
		assertEquals(1.0, v2.getResult(), DELTA);
	}
	
	@Test
	public void testComplexExpression(){
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
		//expression is 5 12 ~ 2 3 * / +
		EvaluatePostfixListVisitor v1 = new EvaluatePostfixListVisitor();
		n8.accept(v1);
		assertEquals(3.0, v1.getResult(), DELTA);
	}

}
