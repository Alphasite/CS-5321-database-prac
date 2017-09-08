package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.utilities.Helpers;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Evaluates an expression list which is in postfix notation style.
 *
 * It recursively proceeds through the list
 *   The system adds any number it encounters to the number stack
 *   When it encounters an operator
 *     it pops of the appropriate number of numbers
 *   	 it evaluates the arguments
 *   	 it places the result onto the stack
 * When the end of the list is reached only the result should be on the stack.
 * 
 * @author Your names and netids go here
 */
public class EvaluatePostfixListVisitor implements ListVisitor {
	private Deque<Double> numberStack;

	public EvaluatePostfixListVisitor() {
		this.numberStack = new ArrayDeque<>();
	}

	/**
	 * Get the result of the computation.
	 * Pop it off the stack.
	 *
	 * @return the result of the computation.
	 */
	public double getResult() {
		return numberStack.pop();
	}

	/**
	 * Visit method for number node
	 * just store the numeric value on the stack and then visit the next node.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(NumberListNode node) {
		this.numberStack.push(node.getData());
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for + node.
	 * Pops off the two operands from the stack (right then left)
	 * Then applies the operator to the 2 operands, storing the result on the stack.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(AdditionListNode node) {
		double rhs = this.numberStack.pop();
		double lhs = this.numberStack.pop();
		this.numberStack.push(lhs + rhs);
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for - node.
	 * Pops off the two operands from the stack (right then left)
	 * Then applies the operator to the 2 operands, storing the result on the stack.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SubtractionListNode node) {
		double rhs = this.numberStack.pop();
		double lhs = this.numberStack.pop();
		this.numberStack.push(lhs - rhs);
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for * node.
	 * Pops off the two operands from the stack (right then left)
	 * Then applies the operator to the 2 operands, storing the result on the stack.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(MultiplicationListNode node) {
		double rhs = this.numberStack.pop();
		double lhs = this.numberStack.pop();
		this.numberStack.push(lhs * rhs);
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for / node.
	 * Pops off the two operands from the stack (right then left)
	 * Then applies the operator to the 2 operands, storing the result on the stack.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(DivisionListNode node) {
		double rhs = this.numberStack.pop();
		double lhs = this.numberStack.pop();
		this.numberStack.push(lhs / rhs);
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for unary - node.
	 * Pops off the operand from the stack
	 * Then applies the operator to the operand, storing the result on the stack.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(UnaryMinusListNode node) {
		double number = this.numberStack.pop();
		this.numberStack.push(-number);
		Helpers.visitNextIfNotNull(node, this);
	}
}
