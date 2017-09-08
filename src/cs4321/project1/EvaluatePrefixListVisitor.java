package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.utilities.Helpers;

/**
 * Evaluate an expression which is represented as an prefix list.
 *
 * It evaluates its by treating the list as a tree,
 * 	 it recursively evaluates each child of the expression until a number is encountered
 * 	   the left hand sub expression is evaluated as the next node of the current node
 * 	   the right hand sub expression is evalated as the next node of the lastNodeEvaluated node
 * 	 on encountering a number
 * 	   it stores that number in the result var
 * 	   it stores the number node in the lastNodeEvaluated var
 * 	   it returns
 * 	 when the sub expressions are evaluated
 * 	   it applies the operator and stores result
 * 	 it does this until the first node has returned and a value is stored in the result variable.
 *
 * @author Your names and netids go here
 */
public class EvaluatePrefixListVisitor implements ListVisitor {
	private ListNode lastNodeEvaluated;
	private Double result;

	public EvaluatePrefixListVisitor() {
		this.lastNodeEvaluated = null;
		this.result = 0.0;
	}

	/**
	 * Get the result of the computation.
	 *
	 * @return the result of the computation.
	 */
	public double getResult() {
		return this.result;
	}

	/**
	 * Visit method for number node
	 * just store the numeric value in the result and set the lastNodeEvaluated.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(NumberListNode node) {
		this.result = node.getData();
		this.lastNodeEvaluated = node;
	}

	/**
	 * Visit method for + node.
	 * Evaluates the left sub expression and stores its result
	 * then evaluates the right hand sub expression, carrying on from where it finished last.
	 * Then applies the operator to the result of the 2 sub expressions, storing the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
    @Override
	public void visit(AdditionListNode node) {
		Helpers.visitNextIfNotNull(node, this);
		double lhs = this.result;

		Helpers.visitNextIfNotNull(lastNodeEvaluated, this);
		double rhs = this.result;

		this.result = lhs + rhs;
	}

	/**
	 * Visit method for - node.
	 * Evaluates the left sub expression and stores its result
	 * then evaluates the right hand sub expression, carrying on from where it finished last.
	 * Then applies the operator to the result of the 2 sub expressions, storing the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SubtractionListNode node) {
		Helpers.visitNextIfNotNull(node, this);
		double lhs = this.result;

		Helpers.visitNextIfNotNull(lastNodeEvaluated, this);
		double rhs = this.result;

		this.result = lhs - rhs;
	}

	/**
	 * Visit method for * node.
	 * Evaluates the left sub expression and stores its result
	 * then evaluates the right hand sub expression, carrying on from where it finished last.
	 * Then applies the operator to the result of the 2 sub expressions, storing the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(MultiplicationListNode node) {
		Helpers.visitNextIfNotNull(node, this);
		double lhs = this.result;

		Helpers.visitNextIfNotNull(lastNodeEvaluated, this);
		double rhs = this.result;

		this.result = lhs * rhs;
	}

	/**
	 * Visit method for / node.
	 * Evaluates the left sub expression and stores its result
	 * then evaluates the right hand sub expression, carrying on from where it finished last.
	 * Then applies the operator to the result of the 2 sub expressions, storing the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(DivisionListNode node) {
		Helpers.visitNextIfNotNull(node, this);
		double lhs = this.result;

		Helpers.visitNextIfNotNull(lastNodeEvaluated, this);
		double rhs = this.result;

		this.result = lhs / rhs;
	}

	/**
	 * Visit method for unary - node.
	 * Evaluates the sub expression
	 * Then stores negated value in the result variable.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(UnaryMinusListNode node) {
		Helpers.visitNextIfNotNull(node, this);
		this.result = -this.result;
	}
}
