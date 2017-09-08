package cs4321.project1;

import cs4321.project1.tree.DivisionTreeNode;
import cs4321.project1.tree.LeafTreeNode;
import cs4321.project1.tree.SubtractionTreeNode;
import cs4321.project1.tree.AdditionTreeNode;
import cs4321.project1.tree.MultiplicationTreeNode;
import cs4321.project1.tree.UnaryMinusTreeNode;

/**
 * Evaluate an expression tree for a value.
 *
 * The expression recursively evaluates the children of each node, placing its
 * computed value into the result variable (effectively a return register)
 * before then evaluating the node its self.
 * 
 * @author Nishad Mathur (nm594), Antoine Klopocki (ajk332), Antoine Salom (ajs672)
 */
public class EvaluateTreeVisitor implements TreeVisitor {
	private double result;

	public EvaluateTreeVisitor() {
		this.result = 0;
	}

	public double getResult() {
		return this.result;
	}

	/**
	 * Visit method for leaf node
	 * just store the integer value into the result var.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LeafTreeNode node) {
		this.result = node.getData();
	}

	/**
	 * Visit method for unary minus node.
	 * Recursively computes the value of its child node
	 * Then applies the negation and stores the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(UnaryMinusTreeNode node) {
		node.getChild().accept(this);
		this.result = -this.result;
	}

	/**
	 * Visit method for + node.
	 * Recursively computes the values of the child expressions
	 * Then applies the operator and stores the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(AdditionTreeNode node) {
		node.getLeftChild().accept(this);
		double lhs = this.result;
		node.getRightChild().accept(this);
		double rhs = this.result;

		this.result = lhs + rhs;
	}

	/**
	 * Visit method for * node.
	 * Recursively computes the values of the child expressions
	 * Then applies the operator and stores the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(MultiplicationTreeNode node) {
		node.getLeftChild().accept(this);
		double lhs = this.result;
		node.getRightChild().accept(this);
		double rhs = this.result;

		this.result = lhs * rhs;
	}

	/**
	 * Visit method for - node.
	 * Recursively computes the values of the child expressions
	 * Then applies the operator and stores the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SubtractionTreeNode node) {
		node.getLeftChild().accept(this);
		double lhs = this.result;
		node.getRightChild().accept(this);
		double rhs = this.result;

		this.result = lhs - rhs;
	}

	/**
	 * Visit method for / node.
	 * Recursively computes the values of the child expressions
	 * Then applies the operator and stores the result.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(DivisionTreeNode node) {
		node.getLeftChild().accept(this);
		double lhs = this.result;
		node.getRightChild().accept(this);
		double rhs = this.result;

		this.result = lhs / rhs;
	}
}
