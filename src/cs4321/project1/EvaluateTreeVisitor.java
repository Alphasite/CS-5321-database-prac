package cs4321.project1;

import cs4321.project1.tree.DivisionTreeNode;
import cs4321.project1.tree.LeafTreeNode;
import cs4321.project1.tree.SubtractionTreeNode;
import cs4321.project1.tree.AdditionTreeNode;
import cs4321.project1.tree.MultiplicationTreeNode;
import cs4321.project1.tree.UnaryMinusTreeNode;

/**
 * Provide a comment about what your class does and the overall logic
 * 
 * @author Your names and netids go here
 */

public class EvaluateTreeVisitor implements TreeVisitor {
	double result;

	public EvaluateTreeVisitor() {
		// TODO fill me in
	}

	public double getResult() {
		// TODO fill me in
		return this.result;
	}

	@Override
	public void visit(LeafTreeNode node) {
		// TODO fill me in
		this.result = node.getData();
	}

	@Override
	public void visit(UnaryMinusTreeNode node) {
		// TODO fill me in
		node.getChild().accept(this);
		this.result = -this.result;
	}

	@Override
	public void visit(AdditionTreeNode node) {
		// TODO fill me in
		node.getLeftChild().accept(this);
		double lhs = this.result;
		node.getRightChild().accept(this);
		double rhs = this.result;

		this.result = lhs + rhs;
	}

	@Override
	public void visit(MultiplicationTreeNode node) {
		// TODO fill me in
		node.getLeftChild().accept(this);
		double lhs = this.result;
		node.getRightChild().accept(this);
		double rhs = this.result;

		this.result = lhs * rhs;
	}

	@Override
	public void visit(SubtractionTreeNode node) {
		// TODO fill me in
		node.getLeftChild().accept(this);
		double lhs = this.result;
		node.getRightChild().accept(this);
		double rhs = this.result;

		this.result = lhs - rhs;
	}

	@Override
	public void visit(DivisionTreeNode node) {
		// TODO fill me in
		node.getLeftChild().accept(this);
		double lhs = this.result;
		node.getRightChild().accept(this);
		double rhs = this.result;

		this.result = lhs / rhs;
	}
}
