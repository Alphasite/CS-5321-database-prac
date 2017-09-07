package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.utilities.Helpers;

/**
 * Provide a comment about what your class does and the overall logic
 *
 * @author Your names and netids go here
 */

public class EvaluatePrefixListVisitor implements ListVisitor {
	private ListNode last;
	private Double value;

	public EvaluatePrefixListVisitor() {
		// TODO fill me in
		this.last = null;
		this.value = 0.0;
	}

	public double getResult() {
		// TODO fill me in
		return this.value;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void visit(NumberListNode node) {
		// TODO fill me in
		this.value = node.getData();
		this.last = node;
	}

    @Override
	public void visit(AdditionListNode node) {
		// TODO fill me in
		Helpers.visitNextIfNotNull(node, this);
		double lhs = this.value;

		Helpers.visitNextIfNotNull(last, this);
		double rhs = this.value;

		this.value = lhs + rhs;
	}

	@Override
	public void visit(SubtractionListNode node) {
		// TODO fill me in
		Helpers.visitNextIfNotNull(node, this);
		double lhs = this.value;

		Helpers.visitNextIfNotNull(last, this);
		double rhs = this.value;

		this.value = lhs - rhs;
	}

	@Override
	public void visit(MultiplicationListNode node) {
		// TODO fill me in
		Helpers.visitNextIfNotNull(node, this);
		double lhs = this.value;

		Helpers.visitNextIfNotNull(last, this);
		double rhs = this.value;

		this.value = lhs * rhs;
	}

	@Override
	public void visit(DivisionListNode node) {
		// TODO fill me in
		Helpers.visitNextIfNotNull(node, this);
		double lhs = this.value;

		Helpers.visitNextIfNotNull(last, this);
		double rhs = this.value;

		this.value = lhs / rhs;
	}

	@Override
	public void visit(UnaryMinusListNode node) {
		// TODO fill me in
		Helpers.visitNextIfNotNull(node, this);
		this.value = -this.value;
	}
}
