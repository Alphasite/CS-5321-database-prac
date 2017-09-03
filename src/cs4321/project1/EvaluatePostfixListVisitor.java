package cs4321.project1;

import cs4321.project1.list.*;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Provide a comment about what your class does and the overall logic
 * 
 * @author Your names and netids go here
 */
public class EvaluatePostfixListVisitor implements ListVisitor {
	private Deque<Double> numberStack;

	public EvaluatePostfixListVisitor() {
		// TODO fill me in
		this.numberStack = new ArrayDeque<>();
	}

	public double getResult() {
		// TODO fill me in
		return numberStack.pop();
	}

	@Override
	public void visit(NumberListNode node) {
		// TODO fill me in
		this.numberStack.push(node.getData());
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(AdditionListNode node) {
		// TODO fill me in
		double rhs = this.numberStack.pop();
		double lhs = this.numberStack.pop();
		this.numberStack.push(lhs + rhs);
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(SubtractionListNode node) {
		// TODO fill me in
		double rhs = this.numberStack.pop();
		double lhs = this.numberStack.pop();
		this.numberStack.push(lhs - rhs);
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(MultiplicationListNode node) {
		// TODO fill me in
		double rhs = this.numberStack.pop();
		double lhs = this.numberStack.pop();
		this.numberStack.push(lhs * rhs);
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(DivisionListNode node) {
		// TODO fill me in
		double rhs = this.numberStack.pop();
		double lhs = this.numberStack.pop();
		this.numberStack.push(lhs / rhs);
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(UnaryMinusListNode node) {
		// TODO fill me in
		double number = this.numberStack.pop();
		this.numberStack.push(-number);
		node.visitNextIfNotNull(this);
	}
}
