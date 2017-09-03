package cs4321.project1;

import cs4321.project1.Utilities.Pair;
import cs4321.project1.list.*;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provide a comment about what your class does and the overall logic
 *
 * @author Your names and netids go here
 */

public class EvaluatePrefixListVisitor implements ListVisitor {
	private Deque<Pair<Object, Integer>> operatorStack;
	private Deque<Double> numberStack;

	public EvaluatePrefixListVisitor() {
		// TODO fill me in
		this.operatorStack = new ArrayDeque<>();
		this.numberStack = new ArrayDeque<>();
	}

	public double getResult() {
		// TODO fill me in
		return this.numberStack.pop();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void visit(NumberListNode node) {
		// TODO fill me in
		this.numberStack.push(node.getData());
        this.decrementTopOfStack();

        while (!operatorStack.isEmpty() && operatorStack.peek().right == 0) {
            Pair<Object, Integer> topOfStack = operatorStack.pop();

            if (topOfStack.left instanceof Function) {
                Function<Double, Double> callable = (Function<Double, Double>) topOfStack.left;

                numberStack.push(callable.apply(
                        this.numberStack.pop()
                ));
            } else if (topOfStack.left instanceof BiFunction) {
                BiFunction<Double, Double, Double> callable = (BiFunction<Double, Double, Double>) topOfStack.left;

                double rhs = this.numberStack.pop();
                double lhs = this.numberStack.pop();

                numberStack.push(callable.apply(
                        lhs,
                        rhs
                ));
            }

            this.decrementTopOfStack();
        }

		node.visitNextIfNotNull(this);
	}

    private void decrementTopOfStack() {
        if (this.operatorStack.size() > 0) {
            Pair<Object, Integer> topOfStack;

            topOfStack = this.operatorStack.pop();
            topOfStack = new Pair<>(topOfStack.left, topOfStack.right - 1);

            this.operatorStack.push(topOfStack);
        }
    }

    @Override
	public void visit(AdditionListNode node) {
		// TODO fill me in
		BiFunction<Double, Double, Double> function = (a, b) -> a + b;
        this.operatorStack.push(new Pair<>(function, 2));
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(SubtractionListNode node) {
		// TODO fill me in
		BiFunction<Double, Double, Double> function = (a, b) -> a - b;
        this.operatorStack.push(new Pair<>(function, 2));
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(MultiplicationListNode node) {
		// TODO fill me in
		BiFunction<Double, Double, Double> function = (a, b) -> a * b;
        this.operatorStack.push(new Pair<>(function, 2));
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(DivisionListNode node) {
		// TODO fill me in
		BiFunction<Double, Double, Double> function = (a, b) -> a / b;
        this.operatorStack.push(new Pair<>(function, 2));
		node.visitNextIfNotNull(this);
	}

	@Override
	public void visit(UnaryMinusListNode node) {
		// TODO fill me in
		Function<Double, Double> function = (a) -> -a;
		this.operatorStack.push(new Pair<>(function, 1));
		node.visitNextIfNotNull(this);
	}
}
