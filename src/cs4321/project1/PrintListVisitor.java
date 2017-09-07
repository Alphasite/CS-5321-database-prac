package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.utilities.Helpers;

import java.util.ArrayList;
import java.util.List;

/**
 * Provide a comment about what your class does and the overall logic
 * 
 * @author Your names and netids go here
 */

public class PrintListVisitor implements ListVisitor {
	List<String> result;

	public PrintListVisitor() {
		// TODO fill me in
		this.result = new ArrayList<>();
	}

	public String getResult() {
		// TODO fill me in
		return String.join(" ", this.result);
	}

	@Override
	public void visit(NumberListNode node) {
		// TODO fill me in
		this.result.add(((Double) node.getData()).toString());
		Helpers.visitNextIfNotNull(node, this);
	}

	@Override
	public void visit(AdditionListNode node) {
		// TODO fill me in
		this.result.add("+");
		Helpers.visitNextIfNotNull(node, this);
	}

	@Override
	public void visit(SubtractionListNode node) {
		// TODO fill me in
		this.result.add("-");
		Helpers.visitNextIfNotNull(node, this);
	}

	@Override
	public void visit(MultiplicationListNode node) {
		// TODO fill me in
		this.result.add("*");
		Helpers.visitNextIfNotNull(node, this);
	}

	@Override
	public void visit(DivisionListNode node) {
		// TODO fill me in
		this.result.add("/");
		Helpers.visitNextIfNotNull(node, this);
	}

	@Override
	public void visit(UnaryMinusListNode node) {
		// TODO fill me in
		this.result.add("~");
		Helpers.visitNextIfNotNull(node, this);
	}
}
