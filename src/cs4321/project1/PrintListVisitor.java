package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.utilities.Helpers;

import java.util.ArrayList;
import java.util.List;

/**
 * A visitor to pretty print a list expression.
 * 
 * @author Nishad Mathur (nm594), Antoine Klopocki (ajk332), Antoine Salom (ajs672)
 */
public class PrintListVisitor implements ListVisitor {
	List<String> result;

	public PrintListVisitor() {
		this.result = new ArrayList<>();
	}

	/**
	 * Method to get the finished string representation when visitor is done
	 *
	 * @return string representation of the visited tree
	 */
	public String getResult() {
		return String.join(" ", this.result);
	}

	/**
	 * Visit method for a number node; just concatenates the numeric value to the
	 * running string
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(NumberListNode node) {
		this.result.add(((Double) node.getData()).toString());
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for addition node; just concatenates the '+' operator to the
	 * running string
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(AdditionListNode node) {
		this.result.add("+");
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for addition node; just concatenates the '-' operator to the
	 * running string
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SubtractionListNode node) {
		this.result.add("-");
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for addition node; just concatenates the '*' operator to the
	 * running string
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(MultiplicationListNode node) {
		this.result.add("*");
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for addition node; just concatenates the '/' operator to the
	 * running string
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(DivisionListNode node) {
		this.result.add("/");
		Helpers.visitNextIfNotNull(node, this);
	}

	/**
	 * Visit method for addition node; just concatenates the '~' operator to the
	 * running string
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(UnaryMinusListNode node) {
		this.result.add("~");
		Helpers.visitNextIfNotNull(node, this);
	}
}
