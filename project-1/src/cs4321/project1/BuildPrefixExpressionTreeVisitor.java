package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.tree.*;

/**
 * This converts the AST into a prefix list.
 * Which is to say that it is a list where operators preceed their operands
 * (e.g. a rather lisp like '(- 1 2)' ).
 *
 * This approach just keeps track of the head and the tail of the list, adding nodes
 * to the tail, using pre-order traversal.
 *
 * @author Nishad Mathur (nm594), Antoine Klopocki (ajk332), Antoine Salon (ajs672)
 */
public class BuildPrefixExpressionTreeVisitor implements TreeVisitor {
	ListNode head;
	ListNode tail;

	public BuildPrefixExpressionTreeVisitor() {
		this.head = null;
		this.tail = null;
	}

	/**
	 * Get the resulting node from applying the visitor to the tree.
	 *
	 * @return the head node of the expression.
	 */
	public ListNode getResult() {
		return head;
	}

	/**
	 * Visit method for leaf node; just add the numeric value to the expression.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(LeafTreeNode node) {
		this.appendNode(new NumberListNode(node.getData()));
	}

	/**
	 * Visit method for unary - node.
	 * Adds the operator before adding the child expression.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(UnaryMinusTreeNode node) {
		this.appendNode(new UnaryMinusListNode());
		node.getChild().accept(this);
	}

	/**
	 * Visit method for + node.
	 * Adds the operator before adding the child expressions.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(AdditionTreeNode node) {
		this.appendNode(new AdditionListNode());
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
	}

	/**
	 * Visit method for * node.
	 * Adds the operator before adding the child expressions.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(MultiplicationTreeNode node) {
		this.appendNode(new MultiplicationListNode());
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
	}

	/**
	 * Visit method for - node.
	 * Adds the operator before adding the child expressions.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SubtractionTreeNode node) {
		this.appendNode(new SubtractionListNode());
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
	}

	/**
	 * Visit method for / node.
	 * Adds the operator before adding the child expressions.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(DivisionTreeNode node) {
		this.appendNode(new DivisionListNode());
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
	}

	/**
	 * This is a helper method for appending a node to the tail of the list.
	 *
	 * @param node The node to append to the list.
	 */
	private void appendNode(ListNode node) {
		if (head == null) {
			head = node;
			tail = node;
		} else {
			tail.setNext(node);
			tail = node;
		}
	}
}
