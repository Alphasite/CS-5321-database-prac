package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.tree.*;

/**
 * This converts the AST into a postfix list.
 * Which is to say that it is a list where operators follow after their operands.
 *
 * This approach just keeps track of the head and the tail of the list, adding nodes
 * to the tail, using post-order traversal.
 * 
 * @author Nishad Mathur (nm594), Antoine Klopocki (ajk332), Antoine Salon (ajs672)
 */
public class BuildPostfixExpressionTreeVisitor implements TreeVisitor {
	ListNode head;
	ListNode tail;

	public BuildPostfixExpressionTreeVisitor() {
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
	 * Visit method for unary minus node.
	 * Adds any child expression before adding the operator.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(UnaryMinusTreeNode node) {
		node.getChild().accept(this);
		this.appendNode(new UnaryMinusListNode());
	}

	/**
	 * Visit method for + node.
	 * Adds any child expressions before adding the operator.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(AdditionTreeNode node) {
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
		this.appendNode(new AdditionListNode());
	}

	/**
	 * Visit method for * node.
	 * Adds any child expressions before adding the operator.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(MultiplicationTreeNode node) {
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
		this.appendNode(new MultiplicationListNode());
	}

	/**
	 * Visit method for - node.
	 * Adds any child expressions before adding the operator.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(SubtractionTreeNode node) {
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
		this.appendNode(new SubtractionListNode());
	}

	/**
	 * Visit method for / node.
	 * Adds any child expressions before adding the operator.
	 *
	 * @param node
	 *            the node to be visited
	 */
	@Override
	public void visit(DivisionTreeNode node) {
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
		this.appendNode(new DivisionListNode());
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
