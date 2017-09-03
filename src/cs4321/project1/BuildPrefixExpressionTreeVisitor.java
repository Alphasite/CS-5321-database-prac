package cs4321.project1;

import cs4321.project1.list.*;
import cs4321.project1.tree.*;

/**
 * Provide a comment about what your class does and the overall logic
 * 
 * @author Your names and netids go here
 */
public class BuildPrefixExpressionTreeVisitor implements TreeVisitor {
	ListNode head;
	ListNode tail;

	public BuildPrefixExpressionTreeVisitor() {
		// TODO fill me in
		this.head = null;
		this.tail = null;
	}

	public ListNode getResult() {
		// TODO fill me in
		return head;
	}

	@Override
	public void visit(LeafTreeNode node) {
		// TODO fill me in
		this.appendNode(new NumberListNode(node.getData()));
	}

	@Override
	public void visit(UnaryMinusTreeNode node) {
		// TODO fill me in
		this.appendNode(new UnaryMinusListNode());
		node.getChild().accept(this);
	}

	@Override
	public void visit(AdditionTreeNode node) {
		// TODO fill me in
		this.appendNode(new AdditionListNode());
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
	}

	@Override
	public void visit(MultiplicationTreeNode node) {
		// TODO fill me in
		this.appendNode(new MultiplicationListNode());
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
	}

	@Override
	public void visit(SubtractionTreeNode node) {
		// TODO fill me in
		this.appendNode(new SubtractionListNode());
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
	}

	@Override
	public void visit(DivisionTreeNode node) {
		// TODO fill me in
		this.appendNode(new DivisionListNode());
		node.getLeftChild().accept(this);
		node.getRightChild().accept(this);
	}

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
