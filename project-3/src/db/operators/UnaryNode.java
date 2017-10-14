package db.operators;

public interface UnaryNode<T> {

    /**
     * Return the single child of this node.
     *
     * @return The child node.
     */
    T getChild();

}
