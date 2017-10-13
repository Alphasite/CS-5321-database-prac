package db.operators;

public interface BinaryNode<T> {

    /**
     * Get the left child of the node
     *
     * @return Left node
     */
    T getLeft();

    /**
     * Get the right child of the node
     *
     * @return Right child
     */
    T getRight();
}
