package db.operators;

import java.util.List;

public interface NaryNode<T> {

    /**
     * Return the children of this node.
     *
     * @return The children of the node.
     */
    List<T> getChildren();
}
