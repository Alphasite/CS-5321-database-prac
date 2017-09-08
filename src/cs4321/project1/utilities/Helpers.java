package cs4321.project1.utilities;

import cs4321.project1.ListVisitor;
import cs4321.project1.list.ListNode;

/**
 * A collection of helper utilities for the system.
 *
 * @author Nishad Mathur (nm594)
 */
public class Helpers {
    /**
     * A utility to visit the next list node if it exists.
     *
     * Ideally this would be a method on the node class/interface, but we aren't allowed to change that.
     *
     * @param listNode The node who's next sibling we will visit.
     * @param visitor The visitor which will be applied to the node.
     */
    public static void visitNextIfNotNull(ListNode listNode, ListVisitor visitor) {
        if (listNode.getNext() != null) {
            listNode.getNext().accept(visitor);
        }
    }
}
