package cs4321.project1.utilities;

import cs4321.project1.ListVisitor;
import cs4321.project1.list.ListNode;

/**
 * Created by nishadmathur on 7/9/17.
 */
public class Helpers {
    public static void visitNextIfNotNull(ListNode listNode, ListVisitor visitor) {
        if (listNode.getNext() != null) {
            listNode.getNext().accept(visitor);
        }
    }
}
