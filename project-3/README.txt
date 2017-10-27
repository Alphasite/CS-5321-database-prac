CS 5321 - Project 3

Top-level class: Project3.java

Logic for Partition Reset during SMJ:
    Our SMJ operator resets back to a particular tuple by calling the
    seek(index) method of the right child sort operator. Our external
    sort operator seeks by using the seek method of its underlying
    BinaryTupleReader class, which calculates the target page from
    the index, and directly loads that page (and only that page) into
    memory.
    Since our SMJ and external sort operators are not saving any tuples
    in memory for the purposes of resetting back to an index, SMJ does
    not keep unbounded state.

Logic for Handling DISTINCT:
    Our distinct operator uses a sorting approach, so since our
    external sort operator does not keep unbounded state, neither
    does our distinct operator.

No known bugs.
