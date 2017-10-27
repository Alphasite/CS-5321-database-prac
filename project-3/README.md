# CS-5321-database-prac

## Project 3

The packages/classes of interest are:
 - Top level class: db.Project3
 - Physical operators: db.operators.physical 
 - Logical operators: db.operators.logical 
 - Builder: db.query.visitors.PhysicalPlanBuilder

Many parametrized tests are also provided in the test folder to verify the behavior of our 
implementation.
 
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
 
### Install notes

On some occasions the project will not build after importing it in eclipse, removing and readding
the execution JRE solves the issue.
