# CS-5321-database-prac

## Project 4

The packages/classes of interest are:
 - Top level class: db.Project3
 - Physical operators: db.operators.physical 
 - Logical operators: db.operators.logical 
 - Builder: db.query.visitors.PhysicalPlanBuilder

Many parametrized tests are also provided in the test folder to verify the behavior of our 
implementation.
 
Logic for Index Scan Operator:
- lowkey and highkey are set in the IndexScanOperator class (this interval is closed)
- Different handling of clustered vs. unclustered indexes can be seen in IndexScanOperator
  (which reads the tuples differently depending on type of index), BulkLoader (which
  creates the indexes), and PhysicalPlanBuilder (which inserts IndexScanOperators into
  the physical plan when appropriate).
- The root-to-leaf tree descent is part of the tree data structure code. The entry point is
  BTree.search(key) that begins at the root and goes down by finding the corresponding child
  entry, then retrieving the node by directly reading the corresponding page. Thus only
  nodes along the search path are deserialized which means each index search requires
  around 3 I/Os (based on typical tree order values as seen in class).


Logic for separating out selection handled via the index:
- Previous to project 4, we already created logic that pushed selection operators
  down the tree. Thus, it is already certain that any logical selection operators
  are directly above scan operators.
- Going by this assumption, when our PhysicalPlanBuilder visits a
  LogicalSelectOperator, we check if any of the selection expressions can be handled
  by an index scan. If not, we proceed the same way as in project 3. If so, we
  replace the base ScanOperator with an IndexScanOperator and add a physical
  SelectionOperator on top of the IndexScanOperator to handle any leftover
  expressions that cannot be handled by the IndexScan.
- This detail was skimmed over in the previous bullet point, but we use a new
  expression visitor called IndexScanEvaluator to decide whether selection
  operator's expressions can be optimized by an IndexScan. This expression
  visitor looks at every expression joined by an AndExpression and if it both
  a) involves the indexed column and b) involves no other columns, it is an
  expression that can be optimized by an IndexScan.


No known bugs.
 
### Install notes

On some occasions the project will not build after importing it in eclipse, removing and readding
the execution JRE solves the issue.
