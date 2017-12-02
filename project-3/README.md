# CS-5321-database-prac

## Project 5

The top level class is _db.Project3_. It reads the configuration file and builds
indexes or evaluates query if requested.
Many parametrized tests are also provided in the test folder to verify the behavior of our 
implementation.

### Optimization algorithms

**Selection pushing** : _QueryBuilder.processWhereClause()_

**Selection implementation choice** : _PhysicalPlanBuilder_

**Join ordering** : _JoinOrderOptimizer_

This implementation follows the provided guidelines : it checks all possible
join orders and evaluates their respective cost, retaining the best one
for comparison. Join cost evaluation is delegated to the JoinPlan class using
VValues.
Note that this implementation uses recursion to generate and evaluate
join orders, but since we do it from the bottom specific subjoin plans
are only computed once which makes this as efficient as dynamic programming
with lower memory usage.


**Join implementation choice** : _JoinPlan.getJoinTypesAndFlips_

The system uses the formulae provided in class to estimate the costs of performing 
each of the different join types (in reality these number would need further tuning
and better stats to be fully accurate, but its a somewhat reasonable approach). 

So SMJ cost:
 - Pages = (sort(L) + sort(S)) + scan(L) + scan(S)
 - Number of passes = ceil(log_(b-1)ceil(n/b))+1
 - Cost of each pass = 2N
 - Pages = number of passes * cost of pass 
 
And BNLJ:
 - Block size = B - 2
 - R blocks = ceil(M / block size)
 - Pages = M + R blocks * S
 
It then selects the cheaper of the two algorithms. 

Also, as request (as i understand it) we decide which operator is inner and which 
is outer here. Outputting a boolean to indicate whether or not to flip the current 
ordering.   

No known bugs

## Project 4
 
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
  are directly above scan operators. Join filter conditions are emitted as early 
  as possible as well, emitting when all require fields are present in the tuple. 
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
