# CS-5321-database-prac

## Project 2

The packages/classes of interest are:
 - Top level class: db.Project3
 - Physical operators: db.operators.physical 
 - Logical operators: db.operators.logical 
 - Builder: db.query.visitors.PhysicalPlanBuilder

Many tests are also provided in the test folder to verify the behavior of our
operator classes and query builder.

The query builder and WHERE expression decomposer can both be found in the db.query package,
 they are well commented and detail the logic that we use to dispatch expression
 evaluation down the operator tree.
