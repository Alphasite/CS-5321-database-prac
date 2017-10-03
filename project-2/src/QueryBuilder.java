import datastore.Database;
import datastore.TableHeader;
import db.Utilities;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import operators.Operator;
import operators.bag.JoinOperator;
import operators.bag.ProjectionOperator;
import operators.bag.RenameOperator;
import operators.bag.SelectionOperator;
import operators.extended.DistinctOperator;
import operators.extended.SortOperator;
import operators.physical.ScanOperator;
import query.BreakWhereBuilder;
import query.TableCouple;

import java.util.*;

/**
 * TODO
 */
public class QueryBuilder {
    private Database db;

    public QueryBuilder(Database db) {
        this.db = db;
    }

    /**
     * Builds an optimized execution plan for the given query using a tree of operators
     * The results of the query can be computed by iterating over the resulting root operator
     *
     * @param query The parsed query object
     * @return The root operator of the query execution plan tree
     */
    public Operator buildQuery(PlainSelect query) {
        // TODO: later on optimize by breaking down SELECT into multiple OPs evaluated as early as possible

        // Store ref to all needed query tokens
        List<SelectItem> selectItems = query.getSelectItems();
        Table fromItem = (Table) query.getFromItem();
        List<Join> joinItems = query.getJoins();
        Expression whereItem = query.getWhere();
        List<OrderByElement> orderBy = query.getOrderByElements();
        boolean isDistinct = query.getDistinct() != null;

        // Keep reference to current root
        Operator rootNode;

        rootNode = processWhereClause(whereItem, joinItems, fromItem);

        // Projection
        if (!(selectItems.get(0) instanceof AllColumns)) {
            List<String> tableNames = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();

            for (SelectItem item : selectItems) {
                Column columnRef = (Column) ((SelectExpressionItem) item).getExpression();
                tableNames.add(columnRef.getTable().getName());
                columnNames.add(columnRef.getColumnName());
            }

            rootNode = new ProjectionOperator(new TableHeader(tableNames, columnNames), rootNode);
        }

        // The spec allows handling sorting and duplicate elimination after projection

        if (orderBy != null) {
            List<Column> orderByColumns = new ArrayList<>();
            for (OrderByElement element : orderBy) {
                orderByColumns.add((Column) element.getExpression());
            }

            TableHeader sortHeader = TableHeader.fromColumns(orderByColumns);
            rootNode = new SortOperator(rootNode, sortHeader);
        }

        if (isDistinct) {
            // Current implementation requires sorted queries
            if (orderBy == null) {
                // Sort all fields
                rootNode = new SortOperator(rootNode, rootNode.getHeader());
            }

            rootNode = new DistinctOperator(rootNode);
        }

        return rootNode;
    }

    /**
     * TODO
     * Collapse all tables referenced in FROM clause for easier handling
     *
     * @param fromItem
     * @param joins
     * @return
     */
    private List<Table> buildTableList(Table fromItem, List<Join> joins) {
        List<Table> list = new ArrayList<>();
        list.add(fromItem);

        if (joins != null) {
            for (Join join : joins) {
                list.add((Table) join.getRightItem());
            }
        }
        return list;
    }

    /**
     * TODO
     * Build the internal maps used to link every part of the WHERE clause to the table they reference
     *
     * @param rootExpression
     * @param joinItems
     * @param rootTable
     * @return
     */
    private Operator processWhereClause(Expression rootExpression, List<Join> joinItems, Table rootTable) {

        // First create the root node.
        Set<String> alreadyJoinedTables = new HashSet<>();
        String rootTabledentifier = Utilities.getIdentifier(rootTable);
        alreadyJoinedTables.add(rootTabledentifier);

        Operator rootNode = this.getScanAndMaybeRename(rootTable);

        // Then find which other tables exist.
        // then store them in a list of tables which haven't yet been joined.

        HashMap<String, Operator> tablesToBeJoined = new HashMap<>();
        if (joinItems != null) {
            for (Join join : joinItems) {
                Table table = (Table) join.getRightItem();
                tablesToBeJoined.put(Utilities.getIdentifier(table), this.getScanAndMaybeRename(table));
            }
        }

        // Find any expressions for the root element

        if (rootExpression != null) {
            BreakWhereBuilder bwb = new BreakWhereBuilder(rootExpression);
            HashMap<String, Expression> electionExpression = bwb.getSelectionExpressions();

            if (electionExpression.containsKey(rootTabledentifier)) {
                rootNode = new SelectionOperator(rootNode, electionExpression.get(rootTabledentifier));
            }
        }

        // Add all of the joined table
        // Add any join expressions to the join operator
        // Add any other expressions below the join.

        if (joinItems != null) {
            BreakWhereBuilder bwb = new BreakWhereBuilder(rootExpression);
            HashMap<String, Expression> hashSelection = bwb.getSelectionExpressions();
            HashMap<TableCouple, Expression> hashJoin = bwb.getJoinExpressions();

            while (!hashJoin.isEmpty()) {
                for (TableCouple tc : hashJoin.keySet()) {
                    Table table1 = tc.getTable1();
                    Table table2 = tc.getTable2();

                    Table table;
                    if (alreadyJoinedTables.contains(Utilities.getIdentifier(table1))) {
                        table = table2;
                    } else {
                        table = table1;
                    }

                    String identifier = Utilities.getIdentifier(table);

                    Operator rightOp = tablesToBeJoined.get(identifier);

                    if (hashSelection.containsKey(identifier)) {
                        rightOp = new SelectionOperator(rightOp, hashSelection.get(identifier));
                    }

                    rootNode = new JoinOperator(rootNode, rightOp, hashJoin.get(tc));

                    hashJoin.remove(tc);

                    alreadyJoinedTables.add(identifier);
                    tablesToBeJoined.remove(identifier);
                }
            }

            if (!tablesToBeJoined.isEmpty()) {
                for (Join join : joinItems) {
                    String identifier = Utilities.getIdentifier((Table) join.getRightItem());

                    if (tablesToBeJoined.containsKey(identifier)) {
                        Operator rightOp = tablesToBeJoined.get(identifier);
                        rootNode = new JoinOperator(rootNode, rightOp);
                    }
                }
            }
        }

        return rootNode;
    }

    /**
     * Build the scan operator for this table and add a rename step if required.
     *
     * @param table The table to be read/renamed.
     * @return the scan +/- the rename operator.
     */
    private Operator getScanAndMaybeRename(Table table) {
        ScanOperator scan = new ScanOperator(db.getTable(table.getName()));

        if (table.getAlias() == null) {
            return scan;
        } else {
            return new RenameOperator(scan, table.getAlias());
        }
    }

}
