package db.query;

import db.Utilities;
import db.datastore.Database;
import db.datastore.TableHeader;
import db.operators.logical.*;
import db.operators.physical.Operator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

/**
 * Query plan generator
 * <p>
 * This class reads the tokens from the parsed SQL query and generates an tree of {@link Operator}
 * that can then be used to retrieve all matching records.
 * <p>
 * Supports SELECT-FROM-WHERE queries with some restrictions as well as DISTINCT and ORDER BY
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
    public LogicalOperator buildQuery(PlainSelect query) {
        // Store ref to all needed query tokens
        List<SelectItem> selectItems = query.getSelectItems();
        Table fromItem = (Table) query.getFromItem();
        List<Join> joinItems = query.getJoins();
        Expression whereItem = query.getWhere();
        List<OrderByElement> orderBy = query.getOrderByElements();
        boolean isDistinct = query.getDistinct() != null;

        // Keep reference to current root
        LogicalOperator rootNode;

        // Build the scan-select-join tree structure
        rootNode = processWhereClause(whereItem, joinItems, fromItem);

        // Add projections
        if (!(selectItems.get(0) instanceof AllColumns)) {
            List<String> tableNames = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();

            for (SelectItem item : selectItems) {
                Column columnRef = (Column) ((SelectExpressionItem) item).getExpression();
                tableNames.add(columnRef.getTable().getName());
                columnNames.add(columnRef.getColumnName());
            }

            rootNode = new LogicalProjectOperator(rootNode, new TableHeader(tableNames, columnNames));
        }

        // The spec allows handling sorting and duplicate elimination after projection

        if (orderBy != null) {
            List<String> aliases = new ArrayList<>();
            List<String> columns = new ArrayList<>();

            for (OrderByElement element : orderBy) {
                Column columnInstance = (Column) element.getExpression();

                String alias = Utilities.getIdentifier(columnInstance.getTable());
                String column = columnInstance.getColumnName();

                aliases.add(alias);
                columns.add(column);
            }

            TableHeader sortHeader = new TableHeader(aliases, columns);
            rootNode = new LogicalSortOperator(rootNode, sortHeader);
        }

        if (isDistinct) {
            // Current implementation requires sorted queries
            if (orderBy == null) {
                // Sort all fields
                rootNode = new LogicalSortOperator(rootNode, new TableHeader());
            }

            rootNode = new LogicalDistinctOperator(rootNode);
        }

        return rootNode;
    }

    /**
     * Build the internal maps used to link every part of the WHERE clause to the table they reference
     *
     * @param rootExpression the where expression
     * @param joinItems      the non root joined tables and their aliases
     * @param rootTable      the root table, special cased for some reason.
     * @return The root node of the sql tree formed from the where and from conditions
     */
    private LogicalOperator processWhereClause(Expression rootExpression, List<Join> joinItems, Table rootTable) {

        // First create the root node.
        Set<String> alreadyJoinedTables = new HashSet<>();

        LogicalOperator rootNode = null;

        // Then find which other tables exist.
        // then store them in a list of tables which haven't yet been joined.

        Map<String, LogicalOperator> tablesToBeJoined = new HashMap<>();
        Set<String> unjoinedTables = new HashSet<>();

        unjoinedTables.add(Utilities.getIdentifier(rootTable));
        tablesToBeJoined.put(Utilities.getIdentifier(rootTable), this.getScanAndMaybeRename(rootTable));

        if (joinItems != null) {
            for (Join join : joinItems) {
                Table table = (Table) join.getRightItem();
                String identifier = Utilities.getIdentifier(table);
                tablesToBeJoined.put(identifier, this.getScanAndMaybeRename(table));
                unjoinedTables.add(identifier);
            }
        }

        // Decompose the expression tree and then add the root nodes expressions to the root node.
        Map<String, Expression> selectionExpressions = new HashMap<>();
        Map<TableCouple, Expression> joinExpressions = new HashMap<>();

        if (rootExpression != null) {
            WhereDecomposer bwb = new WhereDecomposer(rootExpression);
            selectionExpressions.putAll(bwb.getSelectionExpressions());
            joinExpressions.putAll(bwb.getJoinExpressions());
        }

        TriFunction<LogicalOperator, String, Expression, LogicalOperator> joinTable = (root, tableName, expression) -> {
            LogicalOperator rightOp = tablesToBeJoined.get(tableName);

            if (selectionExpressions.containsKey(tableName)) {
                rightOp = new LogicalSelectOperator(rightOp, selectionExpressions.get(tableName));
            }

            alreadyJoinedTables.add(tableName);
            unjoinedTables.remove(tableName);

            if (root != null) {
                return new LogicalJoinOperator(root, rightOp, expression);
            } else {
                return rightOp;
            }
        };

        // Add all of the joined tables
        // Add any join expressions to the join operator
        // Add any other expressions below the join.

        if (joinItems != null) {
            // Iterate over a copy to allow removing from original
            for (TableCouple tc : joinExpressions.keySet()) {
                String table1 = tc.getTable1();
                String table2 = tc.getTable2();

                Expression expression = joinExpressions.get(tc);

                if (!alreadyJoinedTables.contains(table1) && !alreadyJoinedTables.contains(table2)) {
                    rootNode = joinTable.apply(rootNode, table1, null);
                    rootNode = joinTable.apply(rootNode, table2, expression);
                } else if (!alreadyJoinedTables.contains(table1)) {
                    rootNode = joinTable.apply(rootNode, table1, expression);
                } else if (!alreadyJoinedTables.contains(table2)) {
                    rootNode = joinTable.apply(rootNode, table2, expression);
                } else {
                    rootNode = new LogicalSelectOperator(rootNode, expression);
                }
            }
        }

        for (String table : unjoinedTables) {
            rootNode = joinTable.apply(rootNode, table, null);
        }

        return rootNode;
    }

    /**
     * Build the scan operator for this table and add a rename step if required.
     *
     * @param table The table to be read/renamed.
     * @return the scan +/- the rename operator.
     */
    private LogicalOperator getScanAndMaybeRename(Table table) {
        LogicalScanOperator scan = new LogicalScanOperator(db.getTable(table.getName()));

        if (table.getAlias() == null) {
            return scan;
        } else {
            return new LogicalRenameOperator(scan, table.getAlias());
        }
    }
}
