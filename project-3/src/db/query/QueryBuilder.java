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
    /**
     * Map Table name to corresponding selection predicate
     */
    private Map<String, Expression> selectionExpressions;
    /**
     * Map (Table1, Table2) couples to corresponding joining predicates
     */
    private Map<TableCouple, Expression> joinExpressions;

    public QueryBuilder(Database db) {
        this.db = db;
        this.selectionExpressions = new HashMap<>();
        this.joinExpressions = new HashMap<>();
    }

    private void reset() {
        this.selectionExpressions = new HashMap<>();
        this.joinExpressions = new HashMap<>();
    }

    /**
     * Builds an optimized execution plan for the given query using a tree of operators
     * The results of the query can be computed by iterating over the resulting root operator
     *
     * @param query The parsed query object
     * @return The root operator of the query execution plan tree
     */
    public LogicalOperator buildQuery(PlainSelect query) {
        this.reset();

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
            Set<String> alreadySortedColumns = new HashSet<>();

            List<String> aliases = new ArrayList<>();
            List<String> columns = new ArrayList<>();

            for (OrderByElement element : orderBy) {
                Column columnInstance = (Column) element.getExpression();

                String alias = Utilities.getIdentifier(columnInstance.getTable());
                String column = columnInstance.getColumnName();
                String fullName = alias + "." + column;

                if (!alreadySortedColumns.contains(fullName)) {
                    aliases.add(alias);
                    columns.add(column);
                    alreadySortedColumns.add(fullName);
                }
            }

            TableHeader header = rootNode.getHeader();
            for (int i = 0; i < header.columnAliases.size(); i++) {
                String alias = header.columnAliases.get(i);
                String column = header.columnHeaders.get(i);
                String fullName = alias + "." + column;

                // Append non specified columns so that they are used to break ties
                if (!alreadySortedColumns.contains(fullName)) {
                    aliases.add(alias);
                    columns.add(column);
                    alreadySortedColumns.add(fullName);
                }
            }

            TableHeader sortHeader = new TableHeader(aliases, columns);
            rootNode = new LogicalSortOperator(rootNode, sortHeader);
        }

        if (isDistinct) {
            // Current implementation requires sorted queries
            if (orderBy == null) {
                // Sort all fields
                rootNode = new LogicalSortOperator(rootNode, rootNode.getHeader());
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
        String rootTabledentifier = Utilities.getIdentifier(rootTable);
        alreadyJoinedTables.add(rootTabledentifier);

        LogicalOperator rootNode = this.getScanAndMaybeRename(rootTable);

        // Then find which other tables exist.
        // then store them in a list of tables which haven't yet been joined.

        Map<String, LogicalOperator> tablesToBeJoined = new HashMap<>();
        if (joinItems != null) {
            for (Join join : joinItems) {
                Table table = (Table) join.getRightItem();
                tablesToBeJoined.put(Utilities.getIdentifier(table), this.getScanAndMaybeRename(table));
            }
        }

        // Decompose the expression tree and then add the root nodes expressions to the root node.

        if (rootExpression != null) {
            WhereDecomposer bwb = new WhereDecomposer(rootExpression);
            selectionExpressions = bwb.getSelectionExpressions();
            joinExpressions = bwb.getJoinExpressions();
        }

        if (selectionExpressions.containsKey(rootTabledentifier)) {
            rootNode = new LogicalSelectOperator(rootNode, selectionExpressions.get(rootTabledentifier));
        }

        // Add all of the joined tables
        // Add any join expressions to the join operator
        // Add any other expressions below the join.

        if (joinItems != null) {
            while (!joinExpressions.isEmpty()) {
                // Iterate over a copy to allow removing from original
                for (TableCouple tc : new HashMap<>(joinExpressions).keySet()) {
                    String table1 = tc.getTable1();
                    String table2 = tc.getTable2();

                    String identifier;
                    if (alreadyJoinedTables.contains(table1)) {
                        identifier = table2;
                    } else {
                        identifier = table1;
                    }

                    LogicalOperator rightOp = tablesToBeJoined.get(identifier);

                    if (selectionExpressions.containsKey(identifier)) {
                        rightOp = new LogicalSelectOperator(rightOp, selectionExpressions.get(identifier));
                    }

                    rootNode = new LogicalJoinOperator(rootNode, rightOp, joinExpressions.get(tc));

                    joinExpressions.remove(tc);

                    alreadyJoinedTables.add(identifier);
                    tablesToBeJoined.remove(identifier);
                }
            }

            if (!tablesToBeJoined.isEmpty()) {
                for (Join join : joinItems) {
                    String identifier = Utilities.getIdentifier((Table) join.getRightItem());

                    if (tablesToBeJoined.containsKey(identifier)) {
                        LogicalOperator rightOp = tablesToBeJoined.get(identifier);
                        rootNode = new LogicalJoinOperator(rootNode, rightOp, null);
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
    private LogicalOperator getScanAndMaybeRename(Table table) {
        LogicalScanOperator scan = new LogicalScanOperator(db.getTable(table.getName()));

        if (table.getAlias() == null) {
            return scan;
        } else {
            return new LogicalRenameOperator(scan, table.getAlias());
        }
    }

}
