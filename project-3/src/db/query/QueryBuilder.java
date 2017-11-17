package db.query;

import db.Utilities;
import db.datastore.Database;
import db.datastore.TableHeader;
import db.operators.logical.*;
import db.query.visitors.WhereDecomposer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

/**
 * Query plan generator
 * <p>
 * This class reads the tokens from the parsed SQL query and generates a tree of {@link LogicalOperator}
 * that can then be used to retrieve all matching records.
 * <p>
 * Supports SELECT-FROM-WHERE queries with some restrictions as well as DISTINCT and ORDER BY
 */
public class QueryBuilder {
    private Database db;

    /**
     * List of identifying table names (either original name or alias defined in FROM clause).
     * Used by other tokens in query (SELECT, WHERE, ORDER BY).
     */
    private List<String> tableIdentifiers;

    /**
     * Map every table <em>identifier</em> to the corresponding logical operator used to access its tuples
     * (ie. Scan or Scan + Rename).
     */
    private Map<String, LogicalOperator> tableOperators;

    /**
     * Initialize query builder using the provided database object as a source for Table schema information
     */
    public QueryBuilder(Database db) {
        this.db = db;
        this.tableIdentifiers = new ArrayList<>();
        this.tableOperators = new HashMap<>();
    }

    /**
     * Populate internal structures with name/alias info for all referenced tables and create Scan/Rename operators
     *
     * @param fromTable The leftmost table in the join
     * @param joins     All other joins
     */
    private void initTableInfo(Table fromTable, List<Join> joins) {
        // reset tables
        tableIdentifiers.clear();
        tableOperators.clear();

        tableIdentifiers.add(Utilities.getIdentifier(fromTable));
        tableOperators.put(Utilities.getIdentifier(fromTable), getScanAndMaybeRename(fromTable));

        if (joins != null) {
            for (Join join : joins) {
                Table joinTable = (Table) join.getRightItem();
                tableIdentifiers.add(Utilities.getIdentifier(joinTable));
                tableOperators.put(Utilities.getIdentifier(joinTable), getScanAndMaybeRename(joinTable));
            }
        }
    }

    /**
     * Builds an optimized Relational Algebra execution plan for the given query using a tree of logical operators.
     * Optimizations performed at this stage include evaluating predicates as early as possible.
     * It is necessary to convert the resulting tree to a physical plan in order to execute the query.
     *
     * @param query Parsed query object
     * @return Root operator of logical query plan
     */
    @SuppressWarnings("unchecked")
    public LogicalOperator buildQuery(PlainSelect query) {
        // Store ref to all needed query tokens
        List<SelectItem> selectItems = query.getSelectItems();
        Expression whereItem = query.getWhere();
        List<OrderByElement> orderBy = query.getOrderByElements();
        boolean isDistinct = query.getDistinct() != null;

        this.initTableInfo((Table) query.getFromItem(), query.getJoins());

        // Keep reference to current root
        LogicalOperator rootNode;

        // Build the scan-select-join tree structure
        rootNode = processWhereClause(whereItem);

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
        } else {
            // If all columns is selected, the joins may have been reordered, so we need to reproject them in the
            // expected order, as specified by the join statements.

            List<String> aliases = new ArrayList<>();
            List<String> columns = new ArrayList<>();

            TableHeader header = rootNode.getHeader();

            for (String alias : this.tableIdentifiers) {
                for (int i = 0; i < header.size(); i++) {
                    if (header.tableIdentifiers.get(i).equals(alias)) {
                        aliases.add(alias);
                        columns.add(header.columnNames.get(i));
                    }
                }
            }

            rootNode = new LogicalProjectOperator(rootNode, new TableHeader(aliases, columns));
        }

        // The spec allows handling sorting and duplicate elimination after projection

        if (orderBy != null) {
            List<String> referencedTables = new ArrayList<>();
            List<String> columns = new ArrayList<>();

            for (OrderByElement element : orderBy) {
                Column columnInstance = (Column) element.getExpression();

                String tableId = Utilities.getIdentifier(columnInstance.getTable());
                String column = columnInstance.getColumnName();

                referencedTables.add(tableId);
                columns.add(column);
            }

            TableHeader sortHeader = new TableHeader(referencedTables, columns);
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
     * @return The root node of the sql tree formed from the where and from conditions
     */
    private LogicalOperator processWhereClause(Expression rootExpression) {
        // No root node.
        LogicalOperator rootNode = null;

        Set<String> tablesToJoin = new HashSet<>(this.tableIdentifiers);
        Set<String> joinedTables = new HashSet<>();

        // Decompose the expression tree and then add the root nodes expressions to the root node.
        Map<String, Expression> selectionExpressions = new HashMap<>();
        Map<TableCouple, Expression> joinExpressions = new HashMap<>();
        Expression nakedExpression = null;

        if (rootExpression != null) {
            WhereDecomposer bwb = new WhereDecomposer(rootExpression);
            selectionExpressions.putAll(bwb.getSelectionExpressions());
            joinExpressions.putAll(bwb.getJoinExpressions());
            nakedExpression = bwb.getNakedExpression();
        }

        TriFunction<LogicalOperator, String, Expression, LogicalOperator> joinTable = (root, tableName, expression) -> {
            LogicalOperator rightOp = this.tableOperators.get(tableName);

            if (selectionExpressions.containsKey(tableName)) {
                rightOp = new LogicalSelectOperator(rightOp, selectionExpressions.get(tableName));
            }

            joinedTables.add(tableName);
            tablesToJoin.remove(tableName);

            if (root != null) {
                return new LogicalJoinOperator(root, rightOp, expression);
            } else {
                return rightOp;
            }
        };

        // Add all of the joined tables
        // Add any join expressions to the join operator
        // Add any other expressions below the join.
        for (TableCouple tc : joinExpressions.keySet()) {
            String table1 = tc.getTable1();
            String table2 = tc.getTable2();

            Expression expression = joinExpressions.get(tc);

            if (!joinedTables.contains(table1) && !joinedTables.contains(table2)) {
                rootNode = joinTable.apply(rootNode, table1, null);
                rootNode = joinTable.apply(rootNode, table2, expression);
            } else if (!joinedTables.contains(table1)) {
                rootNode = joinTable.apply(rootNode, table1, expression);
            } else if (!joinedTables.contains(table2)) {
                rootNode = joinTable.apply(rootNode, table2, expression);
            } else {
                rootNode = new LogicalSelectOperator(rootNode, expression);
            }
        }

        for (String table : tablesToJoin) {
            rootNode = joinTable.apply(rootNode, table, null);
        }

        // Add naked expression as root selection
        // TODO: evaluate condition at build time and act accordingly
        if (nakedExpression != null) {
            rootNode = new LogicalSelectOperator(rootNode, nakedExpression);
        }

        return rootNode;
    }

    /**
     * Build the scan operator for this table and add a rename step if required.
     *
     * @param table The table token extracted from the SQL FROM clause to be read/renamed.
     * @return scan operator with an optional rename to handle aliases
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
