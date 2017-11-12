package db.query;

import db.Utilities.UnionFind;
import db.Utilities.Utilities;
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
    private UnionFind unionFind;


    /**
     * Initialize query builder using the provided database object as a source for Table schema information
     */
    public QueryBuilder(Database db, UnionFind unionFind) {
        this.db = db;
        this.unionFind = unionFind;
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
        Table fromItem = (Table) query.getFromItem();
        List<Join> joinItems = query.getJoins();
        Expression whereItem = query.getWhere();
        List<OrderByElement> orderBy = query.getOrderByElements();
        boolean isDistinct = query.getDistinct() != null;

        if (joinItems == null) {
            joinItems = new ArrayList<>();
        }

        // Keep reference to current root
        LogicalOperator rootNode;

        // Build the scan-select-join tree structure
        rootNode = processWhereClause(whereItem, fromItem, joinItems);

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

            List<String> tableNames = new ArrayList<>();

            tableNames.add(Utilities.getIdentifier(fromItem));

            for (Join joinItem : joinItems) {
                String alias = Utilities.getIdentifier((Table) joinItem.getRightItem());
                tableNames.add(alias);
            }

            for (String alias : tableNames) {
                for (int i = 0; i < header.size(); i++) {
                    if (header.columnAliases.get(i).equals(alias)) {
                        aliases.add(alias);
                        columns.add(header.columnHeaders.get(i));
                    }
                }
            }

            rootNode = new LogicalProjectOperator(rootNode, new TableHeader(aliases, columns));
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
     * @param joinRootTable the left most table on the expression tree.
     * @param rightJoinExpressions the non root joined tables and their aliases
     * @return The root node of the sql tree formed from the where and from conditions
     */
    private LogicalOperator processWhereClause(Expression rootExpression, Table joinRootTable, List<Join> rightJoinExpressions) {
        // No root node.
        LogicalOperator rootNode = null;

        // Then find which other tables exist.
        // then store them in a list of tables which haven't yet been joined.

        Map<String, LogicalOperator> joinableTableInstances = new HashMap<>();

        Set<String> joinedTables = new HashSet<>();
        Set<String> unjoinedTables = new HashSet<>();

        // Since the root table is a special case, handle that.
        unjoinedTables.add(Utilities.getIdentifier(joinRootTable));
        joinableTableInstances.put(Utilities.getIdentifier(joinRootTable), this.getScanAndMaybeRename(joinRootTable));

        for (Join join : rightJoinExpressions) {
            Table table = (Table) join.getRightItem();
            String identifier = Utilities.getIdentifier(table);
            joinableTableInstances.put(identifier, this.getScanAndMaybeRename(table));
            unjoinedTables.add(identifier);
        }

        // Decompose the expression tree and then add the root nodes expressions to the root node.
        Map<String, Expression> selectionExpressions = new HashMap<>();
        Map<TablePair, Expression> joinExpressions = new HashMap<>();
        Expression nakedExpression = null;

        for (LogicalOperator operator : joinableTableInstances.values()) {
            TableHeader header = operator.getHeader();

            for (int i = 0; i < header.size(); i++) {
                unionFind.add(header.columnAliases.get(i) + "." + header.columnHeaders.get(i));
            }
        }

        if (rootExpression != null) {
            WhereDecomposer bwb = WhereDecomposer.decompose(rootExpression, unionFind);

            selectionExpressions.putAll(bwb.getSelectionExpressions());
            joinExpressions.putAll(bwb.getJoinExpressions());
            nakedExpression = bwb.getNakedExpression();
        }

        TriFunction<LogicalOperator, String, Expression, LogicalOperator> joinTable = (root, tableName, expression) -> {
            LogicalOperator rightOp = joinableTableInstances.get(tableName);

            if (selectionExpressions.containsKey(tableName)) {
                rightOp = new LogicalSelectOperator(rightOp, selectionExpressions.get(tableName));
            }

            joinedTables.add(tableName);
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
        for (TablePair tc : joinExpressions.keySet()) {
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

        for (String table : unjoinedTables) {
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

    public UnionFind getUnionFind() {
        return unionFind;
    }
}
