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

import java.util.ArrayList;
import java.util.List;

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

        List<LogicalScanOperator> tables = new ArrayList<>();
        tables.add(this.getScanAndMaybeRename(joinRootTable));

        for (Join join : rightJoinExpressions) {
            Table table = (Table) join.getRightItem();
            tables.add(this.getScanAndMaybeRename(table));
        }

        // Decompose the expression tree and then add the root nodes expressions to the root node.

        for (LogicalScanOperator operator : tables) {
            TableHeader header = operator.getHeader();

            for (int i = 0; i < header.size(); i++) {
                unionFind.add(header.columnAliases.get(i) + "." + header.columnHeaders.get(i));
            }
        }

        if (rootExpression != null) {
            WhereDecomposer bwb = new WhereDecomposer(unionFind);
            rootExpression.accept(bwb);

            rootNode = new LogicalJoinOperator(tables, bwb.getUnionFind(), bwb.getUnusableExpressions());

            if (bwb.getNakedExpression() != null) {
                rootNode = new LogicalSelectOperator(rootNode, bwb.getNakedExpression());
            }
        } else {
            rootNode = new LogicalJoinOperator(tables, new UnionFind(), new ArrayList<>());
        }

        return rootNode;
    }

    /**
     * Build the scan operator for this table and add a rename step if required.
     *
     * @param table The table token extracted from the SQL FROM clause to be read/renamed.
     * @return scan operator with an optional rename to handle aliases
     */
    private LogicalScanOperator getScanAndMaybeRename(Table table) {
        return new LogicalScanOperator(db.getTable(table.getName()), Utilities.getIdentifier(table));
    }

    public UnionFind getUnionFind() {
        return unionFind;
    }
}
