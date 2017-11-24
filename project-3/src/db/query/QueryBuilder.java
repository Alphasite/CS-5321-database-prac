package db.query;

import db.Utilities.Pair;
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
import java.util.Set;

import static db.Utilities.Utilities.*;

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
     * List of identifying table names (either original name or alias defined in FROM clause).
     * Used by other tokens in query (SELECT, WHERE, ORDER BY).
     */
    private List<String> tableIdentifiers;

    /**
     * List of logical operators used to access table tuples, in the order specified in the FROM token
     * (ie. Scan or Scan + Rename).
     */
    private List<LogicalScanOperator> tableOperators;

    /**
     * Initialize query builder using the provided database object as a source for Table schema information
     */
    public QueryBuilder(Database db, UnionFind unionFind) {
        this.db = db;
        this.unionFind = unionFind;
        this.tableIdentifiers = new ArrayList<>();
        this.tableOperators = new ArrayList<>();
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
        tableOperators.add(getScanAndMaybeRename(fromTable));

        if (joins != null) {
            for (Join join : joins) {
                Table joinTable = (Table) join.getRightItem();
                tableIdentifiers.add(Utilities.getIdentifier(joinTable));
                tableOperators.add(getScanAndMaybeRename(joinTable));
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
        LogicalOperator rootNode;

        // Decompose the expression tree and then add the root nodes expressions to the root node.

        for (LogicalOperator operator : this.tableOperators) {
            TableHeader header = operator.getHeader();

            for (String attribute : header.getQualifiedAttributeNames()) {
                unionFind.add(attribute);
            }
        }

        if (rootExpression != null) {
            WhereDecomposer bwb = new WhereDecomposer(unionFind);
            rootExpression.accept(bwb);

            List<LogicalOperator> scansOrSelects = new ArrayList<>();
            for (LogicalScanOperator scan : this.tableOperators) {
                TableHeader header = scan.getHeader();

                Expression expression = null;

                for (String attribute : header.getQualifiedAttributeNames()) {
                    if (unionFind.getMinimum(attribute) != null) {
                        expression = joinExpression(expression, greaterThanColumn(attribute, unionFind.getMinimum(attribute)));
                    }

                    if (unionFind.getMaximum(attribute) != null) {
                        expression = joinExpression(expression, lessThanColumn(attribute, unionFind.getMaximum(attribute)));
                    }

                    for (Set<String> equalitySet : unionFind.getSets()) {
                        List<String> equalHeaders = new ArrayList<>();

                        for (String column : equalitySet) {
                            Pair<String, String> splitColumn = splitLongFormColumn(column);
                            if (splitColumn.getLeft().equals(scan.getTableName())) {
                                equalHeaders.add(splitColumn.getRight());
                            }
                        }

                        if (equalHeaders.size() > 1) {
                            for (int j = 1; j < equalHeaders.size(); j++) {
                                Expression equalityExpression = Utilities.equalPairToExpression(
                                        scan.getTableName() + "." + equalHeaders.get(j - 1),
                                        scan.getTableName() + "." + equalHeaders.get(j)
                                );

                                expression = joinExpression(expression, equalityExpression);
                            }
                        }
                    }
                }

                if (expression == null) {
                    scansOrSelects.add(scan);
                } else {
                    scansOrSelects.add(new LogicalSelectOperator(scan, expression));
                }
            }

            rootNode = new LogicalJoinOperator(scansOrSelects, bwb.getUnionFind(), bwb.getUnusableExpressions());

            if (bwb.getNakedExpression() != null) {
                rootNode = new LogicalSelectOperator(rootNode, bwb.getNakedExpression());
            }
        } else {
            rootNode = new LogicalJoinOperator(new ArrayList<>(this.tableOperators), new UnionFind(), new ArrayList<>());
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
