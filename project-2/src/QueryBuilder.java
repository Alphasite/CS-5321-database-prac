import datastore.Database;
import datastore.TableHeader;
import datastore.TableInfo;
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

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {
	private Database DB;

	public QueryBuilder(Database db) {
		this.DB = db;
	}

	/**
	 * Builds an optimized execution plan for the given query using a tree of operators
	 * The results of the query can be computed by iterating over the resulting root operator
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

		// Begin by building a SCAN op from the FromItem info
		TableInfo table = DB.getTable(fromItem.getName());
		rootNode = new ScanOperator(table);

		// If an alias is given, just rename the table internally
		// We can do this because we are allowed to assume the original name will not be used in the query
		if (fromItem.getAlias() != null) {
			rootNode = new RenameOperator(rootNode, fromItem.getAlias());
		}

		// Add joins as needed
        if (joinItems != null) {
            for (Join join : joinItems) {
                Table joinTable = (Table) join.getRightItem();
                Operator rightOp = new ScanOperator(DB.getTable(joinTable.getName()));
                if (joinTable.getAlias() != null) {
                	rightOp = new RenameOperator(rightOp, joinTable.getAlias());
				}

                rootNode = new JoinOperator(rootNode, rightOp);
            }
        }

		if (whereItem != null) {
			rootNode = new SelectionOperator(rootNode, whereItem);
		}

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
}
