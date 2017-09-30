import datastore.Database;
import datastore.TableHeader;
import datastore.TableInfo;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import operators.Operator;
import operators.bag.Projection;
import operators.bag.Selection;
import operators.physical.Scan;

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
		// TODO: for now build a simple query plan (SCAN -> JOIN -> SELECT -> PROJECT)
		// TODO: later on optimize by breaking down SELECT into multiple OPs evaluated as early as possible

		// Store ref to all needed query tokens
		List<SelectItem> selectItems = query.getSelectItems();
		Table fromItem = (Table) query.getFromItem();
		List<Join> joinItems = query.getJoins();
		Expression whereItem = query.getWhere();

		// Keep reference to current root
		Operator rootNode;

		// For now only build a SCAN op from the FromItem info
		TableInfo table = DB.getTable(fromItem.getName());
		rootNode = new Scan(table);

		// Add joins as needed
        if (joinItems != null) {
            for (Join join : joinItems) {
                Table joinTable = (Table) join.getRightItem();
                Scan rightScan = new Scan(DB.getTable(joinTable.getName()));

                rootNode = new operators.bag.Join(rootNode, rightScan);
            }
        }

		if (whereItem != null) {
			rootNode = new Selection(rootNode, whereItem);
		}

		// TODO: convert aliases
		if (!(selectItems.get(0) instanceof AllColumns)) {
			List<String> tableNames = new ArrayList<>();
			List<String> columnNames = new ArrayList<>();

			for (SelectItem item : selectItems) {
				Column columnRef = (Column) ((SelectExpressionItem) item).getExpression();
				tableNames.add(columnRef.getTable().getName());
				columnNames.add(columnRef.getColumnName());
			}

			rootNode = new Projection(new TableHeader(tableNames, columnNames), rootNode);
		}

		return rootNode;
	}
}
