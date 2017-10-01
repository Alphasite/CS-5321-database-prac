import datastore.Database;
import datastore.TableHeader;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import operators.Operator;
import operators.bag.JoinOperator;
import operators.bag.ProjectionOperator;
import operators.bag.RenameOperator;
import operators.bag.SelectionOperator;
import operators.physical.ScanOperator;
import query.BreakWhereBuilder;
import query.TableCouple;

import java.util.*;

public class QueryBuilder {
	private Database DB;

	private Map<Table, Expression> mhashSelection;
    private Map<TableCouple, Expression> mhashJoin;

	public QueryBuilder(Database db) {
		this.DB = db;
		this.mhashJoin = new HashMap<>();
		this.mhashSelection = new HashMap<>();
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

		// Keep reference to current root
		Operator rootNode = null;

		List<Table> allTables = buildTableList(fromItem, joinItems);

		preprocessWhereClause(whereItem);

		// Build a left-to-right join tree between all required tables (no reordering)
		for (Table table : allTables) {
		    Operator tableOp = new ScanOperator(DB.getTable(table.getName()));

            // If an alias is given, just rename the table internally
            // We can do this because we are allowed to assume the original name will not be used in the query
            if (table.getAlias() != null) {
                tableOp = new RenameOperator(tableOp, table.getAlias());
            }

            // TODO: add Selection operators when needed

            // Add table operator to the tree
            if (rootNode == null) {
                rootNode = tableOp;
            } else {
                // TODO: conditional join if needed
                rootNode = new JoinOperator(rootNode, tableOp);
            }
        }

		// Add joins as needed
        // TODO: move all logic to preprocessMethod
        if (joinItems != null) {
			BreakWhereBuilder bwb = new BreakWhereBuilder();
			HashMap<TableCouple,Expression> hashJoin = bwb.getHashJoin(query.getWhere());
			HashMap<Table, Expression> hashSelection = bwb.getHashSelection(query.getWhere());

			HashMap<Table,Boolean> alreadyJoinedTables =new HashMap<>();
			while (!hashJoin.isEmpty()){
				Iterator<TableCouple> iterator = hashJoin.keySet().iterator();
				while (iterator.hasNext()){
					TableCouple tc = iterator.next();
					Table table1=tc.getTable1();
					Table table2=tc.getTable2();
					if (alreadyJoinedTables.containsKey(table1)){
						if (hashSelection.containsKey(table2)){

						}


						hashJoin.remove(tc);
						alreadyJoinedTables.put(table2,true);
					}
					else{
						if (alreadyJoinedTables.containsKey(table2)){

						}
					}
			}


			}


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

		return rootNode;
	}

    /**
     *  Collapse all tables referenced in FROM clause for easier handling
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
     * Build the internal maps used to link every part of the WHERE clause to the table they reference
     * @param rootExpression the query WHERE clause
     */
    private void preprocessWhereClause(Expression rootExpression) {
	    if (rootExpression == null) {
	        return;
        }

        BreakWhereBuilder builder = new BreakWhereBuilder();
	    mhashJoin = builder.getHashJoin(rootExpression);
	    mhashSelection = builder.getHashSelection(rootExpression);

	    // TODO: add rest of logic here
    }
}
