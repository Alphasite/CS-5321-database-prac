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

import java.util.ArrayList;
import java.util.HashMap;
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
		Operator rootNode;

		rootNode=processWhereClause(whereItem, joinItems, fromItem);

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
     *
     */
    private Operator processWhereClause(Expression rootExpression, List<Join> joinItems, Table fromItem ) {
        HashMap<String,Boolean> alreadyJoinedTables =new HashMap<>();
        alreadyJoinedTables.put(fromItem.getName(), true);
        List<Table> allTables = buildTableList(fromItem, joinItems);
        HashMap<String,Boolean> tablesToBeJoined= new HashMap<>();
        for (Table t : allTables){
            tablesToBeJoined.put(t.getName(),true);
        }

        Operator rootNode = new ScanOperator(DB.getTable(fromItem.getName()));

        if (rootExpression!=null){
            BreakWhereBuilder bwb = new BreakWhereBuilder();
            HashMap<Table, Expression> hashSelection = bwb.getHashSelection(rootExpression);
            if (hashSelection.containsKey(fromItem)){
                rootNode = new SelectionOperator(rootNode, hashSelection.get(fromItem));
            }
        }
        tablesToBeJoined.remove(fromItem.getName());


        if (joinItems != null) {
            BreakWhereBuilder bwb = new BreakWhereBuilder();
            HashMap<Table, Expression> hashSelection = bwb.getHashSelection(rootExpression);
            HashMap<TableCouple,Expression> hashJoin = bwb.getHashJoin(rootExpression);
            while (!hashJoin.isEmpty()){
                for (TableCouple tc : hashJoin.keySet()){
                    Table table1=tc.getTable1();
                    Table table2=tc.getTable2();
                    Boolean temp = alreadyJoinedTables.containsKey(table1.getName());
                    if (alreadyJoinedTables.containsKey(table1.getName())){
                        Operator rightOp = new ScanOperator(DB.getTable(table2.getName()));
                        if (hashSelection.containsKey(table2)){
                            rightOp = new SelectionOperator(rightOp, hashSelection.get(table2));
                        }
                        if (table2.getAlias() != null) {
                            rightOp = new RenameOperator(rightOp, table2.getAlias());
                        }
                        rootNode = new JoinOperator(rootNode, rightOp, hashJoin.get(tc));
                        hashJoin.remove(tc);
                        alreadyJoinedTables.put(table2.getName(),true);
                        tablesToBeJoined.remove(table2.getName());
                    }
                    else{
                        Operator rightOp = new ScanOperator(DB.getTable(table1.getName()));
                        if (hashSelection.containsKey(table1)){
                            rightOp = new SelectionOperator(rightOp, hashSelection.get(table1));
                        }
                        if (table1.getAlias() != null) {
                            rightOp = new RenameOperator(rightOp, table1.getAlias());
                        }
                        rootNode = new JoinOperator(rootNode, rightOp, hashJoin.get(tc));
                        hashJoin.remove(tc);
                        alreadyJoinedTables.put(table1.getName(),true);
                        tablesToBeJoined.remove(table1.getName());
                    }
                }
            }
        }
        while(!tablesToBeJoined.isEmpty()){

        }
        return rootNode;
    }
}
