import datastore.Database;
import datastore.TableInfo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import operators.Operator;
import operators.physical.Scan;

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

		// For now only build a SCAN op from the FromItem info
		Table from = (Table) query.getFromItem();
		TableInfo table = DB.getTable(from.getName());
		Scan node = new Scan(table);
		return node;
	}
}
