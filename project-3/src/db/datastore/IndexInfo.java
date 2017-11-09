package db.datastore;

/**
 * Holder object for index config : table and attribute, clustered, tree order
 */
public class IndexInfo {

    public String tableName;
    public String attributeName;

    public boolean isClustered;

    public int treeOrder;

    public IndexInfo() { }

    public IndexInfo(String tableName, String attributeName, boolean isClustered, int treeOrder) {
        this.tableName = tableName;
        this.attributeName = attributeName;
        this.isClustered = isClustered;
        this.treeOrder = treeOrder;
    }

    /**
     * Creates an instance of an IndexInfo class from a string in the form of
     * "{tableName} {attributeName} {isClustered} {treeOrder}"
     * @param s The string to parse IndexInfo fields from
     * @return A new IndexInfo instance
     */
    public static IndexInfo parse(String s) {
        IndexInfo info = new IndexInfo();

        String[] tokens = s.split(" ");

        assert tokens.length == 4;

        info.tableName = tokens[0];
        info.attributeName = tokens[1];
        info.isClustered = (tokens[2].equals("1"));
        info.treeOrder = Integer.parseInt(tokens[3]);

        return info;
    }

    @Override
    public String toString() {
        return tableName + " " + attributeName + " " + isClustered + " " + treeOrder;
    }
}
