package db.datastore;

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
