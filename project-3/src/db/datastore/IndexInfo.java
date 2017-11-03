package db.datastore;

public class IndexInfo {

    public String tableName;
    public String attributeName;

    public boolean isClustered;

    public int treeOrder;

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
}
