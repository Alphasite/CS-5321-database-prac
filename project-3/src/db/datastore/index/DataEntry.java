package db.datastore.index;

/**
 * DataEntry = ( key, array of {@link Rid} )
 */
public class DataEntry {

    public int key;
    public Rid[] rids;

    public DataEntry(int key, Rid[] rids) {
        this.key = key;
        this.rids = rids;
    }
}
