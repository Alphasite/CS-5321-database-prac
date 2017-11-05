package db.datastore.index;

/**
 * Rid = ( pageid, tupleid )
 */
public class Rid {

    public int pageid;
    public int recordid;

    public Rid(int pageid, int recordid) {
        this.pageid = pageid;
        this.recordid = recordid;
    }
}
