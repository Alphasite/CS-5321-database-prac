package db.datastore.index;

/**
 * Rid = ( pageid, tupleid )
 *
 * Implements the {@link Comparable} interface : Rids are sorted by pageid, then tupleid
 */
public class Rid implements Comparable<Rid> {

    public int pageid;
    public int tupleid;

    public Rid(int pageid, int tupleid) {
        this.pageid = pageid;
        this.tupleid = tupleid;
    }

    @Override
    public int compareTo(Rid o) {
        if (o.pageid != pageid) {
            return Integer.compare(pageid, o.pageid);
        } else {
            return Integer.compare(tupleid, o.tupleid);
        }
    }
}
