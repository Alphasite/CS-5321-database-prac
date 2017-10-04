package db.datastore;

import java.util.ArrayList;
import java.util.List;

/**
 * A class which represents just 1 tuple, with convenience methods.
 *
 * This class overrides {@link #equals(Object)} and {@link #hashCode()} so it can be used in hashmaps.
 * The internal data is not immutable however so proceed with care.
 */
public class Tuple {
    public final List<Integer> fields;

    public Tuple(List<Integer> fields) {
        this.fields = fields;
    }


    /** A method to join two tuples into a single tuple.
     * @param joinTarget The right hand tuple, it is joined after the left hand tuple.
     * @return The joined tuple.
     */
    public Tuple join(Tuple joinTarget) {
        ArrayList<Integer> fields = new ArrayList<>(this.fields);
        fields.addAll(joinTarget.fields);
        return new Tuple(fields);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(fields.get(0));

        for (int i = 1; i < fields.size(); i++) {
            builder.append(" | " + fields.get(i));
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tuple)) return false;

        Tuple tuple = (Tuple) o;

        return fields.equals(tuple.fields);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }
}
