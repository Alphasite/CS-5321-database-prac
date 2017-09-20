package datastore;

import java.util.ArrayList;
import java.util.List;

public class Tuple {
    public final TableHeader header;
    public final List<Integer> fields;

    public Tuple(TableHeader header, List<Integer> fields) {
        this.header = header;
        this.fields = fields;
    }

    public Tuple join(TableHeader header, Tuple joinTarget) {
        ArrayList<Integer> fields = new ArrayList<>(this.fields);
        fields.addAll(joinTarget.fields);
        return new Tuple(header, fields);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for (int i : fields) {
            builder.append(i + " | ");
        }

        return builder.toString();
    }
}
