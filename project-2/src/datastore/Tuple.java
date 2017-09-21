package datastore;

import java.util.ArrayList;
import java.util.List;

public class Tuple {
    public final List<Integer> fields;

    public Tuple(List<Integer> fields) {
        this.fields = fields;
    }

    public Tuple join(Tuple joinTarget) {
        ArrayList<Integer> fields = new ArrayList<>(this.fields);
        fields.addAll(joinTarget.fields);
        return new Tuple(fields);
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
