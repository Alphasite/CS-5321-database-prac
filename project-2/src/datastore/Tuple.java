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

    public String toRowForm() {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < this.fields.size(); i++) {
            builder.append(this.fields.get(i));
            builder.append(" | ");
        }

        return builder.toString();
    }
}
