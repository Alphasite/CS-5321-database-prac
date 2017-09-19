package datastore;

import java.util.ArrayList;
import java.util.List;

public class TableHeader {
    public final List<String> columnAliases;
    public final List<String> columnHeaders;

    public TableHeader(List<String> columnAlias, List<String> columnHeaders) {
        this.columnAliases = columnAlias;
        this.columnHeaders = columnHeaders;
    }

    public int size() {
        return this.columnHeaders.size();
    }

    public String toHeaderForm() {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < this.columnHeaders.size(); i++) {
            builder.append(this.columnAliases.get(i));
            builder.append(".");
            builder.append(this.columnHeaders.get(i));
            builder.append(" | ");
        }

        return builder.toString();
    }

    @Override
    public TableHeader clone() {
        return new TableHeader(
                new ArrayList<>(this.columnAliases),
                new ArrayList<>(this.columnHeaders)
        );
    }
}
