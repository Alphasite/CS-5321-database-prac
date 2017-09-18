package datastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableHeader {
    public final List<String> columnAliases;
    public final List<String> columnHeaders;
    public final Map<String, Integer> columnMap;
    public final Map<String, Map<String, Integer>> aliasMap;

    public TableHeader(List<String> columnAlias, List<String> columnHeaders) {
        this.columnAliases = columnAlias;
        this.columnHeaders = columnHeaders;
        this.columnMap = new HashMap<>();
        this.aliasMap = new HashMap<>();

        for (int i = 0; i < this.columnHeaders.size(); i++) {
            String table = this.columnAliases.get(i);
            String header = this.columnHeaders.get(i);

            if (!this.columnMap.containsKey(header)) {
                this.columnMap.put(header, i);
            }

            Map<String, Integer> aliasColumnMap = this.aliasMap.getOrDefault(table, new HashMap<>());
            this.aliasMap.put(table, aliasColumnMap);

            if (!aliasColumnMap.containsKey(header)) {
                aliasColumnMap.put(header, i);
            }
        }
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
}
