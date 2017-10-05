package db;

import net.sf.jsqlparser.schema.Table;

public class Utilities {
    /**
     * Get the real identifier of the table, so resolve aliases if they exist, etc.
     *
     * @param table The table to have its name retrieved.
     * @return The tables 'real' name.
     */
    public static String getIdentifier(Table table) {
        if (table.getAlias() != null) {
            return table.getAlias();
        } else {
            return table.getName();
        }
    }
}
