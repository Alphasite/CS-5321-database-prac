package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LogicalSortOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator source;
    private TableHeader sortHeader;

    public LogicalSortOperator(LogicalOperator source, TableHeader sortHeader) {
        this.source = source;

        Set<String> alreadySortedColumns = new HashSet<>();

        List<String> aliases = new ArrayList<>();
        List<String> columns = new ArrayList<>();

        for (int i = 0; i < sortHeader.columnAliases.size(); i++) {
            String alias = sortHeader.columnAliases.get(i);
            String column = sortHeader.columnHeaders.get(i);
            String fullName = alias + "." + column;

            if (!alreadySortedColumns.contains(fullName)) {
                aliases.add(alias);
                columns.add(column);
                alreadySortedColumns.add(fullName);
            }
        }

        TableHeader header = this.source.getHeader();
        for (int i = 0; i < header.columnAliases.size(); i++) {
            String alias = header.columnAliases.get(i);
            String column = header.columnHeaders.get(i);
            String fullName = alias + "." + column;

            // Append non specified columns so that they are used to break ties
            if (!alreadySortedColumns.contains(fullName)) {
                aliases.add(alias);
                columns.add(column);
                alreadySortedColumns.add(fullName);
            }
        }

        this.sortHeader = new TableHeader(aliases, columns);
    }

    public TableHeader getSortHeader() {
        return sortHeader;
    }

    @Override
    public TableHeader getHeader() {
        return source.getHeader();
    }

    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public LogicalOperator getChild() {
        return source;
    }
}
