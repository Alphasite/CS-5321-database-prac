package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.util.ArrayList;
import java.util.List;

public class Projection implements Operator {
    private final Operator source;
    private final TableHeader newHeader;
    private final List<Integer> newToOldColumnMapping;

    public Projection(TableHeader newHeader, Operator source) {
        this.source = source;
        this.newHeader = newHeader.clone();
        this.newToOldColumnMapping = new ArrayList<>();

        nextNewColumnLoop:
        for (int i = 0; i < newHeader.size(); i++) {
            String alias = newHeader.columnAliases.get(i);
            String header = newHeader.columnHeaders.get(i);

            boolean notRequireAliasMatch = alias.equals("");

            List<String> sourceAliases = this.source.getHeader().columnAliases;
            List<String> sourceHeaders = this.source.getHeader().columnHeaders;

            for (int j = 0; j < sourceHeaders.size(); j++) {
                boolean aliasMatch = sourceAliases.get(j).equals(alias);
                boolean headerMatch = sourceHeaders.get(j).equals(header);

                if ((notRequireAliasMatch || aliasMatch) && headerMatch) {
                    this.newToOldColumnMapping.add(j);
                    this.newHeader.columnAliases.set(i, sourceAliases.get(j));
                    continue nextNewColumnLoop;
                }
            }

            throw new RuntimeException("Projection mappings are incorrect. " + alias + "." + header + " has no match.");
        }
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = this.source.getNextTuple();

        if (tuple!=null) {
            List<Integer> newBackingArray = new ArrayList<>(this.newHeader.size());

            for (int i = 0; i < this.newHeader.size(); i++) {
                newBackingArray.add(tuple.fields.get(this.newToOldColumnMapping.get(i)));
            }

            return (new Tuple(newBackingArray));
        } else {
            return tuple;
        }
    }

    @Override
    public TableHeader getHeader() {
        return newHeader;
    }

    @Override
    public boolean reset() {
        return this.source.reset();
    }
}
