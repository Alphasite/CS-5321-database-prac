package operators.extended;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class Sort implements Operator {
    private Operator source;
    private List<Tuple> buffer;
    private Iterator<Tuple> bufferIterator;
    private List<Integer> tupleSortPriorityIndex;

    public Sort(Operator source, TableHeader sortHeaders) {
        this.source = source;
        this.tupleSortPriorityIndex = new ArrayList<>();

        nextNewColumnLoop:
        for (int i = 0; i < sortHeaders.size(); i++) {
            String alias = sortHeaders.columnAliases.get(i);
            String header = sortHeaders.columnHeaders.get(i);

            boolean notRequireAliasMatch = alias.equals("");

            List<String> sourceAliases = this.source.getHeader().columnAliases;
            List<String> sourceHeaders = this.source.getHeader().columnHeaders;

            for (int j = 0; j < sourceHeaders.size(); j++) {
                boolean aliasMatch = sourceAliases.get(j).equals(alias);
                boolean headerMatch = sourceHeaders.get(j).equals(header);

                if ((notRequireAliasMatch || aliasMatch) && headerMatch) {
                    this.tupleSortPriorityIndex.add(j);
                    continue nextNewColumnLoop;
                }
            }

            throw new RuntimeException("Projection mappings are incorrect. " + alias + "." + header + " has no match.");
        }

        this.reset();
    }

    @Override
    public Optional<Tuple> getNextTuple() {
        if (this.bufferIterator.hasNext()) {
            return Optional.of(this.bufferIterator.next());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public TableHeader getHeader() {
        return this.source.getHeader();
    }

    @Override
    public boolean reset() {
        this.buffer();
        return this.source.reset();
    }

    private void buffer() {
        this.buffer = new ArrayList<>();

        Optional<Tuple> tuple;
        while ((tuple = this.source.getNextTuple()).isPresent()) {
            this.buffer.add(tuple.get());
        }

        this.buffer.sort((a, b) -> {
            for (Integer tupleIndex : this.tupleSortPriorityIndex) {
                Integer leftField = a.fields.get(tupleIndex);
                Integer rightField = b.fields.get(tupleIndex);

                int result = Integer.compare(leftField, rightField);

                if (result != 0) {
                    return result;
                }
            }

            return 0;
        });

        this.bufferIterator = this.buffer.iterator();
    }
}
