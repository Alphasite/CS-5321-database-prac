package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * An operator which performs a projection (similar to the relational operator).
 * <p>
 * It removes columns which are not explicitly requested to be retained.
 * <p>
 * If no alias is provided for a column then the first column which matches the name is chosen.
 *
 * @inheritDoc
 */
public class ProjectionOperator implements Operator, UnaryNode<Operator> {
    private final Operator source;

    private final TableHeader newHeader;
    private final List<Integer> newToOldColumnMapping;

    /**
     * Builds an operator that will project source tuples into the schema defined by the subheader
     *
     * @param newHeader The header of any tuples which are to be generated by this operator.
     *                  The header *must* be a subset of the source operator's header.
     *                  Alias' may be left blank, but headers must be filled.
     * @param source    The child operator for whom the tuples are projected.
     */
    public ProjectionOperator(Operator source, TableHeader newHeader) {
        this.source = source;
        this.newHeader = newHeader.clone();
        this.newToOldColumnMapping = new ArrayList<>();

        for (int i = 0; i < newHeader.size(); i++) {
            String alias = newHeader.columnAliases.get(i);
            String header = newHeader.columnHeaders.get(i);

            Optional<Integer> index = this.source.getHeader().resolve(alias, header);

            List<String> sourceAliases = this.source.getHeader().columnAliases;

            if (index.isPresent()) {
                this.newToOldColumnMapping.add(index.get());
                this.newHeader.columnAliases.set(i, sourceAliases.get(index.get()));
            } else {
                throw new RuntimeException("Projection mappings are incorrect. " + alias + "." + header + " has no match.");
            }
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple = this.source.getNextTuple();

        if (tuple != null) {
            List<Integer> newBackingArray = new ArrayList<>(this.newHeader.size());

            for (int i = 0; i < this.newHeader.size(); i++) {
                newBackingArray.add(tuple.fields.get(this.newToOldColumnMapping.get(i)));
            }

            return (new Tuple(newBackingArray));
        } else {
            return tuple;
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return newHeader;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        return this.source.reset();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getChild() {
        return source;
    }
}
