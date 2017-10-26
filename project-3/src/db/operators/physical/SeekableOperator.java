package db.operators.physical;

/**
 * Indicates that the operater can have its buffer seek'ed upon.
 */
public interface SeekableOperator extends Operator {
    /**
     * Move the read pointer to point to this tuple index next.
     *
     * @param index The index of the tuple.
     */
    void seek(long index);
}
