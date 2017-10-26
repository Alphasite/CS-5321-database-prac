package db.operators.physical;

public interface SeekableOperator extends Operator {
    void seek(long index);

}
