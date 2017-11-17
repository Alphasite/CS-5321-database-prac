package db.Utilities;

/**
 * A pair class to encapsulate a pair of items.
 * <p>
 * Implements {@link #equals(Object)} and {@link #hashCode()} to allow use as a key in hash tables.
 */
public class Pair<K, V> {
    private final K left;
    private final V right;

    /**
     * The left and right children.
     *
     * @param left  the left child
     * @param right the right child
     */
    public Pair(K left, V right) {
        this.left = left;
        this.right = right;
    }

    /**
     * @return the left child
     */
    public K getLeft() {
        return left;
    }


    /**
     * @return the right child
     */
    public V getRight() {
        return right;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Pair)) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if (!left.equals(pair.left)) return false;
        return right.equals(pair.right);
    }

    @Override
    public int hashCode() {
        int result = left.hashCode();
        result = 31 * result + right.hashCode();
        return result;
    }
}
