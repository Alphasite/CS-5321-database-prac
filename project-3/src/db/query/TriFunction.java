package db.query;

/**
 * A functional interface for functions with 3 parameters and a result.
 *
 * @param <A> first param
 * @param <B> second param
 * @param <C> third param
 * @param <R> return type
 */
@FunctionalInterface
public interface TriFunction<A, B, C, R> {
    /**
     * Evaluate the expression
     * @param a first param
     * @param b second param
     * @param c third param
     * @return the return for the expression
     */
    R apply(A a, B b, C c);
}
