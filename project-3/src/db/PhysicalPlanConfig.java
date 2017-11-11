package db;

/**
 * A class to store the configuration for the logical to physical tree conversion.
 */
public class PhysicalPlanConfig {

    /**
     * The join type used.
     */
    public enum JoinImplementation {
        TNLJ,
        BNLJ,
        SMJ
    }

    /**
     * The sort type used.
     */
    public enum SortImplementation {
        IN_MEMORY,
        EXTERNAL
    }

    public static final PhysicalPlanConfig DEFAULT_CONFIG = new PhysicalPlanConfig(JoinImplementation.TNLJ, SortImplementation.IN_MEMORY);

    public JoinImplementation joinImplementation;
    public SortImplementation sortImplementation;

    public int joinParameter;
    public int sortParameter;

    public boolean useIndices = false;

    /**
     * @param join the join type
     * @param sort the sort type
     */
    public PhysicalPlanConfig(JoinImplementation join, SortImplementation sort) {
        this.joinImplementation = join;
        this.sortImplementation = sort;
    }

    /**
     * @param join       the join type
     * @param sort       the sort type
     * @param joinParam  the number of blocks used for the join
     * @param sortParam  the number of blocks used for the sort
     * @param useIndices Indicates whether or not indices are used.
     */
    public PhysicalPlanConfig(JoinImplementation join, SortImplementation sort, int joinParam, int sortParam, boolean useIndices) {
        this.joinImplementation = join;
        this.sortImplementation = sort;
        this.joinParameter = joinParam;
        this.sortParameter = sortParam;
        this.useIndices = useIndices;
    }
}
