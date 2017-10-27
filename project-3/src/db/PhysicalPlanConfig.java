package db;

import java.nio.file.Path;
import java.util.Scanner;

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

    /**
     * @param join the join type
     * @param sort the sort type
     */
    public PhysicalPlanConfig(JoinImplementation join, SortImplementation sort) {
        this.joinImplementation = join;
        this.sortImplementation = sort;
    }

    /**
     * @param join      the join type
     * @param sort      the sort type
     * @param joinParam the number of blocks used for the join
     * @param sortParam the number of blocks used for the sort
     */
    public PhysicalPlanConfig(JoinImplementation join, SortImplementation sort, int joinParam, int sortParam) {
        this.joinImplementation = join;
        this.sortImplementation = sort;
        this.joinParameter = joinParam;
        this.sortParameter = sortParam;
    }

    /**
     * Build a config by parsing the provided file.
     * @param configFile the file path for the config file.
     * @return the plan.
     */
    public static PhysicalPlanConfig fromFile(Path configFile) {
        try {
            Scanner scanner = new Scanner(configFile);
            String[] joinParams = scanner.nextLine().split(" ");
            String[] sortParams = scanner.nextLine().split(" ");

            JoinImplementation join = JoinImplementation.values()[Integer.parseInt(joinParams[0])];
            SortImplementation sort = SortImplementation.values()[Integer.parseInt(sortParams[0])];

            int joinBufferPages = 0;
            if (join == JoinImplementation.BNLJ) {
                joinBufferPages = Integer.parseInt(joinParams[1]);
            }

            int sortBufferPages = 0;
            if (sort == SortImplementation.EXTERNAL) {
                sortBufferPages = Integer.parseInt(sortParams[1]);
            }

            return new PhysicalPlanConfig(join, sort, joinBufferPages, sortBufferPages);
        } catch (Exception e) {
            e.printStackTrace();
            return DEFAULT_CONFIG;
        }
    }
}
