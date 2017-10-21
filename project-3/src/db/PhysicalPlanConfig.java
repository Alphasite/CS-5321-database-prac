package db;

import java.nio.file.Path;
import java.util.Scanner;

public class PhysicalPlanConfig {

    public enum JoinImplementation {
        TNLJ,
        BNLJ,
        SMJ
    }

    public enum SortImplementation {
        IN_MEMORY,
        EXTERNAL
    }

    public static final PhysicalPlanConfig DEFAULT_CONFIG = new PhysicalPlanConfig(JoinImplementation.TNLJ, SortImplementation.IN_MEMORY);

    public JoinImplementation joinImplementation;
    public SortImplementation sortImplementation;

    public int joinParameter;
    public int sortParameter;

    public PhysicalPlanConfig(JoinImplementation join, SortImplementation sort) {
        this.joinImplementation = join;
        this.sortImplementation = sort;
    }

    public PhysicalPlanConfig(JoinImplementation join, SortImplementation sort, int joinParam, int sortParam) {
        this.joinImplementation = join;
        this.sortImplementation = sort;
        this.joinParameter = joinParam;
        this.sortParameter = sortParam;
    }

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
