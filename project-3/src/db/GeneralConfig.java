package db;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

/**
 * A config class which contains the in/out/tmp directories and whether or not to build indices, stats, queries, etc.
 */
public class GeneralConfig {

    private static final Path INPUT_PATH = Paths.get("resources/samples-4/input");
    private static final Path OUTPUT_PATH = Paths.get("resources/samples-4/output");
    private static final Path TEMP_PATH = Paths.get("resources/samples-4/tmp");

    public static final GeneralConfig DEFAULT_CONFIG = new GeneralConfig(INPUT_PATH, OUTPUT_PATH, TEMP_PATH);

    public Path inputDir;
    public Path outputDir;
    public Path tempDir;

    public Path dbPath;

    public boolean buildIndexes;
    public boolean gatherStats;
    public boolean evaluateQueries;

    public GeneralConfig(Path inputDir, Path outputDir, Path tempDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.tempDir = tempDir;

        this.dbPath = inputDir.resolve("db");

        this.buildIndexes = false;
        this.gatherStats = true;
        this.evaluateQueries = true;
    }

    /**
     * Initialise the config from the config file.
     *
     * @param configFile the config file to read.
     * @return the parsed config file.
     */
    public static GeneralConfig fromFile(Path configFile) {
        try (Scanner scanner = new Scanner(configFile)) {

            Path inputDir = Paths.get(scanner.nextLine());
            Path outputDir = Paths.get(scanner.nextLine());
            Path tempDir = Paths.get(scanner.nextLine());

            GeneralConfig config = new GeneralConfig(inputDir, outputDir, tempDir);
            config.buildIndexes = true;
            config.gatherStats = true;
            config.evaluateQueries = true;

            return config;
        } catch (IOException e) {
            e.printStackTrace();
            return DEFAULT_CONFIG;
        }
    }
}
