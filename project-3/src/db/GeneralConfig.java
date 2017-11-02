package db;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class GeneralConfig {

    // TODO: fail gracefully ? (provide defaults)

    public Path inputDir;
    public Path outputDir;
    public Path tempDir;

    public Path dbPath;

    public boolean buildIndexes;
    public boolean evaluateQueries;

    public static GeneralConfig fromFile(Path configFile) {
        try {
            Scanner scanner = new Scanner(configFile);

            GeneralConfig config = new GeneralConfig();

            config.inputDir = Paths.get(scanner.nextLine());
            config.outputDir = Paths.get(scanner.nextLine());
            config.tempDir = Paths.get(scanner.nextLine());

            config.dbPath = config.dbPath.resolve("db");

            config.buildIndexes = (Integer.parseInt(scanner.nextLine()) == 1);
            config.evaluateQueries = (Integer.parseInt(scanner.nextLine()) == 1);

            return config;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
