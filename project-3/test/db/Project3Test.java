package db;

import db.performance.DiskIOStatistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Project3Test {

    @Before
    public void setUp() throws Exception {
        Project3.DUMP_TO_CONSOLE = false;
    }

    @Test
    public void main() throws Exception {
        Project3.main(new String[]{});
    }

    @After
    public void openCloseStats() throws Exception {
        System.out.println("Opened: " + DiskIOStatistics.handles_opened);
        System.out.println("Closed: " + DiskIOStatistics.handles_closed);
    }
}