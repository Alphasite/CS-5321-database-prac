package db;

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
}