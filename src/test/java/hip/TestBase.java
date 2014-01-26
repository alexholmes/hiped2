package hip;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

public class TestBase {

    public static final File TEST_ROOT_DIR =
            new File(System.getProperty("test.build.data", "/tmp/hip-test"));

    @Before
    public void before() throws IOException {
        if(TEST_ROOT_DIR.exists()) {
            FileUtils.forceDelete(TEST_ROOT_DIR);
        }
        FileUtils.forceMkdir(TEST_ROOT_DIR);
    }

    @After
    public void after() throws IOException {
        if(TEST_ROOT_DIR.exists()) {
            FileUtils.forceDelete(TEST_ROOT_DIR);
        }
    }
}
