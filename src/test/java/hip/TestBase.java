/*
 * Copyright 2012 Alex Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
