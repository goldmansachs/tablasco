/*
 * Copyright 2017 Goldman Sachs.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gs.tablasco;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.gs.tablasco.files.FilePerClassStrategy;
import com.gs.tablasco.results.ExpectedResultsLoader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ExpectedResultsLoaderTest implements ExpectedResultsLoader {
    @RegisterExtension
    private final TableVerifier verifier = new TableVerifier()
            .withMavenDirectoryStrategy()
            .withFileStrategy(new FilePerClassStrategy() {
                @Override
                public String getExpectedFilename(Class<?> testClass, String methodName) {
                    return super.getExpectedFilename(testClass, methodName).replace(".txt", ".raw.txt");
                }
            })
            .withExpectedResultsLoader(this);

    private static final AtomicInteger loadCount = new AtomicInteger();

    @Override
    public InputStream load(File expectedFile) throws IOException {
        loadCount.incrementAndGet();
        File file = new File(expectedFile.getPath().replace(".raw.txt", ".txt"));
        return Files.newInputStream(file.toPath());
    }

    @Test
    void testOne() {
        runTest();
    }

    @Test
    void testTwo() {
        runTest();
    }

    @Test
    void testThree() {
        runTest();
    }

    private void runTest() {
        this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        assertEquals(1, loadCount.intValue(), "Results should be loaded once and used by all three tests");
    }
}
