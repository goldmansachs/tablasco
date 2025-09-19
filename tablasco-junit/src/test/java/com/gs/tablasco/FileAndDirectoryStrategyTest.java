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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gs.tablasco.files.DirectoryStrategy;
import com.gs.tablasco.files.FilePerClassStrategy;
import java.io.File;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FileAndDirectoryStrategyTest {
    private final TableVerifier verifier = new TableVerifier()
            .withDirectoryStrategy(new DirectoryStrategy() {
                @Override
                public File getExpectedDirectory(Class<?> testClass) {
                    return TableTestUtils.getExpectedDirectory();
                }

                @Override
                public File getOutputDirectory(Class<?> testClass) {
                    return TableTestUtils.getOutputDirectory();
                }

                @Override
                public File getActualDirectory(Class<?> testClass) {
                    return new File(TableTestUtils.getOutputDirectory(), "actual");
                }
            })
            .withFileStrategy(new FilePerClassStrategy() {
                @Override
                public String getExpectedFilename(Class<?> testClass, String methodName) {
                    return "Custom" + super.getExpectedFilename(testClass, methodName);
                }

                @Override
                public String getOutputFilename(Class<?> testClass, String methodName) {
                    return "Custom" + super.getOutputFilename(testClass, methodName);
                }

                @Override
                public String getActualFilename(Class<?> testClass, String methodName) {
                    return "Custom" + super.getActualFilename(testClass, methodName);
                }
            });

    @Rule
    public final TableTestUtils.TestDescription description = new TableTestUtils.TestDescription();

    @BeforeEach
    void setUp() {
        this.verifier.starting(this.description.get());
        this.verifier.getActualFile().delete();
    }

    @Test
    void testFiles() {
        this.verifier.verify(TableTestUtils.TABLE_NAME, TableTestUtils.ACTUAL);
        this.verifier.succeeded(this.description.get());
        assertTrue(new File(
                        new File(TableTestUtils.getOutputDirectory().getPath(), "actual"),
                        "CustomFileAndDirectoryStrategyTest.txt")
                .exists());
        assertTrue(
                new File(TableTestUtils.getOutputDirectory().getPath(), "CustomFileAndDirectoryStrategyTest.html")
                        .exists());
    }
}
