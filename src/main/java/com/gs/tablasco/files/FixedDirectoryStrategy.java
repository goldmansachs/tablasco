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

package com.gs.tablasco.files;

import java.io.File;

public class FixedDirectoryStrategy implements DirectoryStrategy
{
    private final File expectedDir;
    private final File outputDir;

    public FixedDirectoryStrategy(File expectedDir, File outputDir)
    {
        this.expectedDir = expectedDir;
        this.outputDir = outputDir;
    }

    @Override
    public File getExpectedDirectory(Class<?> testClass)
    {
        return this.expectedDir;
    }

    @Override
    public File getOutputDirectory(Class<?> testClass)
    {
        return this.outputDir;
    }

    @Override
    public File getActualDirectory(Class<?> testClass)
    {
        return this.outputDir;
    }
}
