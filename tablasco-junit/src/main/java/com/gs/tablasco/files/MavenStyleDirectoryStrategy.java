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
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MavenStyleDirectoryStrategy implements DirectoryStrategy
{
    private String expectedSubDir = null;
    private String outputSubDir = null;
    private String anchorFile = "pom.xml";

    /**
     * Use a sub-folder of src/test/resources for actual results
     *
     * @param expectedSubDir - the folder in src/test/resources which contains the actual results
     * @return the same {@code MavenStyleDirectoryStrategy}
     */
    public MavenStyleDirectoryStrategy withExpectedSubDir(String expectedSubDir)
    {
        this.expectedSubDir = expectedSubDir;
        return this;
    }

    /**
     * Use a sub-folder of target for output
     *
     * @param outputSubDir - the folder in target which contains the output
     * @return the same {@code MavenStyleDirectoryStrategy}
     */
    public MavenStyleDirectoryStrategy withOutputSubDir(String outputSubDir)
    {
        this.outputSubDir = outputSubDir;
        return this;
    }

    /**
     * Use a different anchor file if not using maven (e.g. build.gradle)
     *
     * @param anchorFile - the file which tells us we have reached the module base dir.  defaults to pom.xml
     * @return the same {@code MavenStyleDirectoryStrategy}
     */
    public MavenStyleDirectoryStrategy withAnchorFile(String anchorFile)
    {
        assert anchorFile != null : "Anchor file cannot be null (for maven it should be pom.xml, which is the default)";
        this.anchorFile = anchorFile;
        return this;
    }

    @Override
    public File getExpectedDirectory(Class<?> testClass)
    {
        final URL sourceLocation = testClass.getProtectionDomain().getCodeSource().getLocation();
        if (sourceLocation.getProtocol().equalsIgnoreCase("file"))
        {
            try
            {
                Path moduleDir = findModuleDir(Paths.get(sourceLocation.toURI()));
                final Path resourcesFolder = moduleDir.resolve("src").resolve("test").resolve("resources");
                return isEmptyString(this.expectedSubDir) ? resourcesFolder.toFile() : resourcesFolder.resolve(this.expectedSubDir).toFile();
            }
            catch (URISyntaxException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            //e.g. when running from a jar file
            throw new UnsupportedOperationException("Not supported unless running from a file");
        }
    }

    @Override
    public File getOutputDirectory(Class<?> testClass) {
        final URL sourceLocation = testClass.getProtectionDomain().getCodeSource().getLocation();
        if (sourceLocation.getProtocol().equalsIgnoreCase("file"))
        {
            try
            {
                Path moduleDir = findModuleDir(Paths.get(sourceLocation.toURI()));
                final Path outputFolder = isEmptyString(this.outputSubDir) ? moduleDir.resolve("target") : moduleDir.resolve("target").resolve(this.outputSubDir);
                Files.createDirectories(outputFolder);
                return outputFolder.toFile();
            }
            catch (URISyntaxException | IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            //e.g. when running from a jar file
            throw new UnsupportedOperationException("Not supported unless running from a file");
        }
    }

    @Override
    public File getActualDirectory(Class<?> testClass)
    {
        return getOutputDirectory(testClass);
    }

    private boolean isEmptyString(String string)
    {
        return string == null || string.isEmpty();
    }

    private boolean hasAnchorFile(Path path)
    {
        return Files.exists(path.resolve(this.anchorFile));
    }

    private Path findModuleDir(Path path)
    {
        Path tmpPath = path;
        while (tmpPath != null && !hasAnchorFile(tmpPath))
        {
            tmpPath = tmpPath.getParent();
        }
        return tmpPath;
    }
}