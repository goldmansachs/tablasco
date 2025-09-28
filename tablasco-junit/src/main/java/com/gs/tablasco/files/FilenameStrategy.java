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

/**
 * A strategy for determining the expected results and verification output filenames for a given test class.
 */
public interface FilenameStrategy {
    /**
     * Returns the expected results filename for a given test class.
     * @param testClass the test class
     * @param methodName the test method name
     * @return the expected results filename
     */
    String getExpectedFilename(Class<?> testClass, String methodName);

    /**
     * Returns the verification output filename for a given test class.
     * @param testClass the test class
     * @param methodName the test method name
     * @return the verification output filename
     */
    String getOutputFilename(Class<?> testClass, String methodName);

    /**
     * Returns the actual results filename for a given test class.
     * @param testClass the test class
     * @param methodName the test method name
     * @return the verification output filename
     */
    String getActualFilename(Class<?> testClass, String methodName);
}
