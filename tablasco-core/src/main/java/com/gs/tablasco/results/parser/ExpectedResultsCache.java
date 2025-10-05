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

package com.gs.tablasco.results.parser;

import com.gs.tablasco.results.ExpectedResults;
import com.gs.tablasco.results.ExpectedResultsLoader;
import java.io.File;
import java.util.Objects;

public class ExpectedResultsCache {
    private static File lastExpectedResultsFile;
    private static ExpectedResults lastExpectedResults;

    public static synchronized ExpectedResults getExpectedResults(
            ExpectedResultsLoader expectedResultsLoader, File expectedResultsFile) {
        if (!Objects.equals(lastExpectedResultsFile, expectedResultsFile)) {
            lastExpectedResultsFile = expectedResultsFile;
            lastExpectedResults = new ExpectedResultsParser(expectedResultsLoader, expectedResultsFile).parse();
        }
        return lastExpectedResults;
    }
}
