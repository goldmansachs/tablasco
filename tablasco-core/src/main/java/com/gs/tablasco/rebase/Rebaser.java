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

package com.gs.tablasco.rebase;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.core.VerifierConfig;
import com.gs.tablasco.verify.Metadata;
import java.io.File;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rebaser {
    private static final Logger LOGGER = LoggerFactory.getLogger(Rebaser.class);
    private static Boolean rebaseMode;
    private final VerifierConfig verifierConfig;
    private final Metadata metadata;
    private final String[] baselineHeaders;

    public static boolean inRebaseMode() {
        if (rebaseMode == null) {
            rebaseMode = initializeRebaseFlag();
        }
        return rebaseMode;
    }

    public Rebaser(VerifierConfig verifierConfig, Metadata metadata, String[] baselineHeaders) {
        this.verifierConfig = verifierConfig;
        this.metadata = metadata;
        this.baselineHeaders = baselineHeaders;
    }

    public void rebase(String methodName, Map<String, VerifiableTable> actualResults, File outputFile) {
        LOGGER.info("Actual results for {} to {}", methodName, outputFile.getAbsolutePath());
        new RebaseFileWriter(
                        this.metadata, this.baselineHeaders, this.verifierConfig.getColumnComparators(), outputFile)
                .writeRebasedResults(methodName, actualResults);
    }

    private static Boolean initializeRebaseFlag() {
        return Boolean.valueOf(System.getProperty("rebase", "false"));
    }
}
