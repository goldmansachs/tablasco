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

package com.gs.tablasco.verify;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.core.VerifierConfig;
import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies that a set of grids (in form of VerifiedCell) matches expected results. Heavy lifting is delegated to
 * SingleTableVerification class: this one is mainly about pulling the pieces together.
 */
// todo: kp: test whether if i rename test methods I get expected 'surplus' tables error, is that confusing?

public class MultiTableVerifier {
    private final VerifierConfig verifierConfig;

    public MultiTableVerifier(VerifierConfig verifierConfig) {
        this.verifierConfig = verifierConfig;
    }

    public Map<String, ResultTable> verifyTables(
            Map<String, ? extends VerifiableTable> expectedResults,
            Map<String, ? extends VerifiableTable> actualResults) {
        SingleTableVerifier singleTableVerifier = new IndexMapTableVerifier(
                this.verifierConfig.getColumnComparators(),
                this.verifierConfig.isVerifyRowOrder(),
                IndexMapTableVerifier.DEFAULT_BEST_MATCH_THRESHOLD,
                this.verifierConfig.isIgnoreSurplusRows(),
                this.verifierConfig.isIgnoreMissingRows(),
                this.verifierConfig.isIgnoreSurplusColumns(),
                this.verifierConfig.isIgnoreMissingColumns(),
                this.verifierConfig.getPartialMatchTimeoutMillis());
        Map<String, ResultTable> results = new LinkedHashMap<>();
        List<String> allTableNames = new ArrayList<>(expectedResults.keySet());
        for (String actualTable : actualResults.keySet()) {
            if (!expectedResults.containsKey(actualTable)) {
                allTableNames.add(actualTable);
            }
        }
        for (String tableName : allTableNames) {
            verifyTable(tableName, actualResults, expectedResults, results, singleTableVerifier);
        }
        return results;
    }

    private void verifyTable(
            String tableName,
            Map<String, ? extends VerifiableTable> actualResults,
            Map<String, ? extends VerifiableTable> expectedResults,
            Map<String, ResultTable> resultsMap,
            SingleTableVerifier singleTableVerifier) {
        VerifiableTable actualData = actualResults.get(tableName);
        VerifiableTable expectedData = expectedResults.get(tableName);

        if (actualData != null && actualData.getColumnCount() == 0) {
            throw new IllegalStateException("Actual table '" + tableName + "' has no columns");
        }
        if (expectedData != null && expectedData.getColumnCount() == 0) {
            throw new IllegalStateException("Expected table '" + tableName + "' has no columns");
        }
        ResultTable results = singleTableVerifier.verify(actualData, expectedData);
        resultsMap.put(tableName, results);
    }
}
