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

import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodName.class)
public class SingleTableVerifierMinBestMatchThresholdTest extends AbstractSingleTableVerifierTest {
    @Override
    protected IndexMapTableVerifier createSingleTableVerifier(ColumnComparators columnComparators) {
        return new IndexMapTableVerifier(columnComparators, true, 1, false, false);
    }

    @Override
    protected List<List<ResultCell>> getExpectedVerification(String methodName) {
        return ROW_KEY_VERIFICATIONS.get(methodName);
    }

    static final Map<String, List<List<ResultCell>>> ROW_KEY_VERIFICATIONS;

    static {
        ROW_KEY_VERIFICATIONS = new HashMap<>(SingleTableVerifierMaxBestMatchThresholdTest.MAX_BEST_MATCH_THRESHOLD);
        addVerification(
                row(pass("Col 1"), pass("Col 2"), pass("Col 3")),
                row(pass("A"), pass("A"), fail("0", "1")),
                row(pass("A"), pass("A"), fail("2", "3")),
                row(pass("B"), pass("A"), fail("0", "1")),
                row(pass("C"), pass("B"), fail("0", "1")),
                row(pass("D"), pass("B"), fail("0", "1")),
                row(pass("E"), pass("C"), fail("0", "1")),
                row(surplus("X"), surplus("C"), surplus("0")),
                row(missing("Y"), missing("C"), missing("1")),
                row(surplus("X"), surplus("X"), surplus("0")),
                row(missing("Y"), missing("Y"), missing("1")));
    }

    private static void addVerification(List<?>... rows) {
        List<List<ResultCell>> castRows = Arrays.stream(rows)
                .map(r -> r.stream().map(ResultCell.class::cast).toList())
                .toList();
        ROW_KEY_VERIFICATIONS.put("adaptiveMatcherLeavesLeastUnmatchedRows", castRows);
    }
}
