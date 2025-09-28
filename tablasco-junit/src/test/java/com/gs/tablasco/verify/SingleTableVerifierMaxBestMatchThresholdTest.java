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

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodName.class)
public class SingleTableVerifierMaxBestMatchThresholdTest extends AbstractSingleTableVerifierTest {
    @Override
    protected IndexMapTableVerifier createSingleTableVerifier(ColumnComparators columnComparators) {
        return new IndexMapTableVerifier(columnComparators, true, Integer.MAX_VALUE, false, false);
    }

    @Override
    protected List<List<ResultCell>> getExpectedVerification(String methodName) {
        return MAX_BEST_MATCH_THRESHOLD.get(methodName);
    }

    static final Map<String, List<List<ResultCell>>> MAX_BEST_MATCH_THRESHOLD;

    static {
        MAX_BEST_MATCH_THRESHOLD = new HashMap<>();
        addVerification("columnHeadersMatch", List.of(pass("Entity"), pass("Account"), pass("Notional")));
        addVerification(
                "columnHeadersAndRowsMatch",
                List.of(pass("Entity"), pass("Account"), pass("Notional")),
                List.of(pass("GSIL"), pass("76543210.01"), pass("1,000,000")));
        addVerification(
                "stringCellMismatch",
                List.of(pass("Entity"), pass("Account"), pass("Notional")),
                List.of(fail("GSE", "GSI"), pass("76543210.01"), pass("1,000,000")));
        addVerification(
                "oneHeaderIsDifferent",
                List.of(pass("Entity"), pass("Account"), surplus("Notional (USD)"), missing("Notional")));
        addVerification(
                "surplusColumnAtEnd",
                List.of(pass("column1"), pass("column2"), pass("column3"), pass("column4"), surplus("column5")));
        addVerification(
                "surplusColumnAtFront",
                List.of(surplus("column1"), pass("column2"), pass("column3"), pass("column4"), pass("column5")));
        addVerification(
                "surplusColumnInMiddle",
                List.of(pass("column1"), pass("column2"), surplus("column3"), pass("column4"), pass("column5")));
        addVerification(
                "twoSurplusColumnsInMiddle",
                List.of(pass("column1"), surplus("column2"), surplus("column3"), pass("column4"), pass("column5")));
        addVerification(
                "surplusColumnWithRowMismatch",
                List.of(pass("Entity"), surplus("Account"), pass("Notional")),
                List.of(fail("GSE", "GSI"), surplus("76543210.01"), pass("1,000,000")));
        addVerification(
                "missingColumnWithRowMismatch",
                List.of(pass("Entity"), missing("Account"), pass("Notional")),
                List.of(fail("GSE", "GSI"), missing("76543210.01"), pass("1,000,000")));
        addVerification(
                "missingColumnAtFront", List.of(missing("column1"), pass("column2"), pass("column3"), pass("column4")));
        addVerification(
                "missingColumnAtEnd", List.of(pass("column1"), pass("column2"), pass("column3"), missing("column4")));
        addVerification(
                "missingColumnInMiddle",
                List.of(pass("column1"), missing("column2"), pass("column3"), pass("column4")));
        addVerification(
                "twoMissingColumnsInMiddle",
                List.of(pass("column1"), missing("column2"), missing("column3"), pass("column4")));
        addVerification(
                "multipleSurplusAndMissingColumns",
                List.of(
                        pass("column1"),
                        missing("column2"),
                        missing("column3"),
                        pass("column4"),
                        surplus("column5"),
                        missing("column6"),
                        pass("column7"),
                        surplus("column8")));
        addVerification(
                "numericMismatchesWithTolerance",
                List.of(pass("column1"), pass("column2"), pass("column3"), pass("column4"), pass("column5")),
                List.of(pass("c1--"), fail("19,843", "19,853"), fail("100,005", "100,002"), pass("101"), pass("100")));
        addVerification(
                "surplusRowAtBottom",
                List.of(pass("column1"), pass("column2"), pass("column3")),
                List.of(pass("--a--"), pass("--b--"), pass("--c--")),
                List.of(surplus("--d--"), surplus("--e--"), surplus("--f--")));
        addVerification(
                "missingRowAtBottom",
                List.of(pass("column1"), pass("column2"), pass("column3")),
                List.of(pass("--a--"), pass("--b--"), pass("--c--")),
                List.of(missing("--d--"), missing("--e--"), missing("--f--")));
        addVerification(
                "unmatchedRowsWithNans",
                List.of(pass("column1"), pass("column2"), pass("column3")),
                List.of(outOfOrder("c1--"), outOfOrder(Double.toString(Double.NaN)), outOfOrder("100")),
                List.of(pass("c1--"), pass("65"), pass("100")));
        addVerification(
                "singleColumnMissingActualInMiddle",
                List.of(pass("Single")),
                List.of(pass("10")),
                List.of(pass("20")),
                List.of(missing("30")),
                List.of(pass("40")));
        addVerification(
                "singleColumnMissingActualAtEnd",
                List.of(pass("Single")),
                List.of(pass("10")),
                List.of(pass("20")),
                List.of(pass("30")),
                List.of(missing("40")));
        addVerification(
                "singleColumnExtraActual",
                List.of(pass("Single")),
                List.of(pass("10")),
                List.of(pass("20")),
                List.of(surplus("25")),
                List.of(pass("30")),
                List.of(pass("40")));
        addVerification(
                "singleColumnOutOfOrder",
                List.of(pass("Single")),
                List.of(pass("10")),
                List.of(outOfOrder("30")),
                List.of(pass("20")),
                List.of(pass("40")));
        addVerification(
                "matchFirstDuplicateRowAndShowSecondAsSurplus",
                List.of(pass("Entity"), pass("Account"), pass("Notional")),
                List.of(pass("GSI"), pass("76543210.01"), pass("1,000,000")),
                List.of(surplus("GSI"), surplus("76543210.01"), surplus("1,000,000")));
        addVerification(
                "matchFirstDuplicateRowAndShowSecondAsMissing",
                List.of(pass("Entity"), pass("Account"), pass("Notional")),
                List.of(pass("GSI"), pass("76543210.01"), pass("1,000,000")),
                List.of(missing("GSI"), missing("76543210.01"), missing("1,000,000")));
        addVerification(
                "matchFirstPartialRowAndShowSecondAsSurplus",
                List.of(pass("Entity"), pass("Account"), pass("Notional")),
                List.of(fail("GSE", "GSI"), pass("76543210.01"), pass("1,000,000")),
                List.of(surplus("GSU"), surplus("76543210.01"), surplus("1,000,000")));
        addVerification(
                "matchFirstPartialRowAndShowSecondAsMissing",
                List.of(pass("Entity"), pass("Account"), pass("Notional")),
                List.of(fail("GSU", "GSI"), pass("76543210.01"), pass("1,000,000")),
                List.of(missing("GSE"), missing("76543210.01"), missing("1,000,000")));
        addVerification(
                "matchBestPartialRowAndShowSecondAsSurplus",
                List.of(pass("Entity"), pass("Account"), pass("Notional")),
                List.of(surplus("GSE"), surplus("76543210.99"), surplus("1,000,000")),
                List.of(fail("GSU", "GSI"), pass("76543210.01"), pass("1,000,000")));
        addVerification(
                "matchBestPartialRowAndShowSecondAsMissing",
                List.of(pass("Entity"), pass("Account"), pass("Notional")),
                List.of(missing("GSI"), missing("76543210.99"), missing("1,000,000")),
                List.of(fail("GSU", "GSE"), pass("76543210.01"), pass("1,000,000")));
        addVerification(
                "stringColumnAtEndIsTreatedAsAnyOtherValueWouldBe",
                List.of(pass("Entity"), pass("Account"), pass("Notional"), pass("Strings")),
                List.of(pass("GSI"), pass("76543210.01"), pass("1,000,000"), fail("a diff string", "a string")));
        addVerification(
                "outOfOrderColumns2",
                List.of(outOfOrder("column4"), pass("column1"), outOfOrder("column3"), pass("column2")));
        addVerification(
                "outOfOrderColumns3",
                List.of(outOfOrder("column4"), pass("column1"), pass("column2"), pass("column3")));
        addVerification("outOfOrderColumns4", List.of(outOfOrder("column3"), outOfOrder("column2"), pass("column1")));
        addVerification(
                "outOfOrderColumns", List.of(pass("column1"), pass("column2"), outOfOrder("column4"), pass("column3")));
        addVerification(
                "surplusRowAtStart",
                List.of(pass("column1"), pass("column2"), pass("column3")),
                List.of(surplus("--d--"), surplus("--e--"), surplus("--f--")),
                List.of(pass("--a--"), pass("--b--"), pass("--c--")));
        addVerification(
                "missingRowAtStart",
                List.of(pass("column1"), pass("column2"), pass("column3")),
                List.of(missing("--d--"), missing("--e--"), missing("--f--")),
                List.of(pass("--a--"), pass("--b--"), pass("--c--")));
        addVerification(
                "proximityIsIncludedInMatch",
                List.of(pass("Key 1"), pass("Key 2"), pass("Bal 1"), pass("Bal 2"), pass("Bal 3")),
                List.of(pass("A"), pass("A"), fail("10", "1"), fail("20", "2"), pass("3")),
                List.of(pass("A"), pass("B"), pass("4"), pass("5"), pass("6")),
                List.of(pass("A"), pass("C"), pass("7"), pass("8"), pass("9")),
                List.of(pass("B"), pass("A"), fail("1", "10"), fail("2", "20"), fail("3", "30")));
        addVerification(
                "proximityIsIncludedInMatch2",
                List.of(pass("Key 1"), pass("Bal 1"), pass("Bal 2"), pass("Bal 3")),
                List.of(pass("A"), fail("3", "1"), pass("0"), pass("0")),
                List.of(surplus("B"), surplus("4"), surplus("0"), surplus("0")),
                List.of(surplus("C"), surplus("5"), surplus("0"), surplus("0")),
                List.of(surplus("D"), surplus("6"), surplus("0"), surplus("0")),
                List.of(pass("E"), fail("7", "2"), pass("0"), pass("0")));
        addVerification(
                "surplusRowInMiddle",
                List.of(pass("column1"), pass("column2"), pass("column3")),
                List.of(pass("--a--"), pass("--b--"), pass("--c--")),
                List.of(surplus("--d--"), surplus("--e--"), surplus("--f--")),
                List.of(pass("--g--"), pass("--h--"), pass("--i--")));
        addVerification(
                "missingRowInMiddle",
                List.of(pass("column1"), pass("column2"), pass("column3")),
                List.of(pass("--a--"), pass("--b--"), pass("--c--")),
                List.of(missing("--d--"), missing("--e--"), missing("--f--")),
                List.of(pass("--g--"), pass("--h--"), pass("--i--")));
        addVerification(
                "brokenRowInMiddle",
                List.of(pass("column1"), pass("column2"), pass("column3")),
                List.of(pass("--a--"), pass("--b--"), pass("--c--")),
                List.of(pass("--d--"), pass("--e--"), fail("--x--", "--f--")),
                List.of(pass("--g--"), pass("--h--"), pass("--i--")),
                List.of(pass("--j--"), pass("--k--"), pass("--l--")));
        addVerification(
                "adaptiveMatcherLeavesLeastUnmatchedRows",
                List.of(pass("Col 1"), pass("Col 2"), pass("Col 3")),
                List.of(pass("A"), pass("A"), fail("0", "1")),
                List.of(pass("A"), pass("A"), fail("2", "3")),
                List.of(pass("B"), pass("A"), fail("0", "1")),
                List.of(pass("C"), pass("B"), fail("0", "1")),
                List.of(pass("D"), pass("B"), fail("0", "1")),
                List.of(pass("E"), pass("C"), fail("0", "1")),
                List.of(fail("X", "Y"), pass("C"), fail("0", "1")),
                List.of(surplus("X"), surplus("X"), surplus("0")),
                List.of(missing("Y"), missing("Y"), missing("1")));
        addVerification(
                "duplicateActualRow",
                List.of(pass("Col 1"), pass("Col 2")),
                List.of(pass("A"), fail("B", "A")),
                List.of(pass("C"), pass("D")),
                List.of(surplus("C"), surplus("D")),
                List.of(pass("E"), fail("F", "E")));

        addVerification(
                "duplicateExpectedRow",
                List.of(pass("Col 1"), pass("Col 2")),
                List.of(pass("A"), fail("B", "A")),
                List.of(pass("C"), pass("D")),
                List.of(missing("C"), missing("D")),
                List.of(pass("E"), fail("F", "E")));
        addVerification(
                "duplicateActualColumn",
                List.of(pass("Col 1"), pass("Col 2"), surplus("Col 2")),
                List.of(pass("A"), pass("B"), surplus("B")));
        addVerification(
                "duplicateExpectedColumn",
                List.of(pass("Col 1"), pass("Col 2"), missing("Col 2")),
                List.of(pass("A"), pass("B"), missing("B")));
        addVerification(
                "duplicateColumnName",
                List.of(pass("Col 1"), pass("Col 2"), pass("Col 2")),
                List.of(pass("A"), pass("B"), pass("C")));
        addVerification(
                "keyColumn",
                List.of(pass("Col 1"), pass("Col 2"), pass("Col 3"), pass("Col 4"), pass("Col 5")),
                List.of(surplus("A"), surplus("B"), surplus("C"), surplus("D"), surplus("E")),
                List.of(missing("Z"), missing("B"), missing("C"), missing("D"), missing("E")),
                List.of(surplus("F"), surplus("G"), surplus("H"), surplus("I"), surplus("J")),
                List.of(missing("F"), missing("G"), missing("Z"), missing("I"), missing("J")),
                List.of(pass("K"), fail("L", "Z"), pass("M"), pass("N"), pass("O")));
        addVerification(
                "keyColumnWithDupes",
                List.of(pass("Col 1"), pass("Col 2"), pass("Col 3")),
                List.of(pass("A"), fail("A", "Z"), pass("K1")),
                List.of(pass("B"), fail("B", "Z"), pass("K1")),
                List.of(surplus("C"), surplus("C"), surplus("K1")),
                List.of(pass("A"), fail("Z", "A"), pass("K2")),
                List.of(pass("B"), fail("Z", "B"), pass("K2")),
                List.of(missing("C"), missing("C"), missing("K2")),
                List.of(surplus("A"), surplus("A"), surplus("K3")),
                List.of(missing("A"), missing("A"), missing("K4")));
        addVerification(
                "keyColumnWithOutOfOrderMissingAndSurplusColumns",
                List.of(missing("Col 4"), outOfOrder("Col 1"), surplus("Col 2"), pass("Col 3")),
                List.of(missing("Z"), outOfOrder("A"), surplus("A"), pass("K1")),
                List.of(missing("Z"), outOfOrder("B"), surplus("B"), pass("K1")),
                List.of(surplus(""), surplus("C"), surplus("C"), surplus("K1")),
                List.of(missing("A"), outOfOrder("A"), surplus("Z"), pass("K2")),
                List.of(missing("B"), outOfOrder("B"), surplus("Z"), pass("K2")),
                List.of(missing("C"), missing("C"), missing(""), missing("K2")));
        addVerification(
                "keyColumnWithMissingKey",
                List.of(surplus("Col 1"), pass("Col 2"), pass("Col 3")),
                List.of(surplus("A"), fail("A", "Z"), pass("K1")),
                List.of(surplus("B"), fail("B", "Z"), pass("K1")),
                List.of(surplus("A"), fail("Z", "A"), pass("K2")),
                List.of(surplus("B"), fail("Z", "B"), pass("K2")));
    }

    private static void addVerification(String testName, List<?>... rows) {
        assertFalse(MAX_BEST_MATCH_THRESHOLD.containsKey(testName));
        List<List<ResultCell>> castRows = Arrays.stream(rows)
                .map(r -> r.stream().map(ResultCell.class::cast).toList())
                .toList();
        MAX_BEST_MATCH_THRESHOLD.put(testName, castRows);
    }
}
