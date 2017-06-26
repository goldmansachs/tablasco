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
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SingleTableVerifierMaxBestMatchThresholdTest extends AbstractSingleTableVerifierTest
{
    @Override
    protected IndexMapTableVerifier createSingleTableVerifier()
    {
        return new IndexMapTableVerifier(true, Integer.MAX_VALUE, false, false);
    }

    @Override
    protected List<List<ResultCell>> getExpectedVerification(String methodName)
    {
        return MAX_BEST_MATCH_THRESHOLD.get(methodName);
    }
    
    static final Map<String, List<List<ResultCell>>> MAX_BEST_MATCH_THRESHOLD;
    static
    {
        MAX_BEST_MATCH_THRESHOLD = UnifiedMap.newMap();
        addVerification("columnHeadersMatch",
                row(pass("Entity"), pass("Account"), pass("Notional"))
                );
        addVerification("columnHeadersAndRowsMatch",
                row(pass("Entity"), pass("Account"), pass("Notional")),
                row(pass("GSIL"), pass("76543210.01"), pass("1,000,000"))
                );
        addVerification("stringCellMismatch",
                row(pass("Entity"), pass("Account"), pass("Notional")),
                row(fail("GSE", "GSI"), pass("76543210.01"), pass("1,000,000"))
                );
        addVerification("oneHeaderIsDifferent",
                row(pass("Entity"), pass("Account"), surplus("Notional (USD)"), missing("Notional"))
                );
        addVerification("surplusColumnAtEnd",
                row(pass("column1"), pass("column2"), pass("column3"), pass("column4"), surplus("column5"))
                );
        addVerification("surplusColumnAtFront",
                row(surplus("column1"), pass("column2"), pass("column3"), pass("column4"), pass("column5"))
                );
        addVerification("surplusColumnInMiddle",
                row(pass("column1"), pass("column2"), surplus("column3"), pass("column4"), pass("column5"))
                );
        addVerification("twoSurplusColumnsInMiddle",
                row(pass("column1"), surplus("column2"), surplus("column3"), pass("column4"), pass("column5"))
                );
        addVerification("surplusColumnWithRowMismatch",
                row(pass("Entity"), surplus("Account"), pass("Notional")),
                row(fail("GSE", "GSI"), surplus("76543210.01"), pass("1,000,000"))
                );
        addVerification("missingColumnWithRowMismatch",
                row(pass("Entity"), missing("Account"), pass("Notional")),
                row(fail("GSE", "GSI"), missing("76543210.01"), pass("1,000,000"))
                );
        addVerification("missingColumnAtFront",
                row(missing("column1"), pass("column2"), pass("column3"), pass("column4"))
                );
        addVerification("missingColumnAtEnd",
                row(pass("column1"), pass("column2"), pass("column3"), missing("column4"))
                );
        addVerification("missingColumnInMiddle",
                row(pass("column1"), missing("column2"), pass("column3"), pass("column4"))
                );
        addVerification("twoMissingColumnsInMiddle",
                row(pass("column1"), missing("column2"), missing("column3"), pass("column4"))
                );
        addVerification("multipleSurplusAndMissingColumns",
                row(pass("column1"), missing("column2"), missing("column3"), pass("column4"), surplus("column5"), missing("column6"), pass("column7"), surplus("column8"))
                );
        addVerification("numericMismatchesWithTolerance",
                row(pass("column1"), pass("column2"), pass("column3"), pass("column4"), pass("column5")),
                row(pass("c1--"), fail("19,843", "19,853"), fail("100,005", "100,002"), pass("101"), pass("100"))
                );
        addVerification("surplusRowAtBottom",
                row(pass("column1"), pass("column2"), pass("column3")),
                row(pass("--a--"), pass("--b--"), pass("--c--")),
                row(surplus("--d--"), surplus("--e--"), surplus("--f--"))
                );
        addVerification("missingRowAtBottom",
                row(pass("column1"), pass("column2"), pass("column3")),
                row(pass("--a--"), pass("--b--"), pass("--c--")),
                row(missing("--d--"), missing("--e--"), missing("--f--"))
                );
        addVerification("unmatchedRowsWithNans",
                row(pass("column1"), pass("column2"), pass("column3")),
                row(outOfOrder("c1--"), outOfOrder(Double.toString(Double.NaN)), outOfOrder("100")),
                row(pass("c1--"), pass("65"), pass("100"))
                );
        addVerification("singleColumnMissingActualInMiddle",
                row(pass("Single")),
                row(pass("10")),
                row(pass("20")),
                row(missing("30")),
                row(pass("40"))
                );
        addVerification("singleColumnMissingActualAtEnd",
                row(pass("Single")),
                row(pass("10")),
                row(pass("20")),
                row(pass("30")),
                row(missing("40"))
                );
        addVerification("singleColumnExtraActual",
                row(pass("Single")),
                row(pass("10")),
                row(pass("20")),
                row(surplus("25")),
                row(pass("30")),
                row(pass("40"))
                );
        addVerification("singleColumnOutOfOrder",
                row(pass("Single")),
                row(pass("10")),
                row(outOfOrder("30")),
                row(pass("20")),
                row(pass("40"))
                );
        addVerification("matchFirstDuplicateRowAndShowSecondAsSurplus",
                row(pass("Entity"), pass("Account"), pass("Notional")),
                row(pass("GSI"), pass("76543210.01"), pass("1,000,000")),
                row(surplus("GSI"), surplus("76543210.01"), surplus("1,000,000"))
                );
        addVerification("matchFirstDuplicateRowAndShowSecondAsMissing",
                row(pass("Entity"), pass("Account"), pass("Notional")),
                row(pass("GSI"), pass("76543210.01"), pass("1,000,000")),
                row(missing("GSI"), missing("76543210.01"), missing("1,000,000"))
                );
        addVerification("matchFirstPartialRowAndShowSecondAsSurplus",
                row(pass("Entity"), pass("Account"), pass("Notional")),
                row(fail("GSE", "GSI"), pass("76543210.01"), pass("1,000,000")),
                row(surplus("GSU"), surplus("76543210.01"), surplus("1,000,000"))
                );
        addVerification("matchFirstPartialRowAndShowSecondAsMissing",
                row(pass("Entity"), pass("Account"), pass("Notional")),
                row(fail("GSU", "GSI"), pass("76543210.01"), pass("1,000,000")),
                row(missing("GSE"), missing("76543210.01"), missing("1,000,000"))
                );
        addVerification("matchBestPartialRowAndShowSecondAsSurplus",
                row(pass("Entity"), pass("Account"), pass("Notional")),
                row(surplus("GSE"), surplus("76543210.99"), surplus("1,000,000")),
                row(fail("GSU", "GSI"), pass("76543210.01"), pass("1,000,000"))
                );
        addVerification("matchBestPartialRowAndShowSecondAsMissing",
                row(pass("Entity"), pass("Account"), pass("Notional")),
                row(missing("GSI"), missing("76543210.99"), missing("1,000,000")),
                row(fail("GSU", "GSE"), pass("76543210.01"), pass("1,000,000"))
                );
        addVerification("stringColumnAtEndIsTreatedAsAnyOtherValueWouldBe",
                row(pass("Entity"), pass("Account"), pass("Notional"), pass("Strings")),
                row(pass("GSI"), pass("76543210.01"), pass("1,000,000"), fail("a diff string", "a string"))
                );
        addVerification("outOfOrderColumns2",
                row(outOfOrder("column4"), pass("column1"), outOfOrder("column3"), pass("column2"))
                );
        addVerification("outOfOrderColumns3",
                row(outOfOrder("column4"), pass("column1"), pass("column2"), pass("column3"))
                );
        addVerification("outOfOrderColumns4",
                row(outOfOrder("column3"), outOfOrder("column2"), pass("column1"))
                );
        addVerification("outOfOrderColumns",
                row(pass("column1"), pass("column2"), outOfOrder("column4"), pass("column3"))
                );
        addVerification("surplusRowAtStart",
                row(pass("column1"), pass("column2"), pass("column3")),
                row(surplus("--d--"), surplus("--e--"), surplus("--f--")),
                row(pass("--a--"), pass("--b--"), pass("--c--"))
                );
        addVerification("missingRowAtStart",
                row(pass("column1"), pass("column2"), pass("column3")),
                row(missing("--d--"), missing("--e--"), missing("--f--")),
                row(pass("--a--"), pass("--b--"), pass("--c--"))
                );
        addVerification("proximityIsIncludedInMatch",
                row(pass("Key 1"), pass("Key 2"), pass("Bal 1"), pass("Bal 2"), pass("Bal 3")),
                row(pass("A"), pass("A"), fail("10", "1"), fail("20", "2"), pass("3")),
                row(pass("A"), pass("B"), pass("4"), pass("5"), pass("6")),
                row(pass("A"), pass("C"), pass("7"), pass("8"), pass("9")),
                row(pass("B"), pass("A"), fail("1", "10"), fail("2", "20"), fail("3", "30"))
                );
        addVerification("proximityIsIncludedInMatch2",
                row(pass("Key 1"), pass("Bal 1"), pass("Bal 2"), pass("Bal 3")),
                row(pass("A"), fail("3", "1"), pass("0"), pass("0")),
                row(surplus("B"), surplus("4"), surplus("0"), surplus("0")),
                row(surplus("C"), surplus("5"), surplus("0"), surplus("0")),
                row(surplus("D"), surplus("6"), surplus("0"), surplus("0")),
                row(pass("E"), fail("7", "2"), pass("0"), pass("0"))
                );
        addVerification("surplusRowInMiddle",
                row(pass("column1"), pass("column2"), pass("column3")),
                row(pass("--a--"), pass("--b--"), pass("--c--")),
                row(surplus("--d--"), surplus("--e--"), surplus("--f--")),
                row(pass("--g--"), pass("--h--"), pass("--i--"))
                );
        addVerification("missingRowInMiddle",
                row(pass("column1"), pass("column2"), pass("column3")),
                row(pass("--a--"), pass("--b--"), pass("--c--")),
                row(missing("--d--"), missing("--e--"), missing("--f--")),
                row(pass("--g--"), pass("--h--"), pass("--i--"))
                );
        addVerification("brokenRowInMiddle",
                row(pass("column1"), pass("column2"), pass("column3")),
                row(pass("--a--"), pass("--b--"), pass("--c--")),
                row(pass("--d--"), pass("--e--"), fail("--x--", "--f--")),
                row(pass("--g--"), pass("--h--"), pass("--i--")),
                row(pass("--j--"), pass("--k--"), pass("--l--"))
                );
        addVerification("adaptiveMatcherLeavesLeastUnmatchedRows",
                row(pass("Col 1"), pass("Col 2"), pass("Col 3")),
                row(pass("A"), pass("A"), fail("0", "1")),
                row(pass("A"), pass("A"), fail("2", "3")),
                row(pass("B"), pass("A"), fail("0", "1")),
                row(pass("C"), pass("B"), fail("0", "1")),
                row(pass("D"), pass("B"), fail("0", "1")),
                row(pass("E"), pass("C"), fail("0", "1")),
                row(fail("X", "Y"), pass("C"), fail("0", "1")),
                row(surplus("X"), surplus("X"), surplus("0")),
                row(missing("Y"), missing("Y"), missing("1"))
                );
        addVerification("duplicateActualRow",
                row(pass("Col 1"),pass("Col 2")),
                row(pass("A"),fail("B","A")),
                row(pass("C"),pass("D")),
                row(surplus("C"),surplus("D")),
                row(pass("E"),fail("F","E"))
                
                );
        addVerification("duplicateExpectedRow",
                row(pass("Col 1"),pass("Col 2")),
                row(pass("A"),fail("B","A")),
                row(pass("C"),pass("D")),
                row(missing("C"),missing("D")),
                row(pass("E"),fail("F","E"))
                );
        addVerification("duplicateActualColumn",
                row(pass("Col 1"),pass("Col 2"),surplus("Col 2")),
                row(pass("A"),pass("B"),surplus("B"))
                );
        addVerification("duplicateExpectedColumn",
                row(pass("Col 1"),pass("Col 2"),missing("Col 2")),
                row(pass("A"),pass("B"),missing("B"))
                );
        addVerification("duplicateColumnName",
                row(pass("Col 1"),pass("Col 2"),pass("Col 2")),
                row(pass("A"),pass("B"),pass("C"))
        );
        addVerification("keyColumn",
                row(pass("Col 1"), pass("Col 2"), pass("Col 3"), pass("Col 4"), pass("Col 5")),
                row(surplus("A"), surplus("B"), surplus("C"), surplus("D"), surplus("E")),
                row(missing("Z"), missing("B"), missing("C"), missing("D"), missing("E")),
                row(surplus("F"), surplus("G"), surplus("H"), surplus("I"), surplus("J")),
                row(missing("F"), missing("G"), missing("Z"), missing("I"), missing("J")),
                row(pass("K"), fail("L", "Z"), pass("M"), pass("N"), pass("O"))
        );
        addVerification("keyColumnWithDupes",
                row(pass("Col 1"), pass("Col 2"), pass("Col 3")),
                row(pass("A"), fail("A", "Z"), pass("K1")),
                row(pass("B"), fail("B", "Z"), pass("K1")),
                row(surplus("C"), surplus("C"), surplus("K1")),
                row(pass("A"), fail("Z", "A"), pass("K2")),
                row(pass("B"), fail("Z", "B"), pass("K2")),
                row(missing("C"), missing("C"), missing("K2")),
                row(surplus("A"), surplus("A"), surplus("K3")),
                row(missing("A"), missing("A"), missing("K4"))
        );
        addVerification("keyColumnWithOutOfOrderMissingAndSurplusColumns",
                row(missing("Col 4"), outOfOrder("Col 1"), surplus("Col 2"), pass("Col 3")),
                row(missing("Z"), outOfOrder("A"), surplus("A"), pass("K1")),
                row(missing("Z"), outOfOrder("B"), surplus("B"), pass("K1")),
                row(surplus(""), surplus("C"), surplus("C"), surplus("K1")),
                row(missing("A"), outOfOrder("A"), surplus("Z"), pass("K2")),
                row(missing("B"), outOfOrder("B"), surplus("Z"), pass("K2")),
                row(missing("C"), missing("C"), missing(""), missing("K2"))
        );
        addVerification("keyColumnWithMissingKey",
                row(surplus("Col 1"), pass("Col 2"), pass("Col 3")),
                row(surplus("A"), fail("A", "Z"), pass("K1")),
                row(surplus("B"), fail("B", "Z"), pass("K1")),
                row(surplus("A"), fail("Z", "A"), pass("K2")),
                row(surplus("B"), fail("Z", "B"), pass("K2"))
        );
    }

    private static void addVerification(String testName, List<ResultCell>... rows)
    {
        Assert.assertFalse(MAX_BEST_MATCH_THRESHOLD.containsKey(testName));
        MAX_BEST_MATCH_THRESHOLD.put(testName, Arrays.asList(rows));
    }
}
