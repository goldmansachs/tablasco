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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.core.HtmlConfig;
import com.gs.tablasco.core.Tables;
import com.gs.tablasco.rebase.RebaseFileWriter;
import com.gs.tablasco.results.ExpectedResults;
import com.gs.tablasco.results.FileSystemExpectedResultsLoader;
import com.gs.tablasco.results.parser.ExpectedResultsParser;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

@TestMethodOrder(MethodName.class)
public abstract class AbstractSingleTableVerifierTest {
    private static final CellComparator CELL_COMPARATOR = new ToleranceCellComparator(new CellFormatter(1.0, true));

    public String testName;

    @TempDir
    public File temporaryFolder;

    private VerifiableTable expected;
    private VerifiableTable actual;

    protected static ResultCell pass(String value) {
        return ResultCell.createMatchedCell(CELL_COMPARATOR, value, value);
    }

    protected static ResultCell fail(String actual, String expected) {
        return ResultCell.createMatchedCell(CELL_COMPARATOR, actual, expected);
    }

    protected static ResultCell missing(String expected) {
        return ResultCell.createMissingCell(CELL_COMPARATOR.getFormatter(), expected);
    }

    protected static ResultCell surplus(String actual) {
        return ResultCell.createSurplusCell(CELL_COMPARATOR.getFormatter(), actual);
    }

    static ResultCell outOfOrder(String actual) {
        return ResultCell.createOutOfOrderCell(CELL_COMPARATOR.getFormatter(), actual);
    }

    private void actualTable(List<?>... headerAndRows) {
        assertNull(this.actual);
        this.actual = Tables.fromList(Arrays.asList(headerAndRows));
    }

    private void expectedTable(List<?>... headerAndRows) {
        assertNull(this.expected);
        this.expected = Tables.fromList(Arrays.asList(headerAndRows));
    }

    private void assertVerification() {
        ColumnComparators columnComparators =
                new ColumnComparators.Builder().withTolerance(1.0d).build();
        SingleTableVerifier verifier = createSingleTableVerifier(columnComparators);
        List<List<ResultCell>> actualVerification =
                verifier.verify(this.actual, this.expected).getVerifiedRows();
        VerifiableTable rebasedExpected = getRebasedExpected();
        List<List<ResultCell>> rebasedActualVerification =
                verifier.verify(this.actual, rebasedExpected).getVerifiedRows();
        List<List<ResultCell>> expectedVerification = this.getExpectedVerification(this.testName);
        this.writeResults("ACTUAL", actualVerification);
        this.writeResults("ACTUAL (REBASE)", rebasedActualVerification);
        this.writeResults("EXPECTED", expectedVerification);
        assertEquals(expectedVerification, actualVerification, "Verification with actual results");
        assertEquals(expectedVerification, rebasedActualVerification, "Verification with REBASED actual results");
    }

    private VerifiableTable getRebasedExpected() {
        try {
            File rebaseFile = File.createTempFile("table.txt", null, this.temporaryFolder);
            new RebaseFileWriter(
                            Metadata.newWithRecordedAt(), null, new ColumnComparators.Builder().build(), rebaseFile)
                    .writeRebasedResults("method", Collections.singletonMap("table", this.expected));
            ExpectedResults expectedResults =
                    new ExpectedResultsParser(new FileSystemExpectedResultsLoader(), rebaseFile).parse();
            return expectedResults.getTable("method", "table");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract SingleTableVerifier createSingleTableVerifier(ColumnComparators columnComparators);

    protected abstract List<List<ResultCell>> getExpectedVerification(String methodName);

    private void writeResults(String tableName, List<List<ResultCell>> verify) {
        File outputFile =
                new File(TableTestUtils.getOutputDirectory(), this.getClass().getSimpleName() + ".html");
        HtmlFormatter htmlFormatter = new HtmlFormatter(outputFile, new HtmlConfig());
        htmlFormatter.appendResults(
                this.testName,
                Collections.singletonMap(
                        tableName, new ResultTable(new boolean[verify.getFirst().size()], verify)),
                Metadata.newEmpty());
    }

    protected static List<?> row(Object... values) {
        return List.of(values);
    }

    @Test
    void columnHeadersMatch() {
        this.actualTable(row("Entity", "Account", "Notional"));
        this.expectedTable(row("Entity", "Account", "Notional"));
        this.assertVerification();
    }

    @Test
    void columnHeadersAndRowsMatch() {
        this.actualTable(row("Entity", "Account", "Notional"), row("GSIL", "76543210.01", 1000000));
        this.expectedTable(row("Entity", "Account", "Notional"), row("GSIL", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void stringCellMismatch() {
        this.actualTable(row("Entity", "Account", "Notional"), row("GSE", "76543210.01", 1000000));
        this.expectedTable(row("Entity", "Account", "Notional"), row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void oneHeaderIsDifferent() {
        this.actualTable(row("Entity", "Account", "Notional (USD)"));
        this.expectedTable(row("Entity", "Account", "Notional"));
        this.assertVerification();
    }

    @Test
    void surplusColumnAtEnd() {
        this.actualTable(row("column1", "column2", "column3", "column4", "column5"));
        this.expectedTable(row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    void surplusColumnAtFront() {
        this.actualTable(row("column1", "column2", "column3", "column4", "column5"));
        this.expectedTable(row(/*      */ "column2", "column3", "column4", "column5"));
        this.assertVerification();
    }

    @Test
    void surplusColumnInMiddle() {
        this.actualTable(row("column1", "column2", "column3", "column4", "column5"));
        this.expectedTable(row("column1", "column2", /*      */ "column4", "column5"));
        this.assertVerification();
    }

    @Test
    void twoSurplusColumnsInMiddle() {
        this.actualTable(row("column1", "column2", "column3", "column4", "column5"));
        this.expectedTable(row("column1", /*                 */ "column4", "column5"));
        this.assertVerification();
    }

    @Test
    void surplusColumnWithRowMismatch() {
        this.actualTable(row("Entity", "Account", "Notional"), row("GSE", "76543210.01", 1000000));
        this.expectedTable(row("Entity", "Notional"), row("GSI", 1000000));
        this.assertVerification();
    }

    @Test
    void missingColumnWithRowMismatch() {
        this.actualTable(row("Entity", "Notional"), row("GSE", 1000000));
        this.expectedTable(row("Entity", "Account", "Notional"), row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void missingColumnAtFront() {
        this.actualTable(row(/*      */ "column2", "column3", "column4"));
        this.expectedTable(row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    void missingColumnAtEnd() {
        this.actualTable(row("column1", "column2", "column3" /*     */));
        this.expectedTable(row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    void missingColumnInMiddle() {
        this.actualTable(row("column1", /*      */ "column3", "column4"));
        this.expectedTable(row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    void twoMissingColumnsInMiddle() {
        this.actualTable(row("column1", "column4"));
        this.expectedTable(row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    void multipleSurplusAndMissingColumns() {
        this.actualTable(row("column1", "column4", "column5" /*   */, "column7", "column8"));
        this.expectedTable(row("column1", "column2", "column3", "column4", "column6" /*   */, "column7" /*      */));
        this.assertVerification();
    }

    @Test
    void numericMismatchesWithTolerance() {
        // testing tolerance of 0.001
        // todo: kernjo: The tolerance should be defined per test rather than globally
        // todo: kernjo: Follow up on what makes sense to display for column 5 when it goes green --
        // Should we consider applying a format on the result to the specified tolerance?

        this.actualTable(
                row("column1", "column2", "column3", "column4", "column5"),
                row("c1--", 19843.11, 100005.0, 101.01, 100.004));
        this.expectedTable(
                row("column1", "column2", "column3", "column4", "column5"),
                row("c1--", 19853.11, 100002.0, 101.00, 100.0034));
        this.assertVerification();
    }

    @Test
    void surplusRowAtBottom() {
        this.actualTable(
                row("column1", "column2", "column3"), row("--a--", "--b--", "--c--"), row("--d--", "--e--", "--f--"));
        this.expectedTable(row("column1", "column2", "column3"), row("--a--", "--b--", "--c--"));
        this.assertVerification();
    }

    @Test
    void missingRowAtBottom() {
        this.actualTable(row("column1", "column2", "column3"), row("--a--", "--b--", "--c--"));
        this.expectedTable(
                row("column1", "column2", "column3"), row("--a--", "--b--", "--c--"), row("--d--", "--e--", "--f--"));
        this.assertVerification();
    }

    @Test
    void unmatchedRowsWithNans() {
        this.actualTable(
                row("column1", "column2", "column3"), row("c1--", Double.NaN, 100.002), row("c1--", 65.2, 100.002));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("c1--", 65.2, 100.002),
                row("c1--", Double.toString(Double.NaN), 100.002));
        this.assertVerification();
    }

    @Test
    void singleColumnMissingActualInMiddle() {
        this.actualTable(row("Single"), row("10"), row("20"), row("40"));
        this.expectedTable(row("Single"), row("10"), row("20"), row("30"), row("40"));
        this.assertVerification();
    }

    @Test
    void singleColumnMissingActualAtEnd() {
        this.actualTable(row("Single"), row("10"), row("20"), row("30"));
        this.expectedTable(row("Single"), row("10"), row("20"), row("30"), row("40"));
        this.assertVerification();
    }

    @Test
    void singleColumnExtraActual() {
        this.actualTable(row("Single"), row("10"), row("20"), row("25"), row("30"), row("40"));
        this.expectedTable(row("Single"), row("10"), row("20"), row("30"), row("40"));
        this.assertVerification();
    }

    @Test
    void singleColumnOutOfOrder() {
        this.actualTable(row("Single"), row("10"), row("30"), row("20"), row("40"));
        this.expectedTable(row("Single"), row("10"), row("20"), row("30"), row("40"));
        this.assertVerification();
    }

    @Test
    void matchFirstDuplicateRowAndShowSecondAsSurplus() {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000),
                row("GSI", "76543210.01", 1000000));
        this.expectedTable(row("Entity", "Account", "Notional"), row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void matchFirstDuplicateRowAndShowSecondAsMissing() {
        this.actualTable(row("Entity", "Account", "Notional"), row("GSI", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000),
                row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void matchFirstPartialRowAndShowSecondAsSurplus() {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSE", "76543210.01", 1000000),
                row("GSU", "76543210.01", 1000000));
        this.expectedTable(row("Entity", "Account", "Notional"), row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void matchFirstPartialRowAndShowSecondAsMissing() {
        this.actualTable(row("Entity", "Account", "Notional"), row("GSU", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000),
                row("GSE", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void matchBestPartialRowAndShowSecondAsSurplus() {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSE", "76543210.99", 1000000),
                row("GSU", "76543210.01", 1000000));
        this.expectedTable(row("Entity", "Account", "Notional"), row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void matchBestPartialRowAndShowSecondAsMissing() {
        this.actualTable(row("Entity", "Account", "Notional"), row("GSU", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.99", 1000000),
                row("GSE", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    void stringColumnAtEndIsTreatedAsAnyOtherValueWouldBe() {
        this.actualTable(
                row("Entity", "Account", "Notional", "Strings"), row("GSI", "76543210.01", 1000000, "a diff string"));
        this.expectedTable(
                row("Entity", "Account", "Notional", "Strings"), row("GSI", "76543210.01", 1000000, "a string"));
        this.assertVerification();
    }

    @Test
    void outOfOrderColumns2() {
        this.actualTable(row("column4", "column1", "column3", "column2"));
        this.expectedTable(row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    void outOfOrderColumns3() {
        this.actualTable(row("column4", "column1", "column2", "column3"));
        this.expectedTable(row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    void outOfOrderColumns4() {
        this.actualTable(row("column3", "column2", "column1"));
        this.expectedTable(row("column1", "column2", "column3"));
        this.assertVerification();
    }

    @Test
    void outOfOrderColumns() {
        this.actualTable(row("column1", "column2", "column4", "column3"));
        this.expectedTable(row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    void surplusRowAtStart() {
        this.actualTable(
                row("column1", "column2", "column3"), row("--d--", "--e--", "--f--"), row("--a--", "--b--", "--c--"));
        this.expectedTable(row("column1", "column2", "column3"), row("--a--", "--b--", "--c--"));
        this.assertVerification();
    }

    @Test
    void missingRowAtStart() {
        this.actualTable(row("column1", "column2", "column3"), row("--a--", "--b--", "--c--"));
        this.expectedTable(
                row("column1", "column2", "column3"), row("--d--", "--e--", "--f--"), row("--a--", "--b--", "--c--"));
        this.assertVerification();
    }

    @Test
    void proximityIsIncludedInMatch() {
        this.actualTable(
                row("Key 1", "Key 2", "Bal 1", "Bal 2", "Bal 3"),
                row("A", "A", 10, 20, 3),
                row("A", "B", 4, 5, 6),
                row("A", "C", 7, 8, 9),
                row("B", "A", 1, 2, 3));
        this.expectedTable(
                row("Key 1", "Key 2", "Bal 1", "Bal 2", "Bal 3"),
                row("A", "A", 1, 2, 3),
                row("A", "B", 4, 5, 6),
                row("A", "C", 7, 8, 9),
                row("B", "A", 10, 20, 30));
        this.assertVerification();
    }

    @Test
    void proximityIsIncludedInMatch2() {
        this.expectedTable(row("Key 1", "Bal 1", "Bal 2", "Bal 3"), row("A", 1, 0, 0), row("E", 2, 0, 0));
        this.actualTable(
                row("Key 1", "Bal 1", "Bal 2", "Bal 3"),
                row("A", 3, 0, 0),
                row("B", 4, 0, 0),
                row("C", 5, 0, 0),
                row("D", 6, 0, 0),
                row("E", 7, 0, 0));
        this.assertVerification();
    }

    @Test
    void surplusRowInMiddle() {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--d--", "--e--", "--f--"),
                row("--g--", "--h--", "--i--"));
        this.expectedTable(
                row("column1", "column2", "column3"), row("--a--", "--b--", "--c--"), row("--g--", "--h--", "--i--"));
        this.assertVerification();
    }

    @Test
    void missingRowInMiddle() {
        this.actualTable(
                row("column1", "column2", "column3"), row("--a--", "--b--", "--c--"), row("--g--", "--h--", "--i--"));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--d--", "--e--", "--f--"),
                row("--g--", "--h--", "--i--"));
        this.assertVerification();
    }

    @Test
    void brokenRowInMiddle() {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--d--", "--e--", "--x--"),
                row("--g--", "--h--", "--i--"),
                row("--j--", "--k--", "--l--"));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--d--", "--e--", "--f--"),
                row("--g--", "--h--", "--i--"),
                row("--j--", "--k--", "--l--"));
        this.assertVerification();
    }

    @Test
    void adaptiveMatcherLeavesLeastUnmatchedRows() {
        this.actualTable(
                row("Col 1", "Col 2", "Col 3"),
                row("A", "A", 0),
                row("A", "A", 2),
                row("B", "A", 0),
                row("C", "B", 0),
                row("D", "B", 0),
                row("E", "C", 0),
                row("X", "C", 0),
                row("X", "X", 0));
        List<?> result;
        Object[] values = new Object[] {"Y", "C", 1};
        result = List.of(values);
        this.expectedTable(
                row("Col 1", "Col 2", "Col 3"),
                row("A", "A", 1),
                row("A", "A", 3),
                row("B", "A", 1),
                row("C", "B", 1),
                row("D", "B", 1),
                row("E", "C", 1),
                result,
                List.of(new Object[] {"Y", "Y", 1}));
        this.assertVerification();
    }

    @Test
    void duplicateActualRow() {
        this.actualTable(
                List.of(new Object[] {"Col 1", "Col 2"}),
                List.of(new Object[] {"A", "B"}),
                List.of(new Object[] {"C", "D"}),
                List.of(new Object[] {"C", "D"}),
                List.of(new Object[] {"E", "F"}));
        this.expectedTable(
                List.of(new Object[] {"Col 1", "Col 2"}),
                List.of(new Object[] {"A", "A"}),
                List.of(new Object[] {"C", "D"}),
                List.of(new Object[] {"E", "E"}));
        this.assertVerification();
    }

    @Test
    void duplicateExpectedRow() {
        this.actualTable(
                List.of(new Object[] {"Col 1", "Col 2"}),
                List.of(new Object[] {"A", "B"}),
                List.of(new Object[] {"C", "D"}),
                List.of(new Object[] {"E", "F"}));
        this.expectedTable(
                List.of(new Object[] {"Col 1", "Col 2"}),
                List.of(new Object[] {"A", "A"}),
                List.of(new Object[] {"C", "D"}),
                List.of(new Object[] {"C", "D"}),
                List.of(new Object[] {"E", "E"}));
        this.assertVerification();
    }

    @Test
    void duplicateActualColumn() {
        this.actualTable(List.of(new Object[] {"Col 1", "Col 2", "Col 2"}), List.of(new Object[] {"A", "B", "B"}));
        this.expectedTable(List.of(new Object[] {"Col 1", "Col 2"}), List.of(new Object[] {"A", "B"}));
        this.assertVerification();
    }

    @Test
    void duplicateExpectedColumn() {
        this.actualTable(List.of(new Object[] {"Col 1", "Col 2"}), List.of(new Object[] {"A", "B"}));
        this.expectedTable(List.of(new Object[] {"Col 1", "Col 2", "Col 2"}), List.of(new Object[] {"A", "B", "B"}));
        this.assertVerification();
    }

    @Test
    void duplicateColumnName() {
        this.actualTable(List.of(new Object[] {"Col 1", "Col 2", "Col 2"}), List.of(new Object[] {"A", "B", "C"}));
        this.expectedTable(List.of(new Object[] {"Col 1", "Col 2", "Col 2"}), List.of(new Object[] {"A", "B", "C"}));
        this.assertVerification();
    }

    @Test
    void keyColumn() {
        this.actualTable(
                List.of(new Object[] {"Col 1", "Col 2", "Col 3", "Col 4", "Col 5"}),
                List.of(new Object[] {"A", "B", "C", "D", "E"}),
                List.of(new Object[] {"F", "G", "H", "I", "J"}),
                List.of(new Object[] {"K", "L", "M", "N", "O"}));
        this.actual = new KeyedVerifiableTableAdapter(this.actual, 0, 2);
        this.expectedTable(
                List.of(new Object[] {"Col 1", "Col 2", "Col 3", "Col 4", "Col 5"}),
                List.of(new Object[] {"Z", "B", "C", "D", "E"}),
                List.of(new Object[] {"F", "G", "Z", "I", "J"}),
                List.of(new Object[] {"K", "Z", "M", "N", "O"}));
        this.assertVerification();
    }

    @Test
    void keyColumnWithDupes() {
        this.actualTable(
                List.of(new Object[] {"Col 1", "Col 2", "Col 3"}),
                List.of(new Object[] {"A", "A", "K1"}),
                List.of(new Object[] {"B", "B", "K1"}),
                List.of(new Object[] {"C", "C", "K1"}),
                List.of(new Object[] {"A", "Z", "K2"}),
                List.of(new Object[] {"B", "Z", "K2"}),
                List.of(new Object[] {"A", "A", "K3"}));
        this.actual = new KeyedVerifiableTableAdapter(this.actual, 2);
        this.expectedTable(
                List.of(new Object[] {"Col 1", "Col 2", "Col 3"}),
                List.of(new Object[] {"A", "Z", "K1"}),
                List.of(new Object[] {"B", "Z", "K1"}),
                List.of(new Object[] {"A", "A", "K2"}),
                List.of(new Object[] {"B", "B", "K2"}),
                List.of(new Object[] {"C", "C", "K2"}),
                List.of(new Object[] {"A", "A", "K4"}));
        this.assertVerification();
    }

    @Test
    void keyColumnWithOutOfOrderMissingAndSurplusColumns() {
        this.actualTable(
                List.of(new Object[] {"Col 1", "Col 2", "Col 3"}),
                List.of(new Object[] {"A", "A", "K1"}),
                List.of(new Object[] {"B", "B", "K1"}),
                List.of(new Object[] {"C", "C", "K1"}),
                List.of(new Object[] {"A", "Z", "K2"}),
                List.of(new Object[] {"B", "Z", "K2"}));
        this.actual = new KeyedVerifiableTableAdapter(this.actual, 2);
        this.expectedTable(
                List.of(new Object[] {"Col 4", "Col 3", "Col 1"}),
                List.of(new Object[] {"Z", "K1", "A"}),
                List.of(new Object[] {"Z", "K1", "B"}),
                List.of(new Object[] {"A", "K2", "A"}),
                List.of(new Object[] {"B", "K2", "B"}),
                List.of(new Object[] {"C", "K2", "C"}));
        this.assertVerification();
    }

    @Test
    void keyColumnWithMissingKey() {
        this.actualTable(
                List.of(new Object[] {"Col 1", "Col 2", "Col 3"}),
                List.of(new Object[] {"A", "A", "K1"}),
                List.of(new Object[] {"B", "B", "K1"}),
                List.of(new Object[] {"A", "Z", "K2"}),
                List.of(new Object[] {"B", "Z", "K2"}));
        this.actual = new KeyedVerifiableTableAdapter(this.actual, 0, 2);
        this.expectedTable(
                List.of(new Object[] {"Col 2", "Col 3"}),
                List.of(new Object[] {"Z", "K1"}),
                List.of(new Object[] {"Z", "K1"}),
                List.of(new Object[] {"A", "K2"}),
                List.of(new Object[] {"B", "K2"}));
        this.assertVerification();
    }

    @BeforeEach
    public void setup(TestInfo testInfo) {
        Optional<Method> testMethod = testInfo.getTestMethod();
        testMethod.ifPresent(method -> this.testName = method.getName());
    }
}
