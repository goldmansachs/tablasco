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

import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.rebase.RebaseFileWriter;
import com.gs.tablasco.results.ExpectedResults;
import com.gs.tablasco.results.FileSystemExpectedResultsLoader;
import com.gs.tablasco.results.parser.ExpectedResultsParser;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractSingleTableVerifierTest
{
    private static final CellComparator CELL_COMPARATOR = new ToleranceCellComparator(new CellFormatter(1.0, true));

    @Rule
    public final TestName testName = new TestName();
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(TableTestUtils.getOutputDirectory());

    private VerifiableTable expected;
    private VerifiableTable actual;

    protected static ResultCell pass(String value)
    {
        return ResultCell.createMatchedCell(CELL_COMPARATOR, value, value);
    }

    protected static ResultCell fail(String actual, String expected)
    {
        return ResultCell.createMatchedCell(CELL_COMPARATOR, actual, expected);
    }

    protected static ResultCell missing(String expected)
    {
        return ResultCell.createMissingCell(CELL_COMPARATOR.getFormatter(), expected);
    }

    protected static ResultCell surplus(String actual)
    {
        return ResultCell.createSurplusCell(CELL_COMPARATOR.getFormatter(), actual);
    }

    static ResultCell outOfOrder(String actual)
    {
        return ResultCell.createOutOfOrderCell(CELL_COMPARATOR.getFormatter(), actual);
    }

    private void actualTable(List... headerAndRows)
    {
        Assert.assertNull(this.actual);
        this.actual = new ListVerifiableTable(Arrays.asList(headerAndRows));
    }

    private void expectedTable(List... headerAndRows)
    {
        Assert.assertNull(this.expected);
        this.expected = new ListVerifiableTable(Arrays.asList(headerAndRows));
    }

    private void assertVerification()
    {
        ColumnComparators columnComparators = new ColumnComparators.Builder().withTolerance(1.0d).build();
        SingleTableVerifier verifier = createSingleTableVerifier(columnComparators);
        List<List<ResultCell>> actualVerification = verifier.verify(this.actual, this.expected).getVerifiedRows();
        VerifiableTable rebasedExpected = getRebasedExpected();
        List<List<ResultCell>> rebasedActualVerification = verifier.verify(this.actual, rebasedExpected).getVerifiedRows();
        List<List<ResultCell>> expectedVerification = this.getExpectedVerification(this.testName.getMethodName());
        this.writeResults("ACTUAL", actualVerification);
        this.writeResults("ACTUAL (REBASE)", rebasedActualVerification);
        this.writeResults("EXPECTED", expectedVerification);
        Assert.assertEquals("Verification with actual results", expectedVerification, actualVerification);
        Assert.assertEquals("Verification with REBASED actual results", expectedVerification, rebasedActualVerification);
    }

    private VerifiableTable getRebasedExpected()
    {
        try
        {
            File rebaseFile = this.temporaryFolder.newFile("table.txt");
            new RebaseFileWriter(Metadata.newWithRecordedAt(), null, new ColumnComparators.Builder().build(), rebaseFile).writeRebasedResults("method", Collections.singletonMap("table", this.expected));
            ExpectedResults expectedResults = new ExpectedResultsParser(new FileSystemExpectedResultsLoader(), rebaseFile).parse();
            return expectedResults.getTable("method", "table");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected abstract SingleTableVerifier createSingleTableVerifier(ColumnComparators columnComparators);

    protected abstract List<List<ResultCell>> getExpectedVerification(String methodName);

    private void writeResults(String tableName, List<List<ResultCell>> verify)
    {
        File outputFile = new File(TableTestUtils.getOutputDirectory(), this.getClass().getSimpleName() + ".html");
        HtmlFormatter htmlFormatter = new HtmlFormatter(outputFile, new HtmlOptions(false, HtmlFormatter.DEFAULT_ROW_LIMIT, false, false, false, Collections.emptySet()));
        htmlFormatter.appendResults(this.testName.getMethodName(), Collections.singletonMap(tableName, new ResultTable(new boolean[verify.get(0).size()], verify)), Metadata.newEmpty());
    }

    protected static <T> List<T> row(T... values)
    {
        return Arrays.asList(values);
    }

    @Test
    public void columnHeadersMatch()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"));
        this.expectedTable(
                row("Entity", "Account", "Notional"));
        this.assertVerification();
    }

    @Test
    public void columnHeadersAndRowsMatch()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSIL", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSIL", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void stringCellMismatch()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSE", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void oneHeaderIsDifferent()
    {
        this.actualTable(
                row("Entity", "Account", "Notional (USD)"));
        this.expectedTable(
                row("Entity", "Account", "Notional"));
        this.assertVerification();
    }

    @Test
    public void surplusColumnAtEnd()
    {
        this.actualTable(
                row("column1", "column2", "column3", "column4", "column5"));
        this.expectedTable(
                row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    public void surplusColumnAtFront()
    {
        this.actualTable(
				row("column1", "column2", "column3", "column4", "column5"));
        this.expectedTable(
				row(/*      */ "column2", "column3", "column4", "column5"));
        this.assertVerification();
    }

    @Test
    public void surplusColumnInMiddle()
    {
        this.actualTable(
				row("column1", "column2", "column3", "column4", "column5"));
        this.expectedTable(
				row("column1", "column2", /*      */ "column4", "column5"));
        this.assertVerification();
    }

    @Test
    public void twoSurplusColumnsInMiddle()
    {
        this.actualTable(
				row("column1", "column2", "column3", "column4", "column5"));
        this.expectedTable(
				row("column1", /*                 */ "column4", "column5"));
        this.assertVerification();
    }

    @Test
    public void surplusColumnWithRowMismatch()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSE", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Notional"),
                row("GSI", 1000000));
        this.assertVerification();
    }

    @Test
    public void missingColumnWithRowMismatch()
    {
        this.actualTable(
                row("Entity", "Notional"),
                row("GSE", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void missingColumnAtFront()
    {
        this.actualTable(
				row(/*      */ "column2", "column3", "column4"));
        this.expectedTable(
				row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    public void missingColumnAtEnd()
    {
        this.actualTable(
				row("column1", "column2", "column3" /*     */));
        this.expectedTable(
				row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    public void missingColumnInMiddle()
    {
        this.actualTable(
				row("column1", /*      */ "column3", "column4"));
        this.expectedTable(
				row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    public void twoMissingColumnsInMiddle()
    {
        this.actualTable(
				row("column1", "column4"));
        this.expectedTable(
				row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    public void multipleSurplusAndMissingColumns()
    {
        this.actualTable(
				row("column1", "column4", "column5"/*   */, "column7", "column8"));
        this.expectedTable(
				row("column1", "column2", "column3", "column4", "column6"/*   */, "column7" /*      */));
        this.assertVerification();
    }

    @Test
    public void numericMismatchesWithTolerance()
    {
        //testing tolerance of 0.001
        //todo: kernjo: The tolerance should be defined per test rather than globally
        //todo: kernjo: Follow up on what makes sense to display for column 5 when it goes green --
        //Should we consider applying a format on the result to the specified tolerance?

        this.actualTable(
                row("column1", "column2", "column3", "column4", "column5"),
                row("c1--", 19843.11, 100005.0, 101.01, 100.004));
        this.expectedTable(
                row("column1", "column2", "column3", "column4", "column5"),
                row("c1--", 19853.11, 100002.0, 101.00, 100.0034));
        this.assertVerification();
    }

    @Test
    public void surplusRowAtBottom()
    {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--d--", "--e--", "--f--"));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"));
        this.assertVerification();
    }

    @Test
    public void missingRowAtBottom()
    {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--d--", "--e--", "--f--"));
        this.assertVerification();
    }

    @Test
    public void unmatchedRowsWithNans()
    {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("c1--", Double.NaN, 100.002),
                row("c1--", 65.2, 100.002));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("c1--", 65.2, 100.002),
                row("c1--", Double.toString(Double.NaN), 100.002));
        this.assertVerification();

    }

    @Test
    public void singleColumnMissingActualInMiddle()
    {
        this.actualTable(
                row("Single"),
                row("10"),
                row("20"),
                row("40"));
        this.expectedTable(
                row("Single"),
                row("10"),
                row("20"),
                row("30"),
                row("40"));
        this.assertVerification();
    }

    @Test
    public void singleColumnMissingActualAtEnd()
    {
        this.actualTable(
                row("Single"),
                row("10"),
                row("20"),
                row("30"));
        this.expectedTable(
                row("Single"),
                row("10"),
                row("20"),
                row("30"),
                row("40"));
        this.assertVerification();
    }

    @Test
    public void singleColumnExtraActual()
    {
        this.actualTable(
                row("Single"),
                row("10"),
                row("20"),
                row("25"),
                row("30"),
                row("40"));
        this.expectedTable(
                row("Single"),
                row("10"),
                row("20"),
                row("30"),
                row("40"));
        this.assertVerification();
    }

    @Test
    public void singleColumnOutOfOrder()
    {
        this.actualTable(
                row("Single"),
                row("10"),
                row("30"),
                row("20"),
                row("40"));
        this.expectedTable(
                row("Single"),
                row("10"),
                row("20"),
                row("30"),
                row("40"));
        this.assertVerification();
    }

    @Test
    public void matchFirstDuplicateRowAndShowSecondAsSurplus()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000),
                row("GSI", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void matchFirstDuplicateRowAndShowSecondAsMissing()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000),
                row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void matchFirstPartialRowAndShowSecondAsSurplus()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSE", "76543210.01", 1000000),
                row("GSU", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void matchFirstPartialRowAndShowSecondAsMissing()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSU", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000),
                row("GSE", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void matchBestPartialRowAndShowSecondAsSurplus()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSE", "76543210.99", 1000000),
                row("GSU", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void matchBestPartialRowAndShowSecondAsMissing()
    {
        this.actualTable(
                row("Entity", "Account", "Notional"),
                row("GSU", "76543210.01", 1000000));
        this.expectedTable(
                row("Entity", "Account", "Notional"),
                row("GSI", "76543210.99", 1000000),
                row("GSE", "76543210.01", 1000000));
        this.assertVerification();
    }

    @Test
    public void stringColumnAtEndIsTreatedAsAnyOtherValueWouldBe()
    {
        this.actualTable(
                row("Entity", "Account", "Notional", "Strings"),
                row("GSI", "76543210.01", 1000000, "a diff string"));
        this.expectedTable(
                row("Entity", "Account", "Notional", "Strings"),
                row("GSI", "76543210.01", 1000000, "a string"));
        this.assertVerification();
    }

    @Test
    public void outOfOrderColumns2()
    {
        this.actualTable(
				row("column4", "column1", "column3", "column2"));
        this.expectedTable(
				row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    public void outOfOrderColumns3()
    {
        this.actualTable(
				row("column4", "column1", "column2", "column3"));
        this.expectedTable(
				row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    public void outOfOrderColumns4()
    {
        this.actualTable(
				row("column3", "column2", "column1"));
        this.expectedTable(
				row("column1", "column2", "column3"));
        this.assertVerification();
    }

    @Test
    public void outOfOrderColumns()
    {
        this.actualTable(
				row("column1", "column2", "column4", "column3"));
        this.expectedTable(
				row("column1", "column2", "column3", "column4"));
        this.assertVerification();
    }

    @Test
    public void surplusRowAtStart()
    {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("--d--", "--e--", "--f--"),
                row("--a--", "--b--", "--c--"));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"));
        this.assertVerification();
    }

    @Test
    public void missingRowAtStart()
    {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("--d--", "--e--", "--f--"),
                row("--a--", "--b--", "--c--"));
        this.assertVerification();
    }

    @Test
    public void proximityIsIncludedInMatch()
    {
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
    public void proximityIsIncludedInMatch2()
    {
        this.expectedTable(
                row("Key 1", "Bal 1", "Bal 2", "Bal 3"),
                row("A", 1, 0, 0),
                row("E", 2, 0, 0));
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
    public void surplusRowInMiddle()
    {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--d--", "--e--", "--f--"),
                row("--g--", "--h--", "--i--"));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--g--", "--h--", "--i--"));
        this.assertVerification();
    }

    @Test
    public void missingRowInMiddle()
    {
        this.actualTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--g--", "--h--", "--i--"));
        this.expectedTable(
                row("column1", "column2", "column3"),
                row("--a--", "--b--", "--c--"),
                row("--d--", "--e--", "--f--"),
                row("--g--", "--h--", "--i--"));
        this.assertVerification();
    }

    @Test
    public void brokenRowInMiddle()
    {
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
    public void adaptiveMatcherLeavesLeastUnmatchedRows()
    {
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
        this.expectedTable(
                row("Col 1", "Col 2", "Col 3"),
                row("A", "A", 1),
                row("A", "A", 3),
                row("B", "A", 1),
                row("C", "B", 1),
                row("D", "B", 1),
                row("E", "C", 1),
                row("Y", "C", 1),
                row("Y", "Y", 1));
        this.assertVerification();
    }

    @Test
    public void duplicateActualRow()
    {
        this.actualTable(
                row("Col 1","Col 2"),
                row("A","B"),
                row("C","D"),
                row("C","D"),
                row("E","F"));
        this.expectedTable(
                row("Col 1","Col 2"),
                row("A","A"),
                row("C","D"),
                row("E","E"));
        this.assertVerification();
    }

    @Test
    public void duplicateExpectedRow()
    {
        this.actualTable(
                row("Col 1","Col 2"),
                row("A","B"),
                row("C","D"),
                row("E","F"));
        this.expectedTable(
                row("Col 1","Col 2"),
                row("A","A"),
                row("C","D"),
                row("C","D"),
                row("E","E"));
        this.assertVerification();
    }

    @Test
    public void duplicateActualColumn()
    {
        this.actualTable(
                row("Col 1","Col 2","Col 2"),
                row("A","B","B"));
        this.expectedTable(
                row("Col 1","Col 2"),
                row("A","B"));
        this.assertVerification();
    }

    @Test
    public void duplicateExpectedColumn()
    {
        this.actualTable(
                row("Col 1","Col 2"),
                row("A","B"));
        this.expectedTable(
                row("Col 1","Col 2","Col 2"),
                row("A","B","B"));
        this.assertVerification();
    }

    @Test
    public void duplicateColumnName()
    {
        this.actualTable(
                row("Col 1","Col 2","Col 2"),
                row("A","B","C"));
        this.expectedTable(
                row("Col 1","Col 2","Col 2"),
                row("A","B","C"));
        this.assertVerification();
    }

    @Test
    public void keyColumn()
    {
        this.actualTable(
                row("Col 1","Col 2","Col 3","Col 4","Col 5"),
                row("A","B","C","D","E"),
                row("F","G","H","I","J"),
                row("K","L","M","N","O"));
        this.actual = new KeyedVerifiableTableAdapter(this.actual, 0, 2);
        this.expectedTable(
                row("Col 1","Col 2","Col 3","Col 4","Col 5"),
                row("Z","B","C","D","E"),
                row("F","G","Z","I","J"),
                row("K","Z","M","N","O"));
        this.assertVerification();
    }

    @Test
    public void keyColumnWithDupes()
    {
        this.actualTable(
                row("Col 1","Col 2","Col 3"),
                row("A","A","K1"),
                row("B","B","K1"),
                row("C","C","K1"),
                row("A","Z","K2"),
                row("B","Z","K2"),
                row("A","A","K3"));
        this.actual = new KeyedVerifiableTableAdapter(this.actual, 2);
        this.expectedTable(
                row("Col 1","Col 2","Col 3"),
                row("A","Z","K1"),
                row("B","Z","K1"),
                row("A","A","K2"),
                row("B","B","K2"),
                row("C","C","K2"),
                row("A","A","K4"));
        this.assertVerification();
    }

    @Test
    public void keyColumnWithOutOfOrderMissingAndSurplusColumns()
    {
        this.actualTable(
                row("Col 1","Col 2","Col 3"),
                row("A","A","K1"),
                row("B","B","K1"),
                row("C","C","K1"),
                row("A","Z","K2"),
                row("B","Z","K2"));
        this.actual = new KeyedVerifiableTableAdapter(this.actual, 2);
        this.expectedTable(
                row("Col 4","Col 3","Col 1"),
                row("Z","K1","A"),
                row("Z","K1","B"),
                row("A","K2","A"),
                row("B","K2","B"),
                row("C","K2","C"));
        this.assertVerification();
    }

    @Test
    public void keyColumnWithMissingKey()
    {
        this.actualTable(
                row("Col 1","Col 2","Col 3"),
                row("A","A","K1"),
                row("B","B","K1"),
                row("A","Z","K2"),
                row("B","Z","K2"));
        this.actual = new KeyedVerifiableTableAdapter(this.actual, 0, 2);
        this.expectedTable(
                row("Col 2","Col 3"),
                row("Z","K1"),
                row("Z","K1"),
                row("A","K2"),
                row("B","K2"));
        this.assertVerification();
    }
}
