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

package com.gs.tablasco;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

public class HtmlFormattingTest {
    @Rule
    public final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy();

    @Test
    void nonNumericBreak() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A9", "B1", "B9");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        <td class="fail">A2<p>Expected</p>
                        <hr/>A9<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass">B1</td>
                        <td class="fail">B2<p>Expected</p>
                        <hr/>B9<p>Actual</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void numericBreak() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 10.123);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 20.456);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.01d).verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">A</td>
                        <td class="fail number">10.12<p>Expected</p>
                        <hr/>20.46<p>Actual</p>
                        <hr/>-10.33 / 102.07%<p>Difference / Variance</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void nonNumericActualNumericExpectedBreak() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 390.0);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", "A2");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withVarianceThreshold(5.0d).verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">A</td>
                        <td class="fail">390<p>Expected</p>
                        <hr/>A2<p>Actual</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void numericActualNonNumericExpectedBreak() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", "A1");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 48.0);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.1d).verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">A</td>
                        <td class="fail">A1<p>Expected</p>
                        <hr/>48<p>Actual</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void outOfOrderColumnPassedCellNonNumeric() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", "A2", "A1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="outoforder">Col 2<p>Out of order</p>
                        </th>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="outoforder">A2<p>Out of order</p>
                        </td>
                        <td class="pass">A1</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void outOfOrderColumnFailedCellNonNumeric() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", "A3", "A1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="outoforder">Col 2<p>Out of order</p>
                        </th>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="fail">A2<p>Expected</p>
                        <hr/>A3<p>Actual</p>
                        </td>
                        <td class="pass">A1</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void outOfOrderColumnPassedCellNumeric() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", 25, 30.78);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.01d).verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="outoforder">Col 2<p>Out of order</p>
                        </th>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="outoforder number">25<p>Out of order</p>
                        </td>
                        <td class="pass number">30.78</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void outOfOrderColumnFailedCellNumeric() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", 25.3, 30.78);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.01d).verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="outoforder">Col 2<p>Out of order</p>
                        </th>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="fail number">25<p>Expected</p>
                        <hr/>25.3<p>Actual</p>
                        <hr/>-0.3 / 1.2%<p>Difference / Variance</p>
                        </td>
                        <td class="pass number">30.78</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void missingSurplusColumnsNonNumeric() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 4", "Col 1", "A2", "A1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="surplus">Col 4<p>Surplus</p>
                        </th>
                        <th class="pass">Col 1</th>
                        <th class="missing">Col 2<p>Missing</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="surplus">A2<p>Surplus</p>
                        </td>
                        <td class="pass">A1</td>
                        <td class="missing">A2<p>Missing</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void missingSurplusColumnsNumeric() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 4", "Col 1", 26, 30.78);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.01d).verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="surplus">Col 4<p>Surplus</p>
                        </th>
                        <th class="pass">Col 1</th>
                        <th class="missing">Col 2<p>Missing</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="surplus number">26<p>Surplus</p>
                        </td>
                        <td class="pass number">30.78</td>
                        <td class="missing number">25<p>Missing</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void missingSurplusRowsNonNumeric() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="surplus">C1<p>Surplus</p>
                        </td>
                        <td class="surplus">C2<p>Surplus</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="missing">A1<p>Missing</p>
                        </td>
                        <td class="missing">A2<p>Missing</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void missingSurplusRowsNumeric() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 345.66, 13.0, 56.44, 45.01);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", 345.63, 12.8, 56.65, 45.31);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withTolerance(0.1d).verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass number">345.6</td>
                        <td class="fail number">13<p>Expected</p>
                        <hr/>12.8<p>Actual</p>
                        <hr/>0.2 / -1.5%<p>Difference / Variance</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="surplus number">56.6<p>Surplus</p>
                        </td>
                        <td class="surplus number">45.3<p>Surplus</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="missing number">56.4<p>Missing</p>
                        </td>
                        <td class="missing number">45<p>Missing</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void assertionSummaryWithSuccess() throws IOException {
        VerifiableTable table = TableTestUtils.createTable(1, "Col 1", "A1");
        this.tableVerifier.withAssertionSummary(true).verify("name", table, table);
        assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>assertionSummaryWithSuccess</h1>
                        <div>
                        <h2>Assertions</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">2 right, 0 wrong, 100.0% correct</th>
                        </tr>
                        </table>
                        </div>
                        <div id="assertionSummaryWithSuccess.name">
                        <h2>name</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        </tr>
                        </table>
                        </div>
                        </body>""",
                TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    void assertionSummaryWithFailure() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A9", "B1", "B9");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withAssertionSummary(true).verify("name", table1, table2));
        assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>assertionSummaryWithFailure</h1>
                        <div>
                        <h2>Assertions</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="fail">4 right, 2 wrong, 66.6% correct</th>
                        </tr>
                        </table>
                        </div>
                        <div id="assertionSummaryWithFailure.name">
                        <h2>name</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        <td class="fail">A2<p>Expected</p>
                        <hr/>A9<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass">B1</td>
                        <td class="fail">B2<p>Expected</p>
                        <hr/>B9<p>Actual</p>
                        </td>
                        </tr>
                        </table>
                        </div>
                        </body>""",
                TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    void assertionSummaryWithMultipleVerify() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(1, "Col 1", "A1");
        final VerifiableTable table2 = TableTestUtils.createTable(1, "Col 1", "A2");
        this.tableVerifier.withAssertionSummary(true).verify("name1", table1, table1);
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withAssertionSummary(true).verify("name2", table1, table2));
        assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>assertionSummaryWithMultipleVerify</h1>
                        <div>
                        <h2>Assertions</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">2 right, 0 wrong, 100.0% correct</th>
                        </tr>
                        </table>
                        </div>
                        <div id="assertionSummaryWithMultipleVerify.name1">
                        <h2>name1</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        </tr>
                        </table>
                        </div>
                        <div>
                        <h2>Assertions</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="fail">1 right, 2 wrong, 33.3% correct</th>
                        </tr>
                        </table>
                        </div>
                        <div id="assertionSummaryWithMultipleVerify.name2">
                        <h2>name2</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="surplus">A2<p>Surplus</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="missing">A1<p>Missing</p>
                        </td>
                        </tr>
                        </table>
                        </div>
                        </body>""",
                TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    void assertionSummaryWithMissingSurplusTables() throws IOException {
        final VerifiableTable table = TableTestUtils.createTable(1, "Col 1", "A1");
        TableTestUtils.assertAssertionError(() -> tableVerifier
                .withAssertionSummary(true)
                .verify(
                        TableTestUtils.toNamedTables("name", table, "name2", table),
                        TableTestUtils.toNamedTables("name", table, "name3", table)));
        assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>assertionSummaryWithMissingSurplusTables</h1>
                        <div>
                        <h2>Assertions</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="fail">2 right, 4 wrong, 33.3% correct</th>
                        </tr>
                        </table>
                        </div>
                        <div id="assertionSummaryWithMissingSurplusTables.name">
                        <h2>name</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        </tr>
                        </table>
                        </div>
                        <div id="assertionSummaryWithMissingSurplusTables.name2">
                        <h2>name2</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="missing">Col 1<p>Missing</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="missing">A1<p>Missing</p>
                        </td>
                        </tr>
                        </table>
                        </div>
                        <div id="assertionSummaryWithMissingSurplusTables.name3">
                        <h2>name3</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="surplus">Col 1<p>Surplus</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="surplus">A1<p>Surplus</p>
                        </td>
                        </tr>
                        </table>
                        </div>
                        </body>""",
                TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    void assertionSummaryWithHideMatchedRows() throws IOException {
        final VerifiableTable table1 =
                TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2");
        final VerifiableTable table2 =
                TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C9");
        TableTestUtils.assertAssertionError(() -> tableVerifier
                .withAssertionSummary(true)
                .withHideMatchedRows(true)
                .verify("name", table1, table2));
        assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>assertionSummaryWithHideMatchedRows</h1>
                        <div>
                        <h2>Assertions</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="fail">7 right, 1 wrong, 87.5% correct</th>
                        </tr>
                        </table>
                        </div>
                        <div id="assertionSummaryWithHideMatchedRows.name">
                        <h2>name</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">2 matched rows...</td>
                        </tr>
                        <tr>
                        <td class="pass">C1</td>
                        <td class="fail">C2<p>Expected</p>
                        <hr/>C9<p>Actual</p>
                        </td>
                        </tr>
                        </table>
                        </div>
                        </body>""",
                TableTestUtils.getHtml(this.tableVerifier, "body"));
    }
}
