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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TablascoExtension.class)
public class IgnoreMissingAndSurplusTest {

    public final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy();

    @Test
    void allRowsMatch() throws IOException {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table, table);
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        <td class="pass">A2</td>
                        </tr>
                        <tr>
                        <td class="pass">B1</td>
                        <td class="pass">B2</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void failsWithSurplusAndToldToIgnoreJustMissing() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");

        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withIgnoreMissingRows().verify("name", table1, table2));

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
                        <td class="pass">B1</td>
                        <td class="pass">B2</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void failsWithMissingAndToldToIgnoreJustSurplus() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");

        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withIgnoreSurplusRows().verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="missing">A1<p>Missing</p>
                        </td>
                        <td class="missing">A2<p>Missing</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass">B1</td>
                        <td class="pass">B2</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void passesWithSurplusAndMissing() throws IOException {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">B1</td>
                        <td class="pass">B2</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void failsWithMissingSurplusHeader() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 3", "C1", "C2", "B1", "B2");
        TableTestUtils.assertAssertionError(() ->
                tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="surplus">Col 3<p>Surplus</p>
                        </th>
                        <th class="missing">Col 2<p>Missing</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="pass">B1</td>
                        <td class="surplus">B2<p>Surplus</p>
                        </td>
                        <td class="missing">B2<p>Missing</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void failsWithDifferenceInCommonRow() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B3");
        TableTestUtils.assertAssertionError(() ->
                tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">B1</td>
                        <td class="fail">B2<p>Expected</p>
                        <hr/>B3<p>Actual</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void passesWithEmptyExpected() throws IOException {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void passesWithEmptyActual() throws IOException {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void ignoreSurplusColumnsPassesWithSurplus() throws IOException {
        VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        VerifiableTable actual = TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3");
        this.tableVerifier.withIgnoreSurplusColumns().verify("name", expected, actual);
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 3</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        <td class="pass">A3</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void ignoreSurplusColumnsFailsWithMissing() throws IOException {
        final VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        final VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withIgnoreSurplusColumns().verify("name", expected, actual));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="missing">Col 3<p>Missing</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        <td class="missing">A3<p>Missing</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void ignoreMissingColumnsPassesWithMissing() throws IOException {
        VerifiableTable expected = TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3");
        VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        this.tableVerifier.withIgnoreMissingColumns().verify("name", expected, actual);
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 3</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        <td class="pass">A3</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void ignoreMissingColumnsFailsWithSurplus() throws IOException {
        final VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        TableTestUtils.assertAssertionError(
                () -> tableVerifier.withIgnoreMissingColumns().verify("name", expected, actual));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="surplus">Col 3<p>Surplus</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        <td class="surplus">A3<p>Surplus</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void ignoreMissingAndSurplusColumnsPasses() throws IOException {
        VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        this.tableVerifier.withIgnoreMissingColumns().withIgnoreSurplusColumns().verify("name", expected, actual);
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}
