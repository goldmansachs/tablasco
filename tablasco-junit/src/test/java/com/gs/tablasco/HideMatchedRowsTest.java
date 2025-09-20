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
public class HideMatchedRowsTest {

    public final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy().withHideMatchedRows(true);

    @Test
    void allRowsMatch() throws IOException {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        this.tableVerifier.verify("name", table, table);
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">2 matched rows...</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void alwaysShowMatchedRowsFor() throws IOException {
        final VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        TableTestUtils.assertAssertionError(() -> tableVerifier
                .withAlwaysShowMatchedRowsFor("name2")
                .verify(
                        TableTestUtils.toNamedTables("name", table, "name2", table, "name3", table),
                        TableTestUtils.toNamedTables("name", table, "name2", table, "name4", table)));
        assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>alwaysShowMatchedRowsFor</h1>
                        <div id="alwaysShowMatchedRowsFor.name">
                        <h2>name</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">2 matched rows...</td>
                        </tr>
                        </table>
                        </div>
                        <div id="alwaysShowMatchedRowsFor.name2">
                        <h2>name2</h2>
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
                        </table>
                        </div>
                        <div id="alwaysShowMatchedRowsFor.name3">
                        <h2>name3</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="missing">Col 1<p>Missing</p>
                        </th>
                        <th class="missing">Col 2<p>Missing</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="missing">A1<p>Missing</p>
                        </td>
                        <td class="missing">A2<p>Missing</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="missing">B1<p>Missing</p>
                        </td>
                        <td class="missing">B2<p>Missing</p>
                        </td>
                        </tr>
                        </table>
                        </div>
                        <div id="alwaysShowMatchedRowsFor.name4">
                        <h2>name4</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="surplus">Col 1<p>Surplus</p>
                        </th>
                        <th class="surplus">Col 2<p>Surplus</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="surplus">A1<p>Surplus</p>
                        </td>
                        <td class="surplus">A2<p>Surplus</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="surplus">B1<p>Surplus</p>
                        </td>
                        <td class="surplus">B2<p>Surplus</p>
                        </td>
                        </tr>
                        </table>
                        </div>
                        </body>""",
                TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    void outOfOrderRowMatch() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "B1", "B2", "A1", "A2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="outoforder">B1<p>Out of order</p>
                        </td>
                        <td class="outoforder">B2<p>Out of order</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">1 matched row...</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void allRowsFail() throws IOException {
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
    void missingSurplusColumns() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3", "B1", "B2", "B3", "C1", "C2", "C3");
        final VerifiableTable table2 = TableTestUtils.createTable(
                3, "Col 3", "Col 4", "Col 1", "A3", "A2", "A1", "B3", "B2", "B9", "C3", "C2", "C1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="outoforder">Col 3<p>Out of order</p>
                        </th>
                        <th class="surplus">Col 4<p>Surplus</p>
                        </th>
                        <th class="pass">Col 1</th>
                        <th class="missing">Col 2<p>Missing</p>
                        </th>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="4">1 matched row...</td>
                        </tr>
                        <tr>
                        <td class="outoforder">B3<p>Out of order</p>
                        </td>
                        <td class="surplus">B2<p>Surplus</p>
                        </td>
                        <td class="fail">B1<p>Expected</p>
                        <hr/>B9<p>Actual</p>
                        </td>
                        <td class="missing">B2<p>Missing</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="4">1 matched row...</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    void someRowsMatch() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2", "F1", "F2", "G1", "G2",
                "H1", "H2");
        final VerifiableTable table2 = TableTestUtils.createTable(
                2, "Col 1", "Col 2", "A1", "A2", "B1", "X2", "C1", "C2", "D1", "D2", "E1", "X2", "F1", "F2", "G1", "G2",
                "H1", "H2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">1 matched row...</td>
                        </tr>
                        <tr>
                        <td class="pass">B1</td>
                        <td class="fail">B2<p>Expected</p>
                        <hr/>X2<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">2 matched rows...</td>
                        </tr>
                        <tr>
                        <td class="pass">E1</td>
                        <td class="fail">E2<p>Expected</p>
                        <hr/>X2<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">3 matched rows...</td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}
