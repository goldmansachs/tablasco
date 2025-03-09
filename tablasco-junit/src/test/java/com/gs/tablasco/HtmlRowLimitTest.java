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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class HtmlRowLimitTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy()
            .withHtmlRowLimit(3);

    @Test
    public void tablesMatch() throws IOException
    {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        this.tableVerifier.verify("name", table, table);
        Assert.assertEquals(
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
                        <tr>
                        <td class="pass">C1</td>
                        <td class="pass">C2</td>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">2 more rows...</td>
                        </tr>
                        </table>""", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void tablesDoNotMatch() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "DX", "E1", "E2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        Assert.assertEquals(
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
                        <tr>
                        <td class="pass">C1</td>
                        <td class="pass">C2</td>
                        </tr>
                        <tr>
                        <td class="fail multi" colspan="2">2 more rows...</td>
                        </tr>
                        </table>""", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void hideMatchedRows() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "AX", "B1", "B2", "C1", "C2", "D1", "DX", "E1", "E2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withHideMatchedRows(true).verify("name", table1, table2));
        Assert.assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass">A1</td>
                        <td class="fail">A2<p>Expected</p>
                        <hr/>AX<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">2 matched rows...</td>
                        </tr>
                        <tr>
                        <td class="pass">D1</td>
                        <td class="fail">D2<p>Expected</p>
                        <hr/>DX<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="fail multi" colspan="2">1 more row...</td>
                        </tr>
                        </table>""", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void hideMatchedRows2() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "CX", "D1", "DX");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withHtmlRowLimit(1).withHideMatchedRows(true).verify("name", table1, table2));
        Assert.assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        <th class="pass">Col 2</th>
                        </tr>
                        <tr>
                        <td class="pass multi" colspan="2">2 matched rows...</td>
                        </tr>
                        <tr>
                        <td class="fail multi" colspan="2">2 more rows...</td>
                        </tr>
                        </table>""", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}