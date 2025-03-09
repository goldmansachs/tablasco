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

public class IgnoreTablesTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy();

    @Test
    public void ignoreTables() throws IOException
    {
        VerifiableTable tableA = TableTestUtils.createTable(1, "Col 1", "A");
        VerifiableTable tableX = TableTestUtils.createTable(1, "Col 1", "X");
        this.tableVerifier.withIgnoreTables("table1", "table3").verify(
                TableTestUtils.toNamedTables("table1", tableA, "table2", tableA, "table3", tableX),
                TableTestUtils.toNamedTables("table1", tableX, "table2", tableA, "table3", tableA));

        Assert.assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>ignoreTables</h1>
                        <div id="ignoreTables.table2">
                        <h2>table2</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="pass">A</td>
                        </tr>
                        </table>
                        </div>
                        </body>""", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }
}