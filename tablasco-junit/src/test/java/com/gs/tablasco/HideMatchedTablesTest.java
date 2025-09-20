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
public class HideMatchedTablesTest {

    public final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy().withHideMatchedTables(true);

    @Test
    void matchedTablesAreHidden() throws IOException {
        final VerifiableTable matchTable = new TestTable("Col").withRow("A");
        final VerifiableTable outOfOrderTableExpected = new TestTable("Col 1", "Col 2").withRow("A", "B");
        final VerifiableTable outOfOrderTableActual = new TestTable("Col 2", "Col 1").withRow("B", "A");
        final VerifiableTable breakTableExpected = new TestTable("Col").withRow("A");
        final VerifiableTable breakTableActual = new TestTable("Col").withRow("B");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(
                TableTestUtils.toNamedTables(
                        "match", matchTable, "break", breakTableExpected, "outOfOrder", outOfOrderTableExpected),
                TableTestUtils.toNamedTables(
                        "match", matchTable, "break", breakTableActual, "outOfOrder", outOfOrderTableActual)));
        assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>matchedTablesAreHidden</h1>
                        <div id="matchedTablesAreHidden.break">
                        <h2>break</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col</th>
                        </tr>
                        <tr>
                        <td class="surplus">B<p>Surplus</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="missing">A<p>Missing</p>
                        </td>
                        </tr>
                        </table>
                        </div>
                        <div id="matchedTablesAreHidden.outOfOrder">
                        <h2>outOfOrder</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="outoforder">Col 2<p>Out of order</p>
                        </th>
                        <th class="pass">Col 1</th>
                        </tr>
                        <tr>
                        <td class="outoforder">B<p>Out of order</p>
                        </td>
                        <td class="pass">A</td>
                        </tr>
                        </table>
                        </div>
                        </body>""",
                TableTestUtils.getHtml(this.tableVerifier, "body"));
    }
}
