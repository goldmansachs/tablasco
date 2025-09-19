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

public class SummarisedResultsTest {
    @Rule
    public final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy().withSummarisedResults(true);

    @Test
    void summarisedResults() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                2, "key", "v1", "d", "4", "d", "4", "d", "4", "d", "4", "e", "5", "e", "5", "e", "5", "e", "5");
        final VerifiableTable table2 = TableTestUtils.createTable(
                2, "key", "v1", "d", "4", "d", "4", "d", "4", "d", "4", "e", "x", "e", "x", "e", "x", "e", "x");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(
                TableTestUtils.toNamedTables("name1", table1, "name2", table1),
                TableTestUtils.toNamedTables("name1", table2, "name2", table2)));
        assertEquals(
                """
                        <body>
                        <div class="metadata"/>
                        <h1>summarisedResults</h1>
                        <div id="summarisedResults.name1">
                        <h2>name1</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">key</th>
                        <th class="pass">v1</th>
                        </tr>
                        <tr>
                        <td class="blank_row" colspan="2"/>
                        </tr>
                        <tr>
                        <td class="pass">d</td>
                        <td class="pass">4</td>
                        </tr>
                        <tr>
                        <td class="pass">d</td>
                        <td class="pass">4</td>
                        </tr>
                        <tr>
                        <td class="pass">d</td>
                        <td class="pass">4</td>
                        </tr>
                        <tr onclick="toggleVisibility('summarisedResults.name1.summaryRow0')">
                        <td class="summary" colspan="2">
                        <a class="link">1 more matched row...</a>
                        </td>
                        </tr>
                        <tr id="summarisedResults.name1.summaryRow0" style="display:none">
                        <td class="summary small">d
                        <span class="grey">- </span>
                        <span class="italic blue">1 row</span>
                        <br/>
                        </td>
                        <td class="summary small">4
                        <span class="grey">- </span>
                        <span class="italic blue">1 row</span>
                        <br/>
                        </td>
                        </tr>
                        <tr>
                        <td class="blank_row" colspan="2"/>
                        </tr>
                        <tr>
                        <td class="pass">e</td>
                        <td class="fail">5<p>Expected</p>
                        <hr/>x<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass">e</td>
                        <td class="fail">5<p>Expected</p>
                        <hr/>x<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass">e</td>
                        <td class="fail">5<p>Expected</p>
                        <hr/>x<p>Actual</p>
                        </td>
                        </tr>
                        <tr onclick="toggleVisibility('summarisedResults.name1.summaryRow1')">
                        <td class="summary" colspan="2">
                        <a class="link">1 more break like this...</a>
                        </td>
                        </tr>
                        <tr id="summarisedResults.name1.summaryRow1" style="display:none">
                        <td class="summary small">e
                        <span class="grey">- </span>
                        <span class="italic blue">1 row</span>
                        <br/>
                        </td>
                        <td class="summary small"><span class="grey">Expected </span>
                        5
                        <span class="grey">Actual </span>
                        x
                        <span class="grey">- </span>
                        <span class="italic blue">1 row</span>
                        <br/>
                        </td>
                        </tr>
                        </table>
                        </div>
                        <div id="summarisedResults.name2">
                        <h2>name2</h2>
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">key</th>
                        <th class="pass">v1</th>
                        </tr>
                        <tr>
                        <td class="blank_row" colspan="2"/>
                        </tr>
                        <tr>
                        <td class="pass">d</td>
                        <td class="pass">4</td>
                        </tr>
                        <tr>
                        <td class="pass">d</td>
                        <td class="pass">4</td>
                        </tr>
                        <tr>
                        <td class="pass">d</td>
                        <td class="pass">4</td>
                        </tr>
                        <tr onclick="toggleVisibility('summarisedResults.name2.summaryRow0')">
                        <td class="summary" colspan="2">
                        <a class="link">1 more matched row...</a>
                        </td>
                        </tr>
                        <tr id="summarisedResults.name2.summaryRow0" style="display:none">
                        <td class="summary small">d
                        <span class="grey">- </span>
                        <span class="italic blue">1 row</span>
                        <br/>
                        </td>
                        <td class="summary small">4
                        <span class="grey">- </span>
                        <span class="italic blue">1 row</span>
                        <br/>
                        </td>
                        </tr>
                        <tr>
                        <td class="blank_row" colspan="2"/>
                        </tr>
                        <tr>
                        <td class="pass">e</td>
                        <td class="fail">5<p>Expected</p>
                        <hr/>x<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass">e</td>
                        <td class="fail">5<p>Expected</p>
                        <hr/>x<p>Actual</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="pass">e</td>
                        <td class="fail">5<p>Expected</p>
                        <hr/>x<p>Actual</p>
                        </td>
                        </tr>
                        <tr onclick="toggleVisibility('summarisedResults.name2.summaryRow1')">
                        <td class="summary" colspan="2">
                        <a class="link">1 more break like this...</a>
                        </td>
                        </tr>
                        <tr id="summarisedResults.name2.summaryRow1" style="display:none">
                        <td class="summary small">e
                        <span class="grey">- </span>
                        <span class="italic blue">1 row</span>
                        <br/>
                        </td>
                        <td class="summary small"><span class="grey">Expected </span>
                        5
                        <span class="grey">Actual </span>
                        x
                        <span class="grey">- </span>
                        <span class="italic blue">1 row</span>
                        <br/>
                        </td>
                        </tr>
                        </table>
                        </div>
                        </body>""",
                TableTestUtils.getHtml(this.tableVerifier, "body"));
    }
}
