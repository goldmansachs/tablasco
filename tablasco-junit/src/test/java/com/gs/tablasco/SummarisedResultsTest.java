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

public class SummarisedResultsTest {
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy()
            .withSummarisedResults(true);

    @Test
    public void summarisedResults() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(2,
                "key", "v1",
                "d", "4",
                "d", "4",
                "d", "4",
                "d", "4",
                "e", "5",
                "e", "5",
                "e", "5",
                "e", "5"
        );
        final VerifiableTable table2 = TableTestUtils.createTable(2,
                "key", "v1",
                "d", "4",
                "d", "4",
                "d", "4",
                "d", "4",
                "e", "x",
                "e", "x",
                "e", "x",
                "e", "x"
        );
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(TableTestUtils.doubletonMap("name1", table1, "name2", table1), TableTestUtils.doubletonMap("name1", table2, "name2", table2)));
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\"/>\n" +
                "<h1>summarisedResults</h1>\n" +
                "<div id=\"summarisedResults.name1\">\n" +
                "<h2>name1</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">key</th>\n" +
                "<th class=\"pass\">v1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"blank_row\" colspan=\"2\"/>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">d</td>\n" +
                "<td class=\"pass\">4</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">d</td>\n" +
                "<td class=\"pass\">4</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">d</td>\n" +
                "<td class=\"pass\">4</td>\n" +
                "</tr>\n" +
                "<tr onclick=\"toggleVisibility('summarisedResults.name1.summaryRow0')\">\n" +
                "<td class=\"summary\" colspan=\"2\">\n" +
                "<a class=\"link\">1 more matched row...</a>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr id=\"summarisedResults.name1.summaryRow0\" style=\"display:none\">\n" +
                "<td class=\"summary small\">d <span class=\"grey\">- </span>\n" +
                "<span class=\"italic blue\">1 row</span>\n" +
                "<br/>\n" +
                "</td>\n" +
                "<td class=\"summary small\">4 <span class=\"grey\">- </span>\n" +
                "<span class=\"italic blue\">1 row</span>\n" +
                "<br/>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"blank_row\" colspan=\"2\"/>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">e</td>\n" +
                "<td class=\"fail\">5<p>Expected</p>\n" +
                "<hr/>x<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">e</td>\n" +
                "<td class=\"fail\">5<p>Expected</p>\n" +
                "<hr/>x<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">e</td>\n" +
                "<td class=\"fail\">5<p>Expected</p>\n" +
                "<hr/>x<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr onclick=\"toggleVisibility('summarisedResults.name1.summaryRow1')\">\n" +
                "<td class=\"summary\" colspan=\"2\">\n" +
                "<a class=\"link\">1 more break like this...</a>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr id=\"summarisedResults.name1.summaryRow1\" style=\"display:none\">\n" +
                "<td class=\"summary small\">e <span class=\"grey\">- </span>\n" +
                "<span class=\"italic blue\">1 row</span>\n" +
                "<br/>\n" +
                "</td>\n" +
                "<td class=\"summary small\">\n" +
                "<span class=\"grey\">Expected </span>5 <span class=\"grey\">Actual </span>x <span class=\"grey\">- </span>\n" +
                "<span class=\"italic blue\">1 row</span>\n" +
                "<br/>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"summarisedResults.name2\">\n" +
                "<h2>name2</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">key</th>\n" +
                "<th class=\"pass\">v1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"blank_row\" colspan=\"2\"/>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">d</td>\n" +
                "<td class=\"pass\">4</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">d</td>\n" +
                "<td class=\"pass\">4</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">d</td>\n" +
                "<td class=\"pass\">4</td>\n" +
                "</tr>\n" +
                "<tr onclick=\"toggleVisibility('summarisedResults.name2.summaryRow0')\">\n" +
                "<td class=\"summary\" colspan=\"2\">\n" +
                "<a class=\"link\">1 more matched row...</a>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr id=\"summarisedResults.name2.summaryRow0\" style=\"display:none\">\n" +
                "<td class=\"summary small\">d <span class=\"grey\">- </span>\n" +
                "<span class=\"italic blue\">1 row</span>\n" +
                "<br/>\n" +
                "</td>\n" +
                "<td class=\"summary small\">4 <span class=\"grey\">- </span>\n" +
                "<span class=\"italic blue\">1 row</span>\n" +
                "<br/>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"blank_row\" colspan=\"2\"/>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">e</td>\n" +
                "<td class=\"fail\">5<p>Expected</p>\n" +
                "<hr/>x<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">e</td>\n" +
                "<td class=\"fail\">5<p>Expected</p>\n" +
                "<hr/>x<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">e</td>\n" +
                "<td class=\"fail\">5<p>Expected</p>\n" +
                "<hr/>x<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr onclick=\"toggleVisibility('summarisedResults.name2.summaryRow1')\">\n" +
                "<td class=\"summary\" colspan=\"2\">\n" +
                "<a class=\"link\">1 more break like this...</a>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr id=\"summarisedResults.name2.summaryRow1\" style=\"display:none\">\n" +
                "<td class=\"summary small\">e <span class=\"grey\">- </span>\n" +
                "<span class=\"italic blue\">1 row</span>\n" +
                "<br/>\n" +
                "</td>\n" +
                "<td class=\"summary small\">\n" +
                "<span class=\"grey\">Expected </span>5 <span class=\"grey\">Actual </span>x <span class=\"grey\">- </span>\n" +
                "<span class=\"italic blue\">1 row</span>\n" +
                "<br/>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }
}