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
import java.util.Collections;

public class HideMatchedRowsTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy()
            .withHideMatchedRows(true);

    @Test
    public void allRowsMatch() throws IOException
    {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        this.tableVerifier.verify(Collections.singletonMap("name", table), Collections.singletonMap("name", table));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">2 matched rows...</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void alwaysShowMatchedRowsFor() throws IOException
    {
        final VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withAlwaysShowMatchedRowsFor("name2").verify(
                TableTestUtils.tripletonMap("name", table, "name2", table, "name3", table),
                TableTestUtils.tripletonMap("name", table, "name2", table, "name4", table)));
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\"/>\n" +
                "<h1>alwaysShowMatchedRowsFor</h1>\n" +
                "<div id=\"alwaysShowMatchedRowsFor.name\">\n" +
                "<h2>name</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">2 matched rows...</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"alwaysShowMatchedRowsFor.name2\">\n" +
                "<h2>name2</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "<td class=\"pass\">A2</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">B1</td>\n" +
                "<td class=\"pass\">B2</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"alwaysShowMatchedRowsFor.name3\">\n" +
                "<h2>name3</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"missing\">Col 1<p>Missing</p>\n" +
                "</th>\n" +
                "<th class=\"missing\">Col 2<p>Missing</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">A1<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">A2<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">B1<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">B2<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"alwaysShowMatchedRowsFor.name4\">\n" +
                "<h2>name4</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"surplus\">Col 1<p>Surplus</p>\n" +
                "</th>\n" +
                "<th class=\"surplus\">Col 2<p>Surplus</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">A1<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"surplus\">A2<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">B1<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"surplus\">B2<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void outOfOrderRowMatch() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "B1", "B2", "A1", "A2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(Collections.singletonMap("name", table1), Collections.singletonMap("name", table2)));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"outoforder\">B1<p>Out of order</p>\n" +
                "</td>\n" +
                "<td class=\"outoforder\">B2<p>Out of order</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">1 matched row...</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void allRowsFail() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A9", "B1", "B9");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(Collections.singletonMap("name", table1), Collections.singletonMap("name", table2)));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "<td class=\"fail\">A2<p>Expected</p>\n" +
                "<hr/>A9<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">B1</td>\n" +
                "<td class=\"fail\">B2<p>Expected</p>\n" +
                "<hr/>B9<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void missingSurplusColumns() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3", "B1", "B2", "B3", "C1", "C2", "C3");
        final VerifiableTable table2 = TableTestUtils.createTable(3, "Col 3", "Col 4", "Col 1", "A3", "A2", "A1", "B3", "B2", "B9", "C3", "C2", "C1");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(Collections.singletonMap("name", table1), Collections.singletonMap("name", table2)));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"outoforder\">Col 3<p>Out of order</p>\n" +
                "</th>\n" +
                "<th class=\"surplus\">Col 4<p>Surplus</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"missing\">Col 2<p>Missing</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"4\">1 matched row...</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"outoforder\">B3<p>Out of order</p>\n" +
                "</td>\n" +
                "<td class=\"surplus\">B2<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"fail\">B1<p>Expected</p>\n" +
                "<hr/>B9<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">B2<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"4\">1 matched row...</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void someRowsMatch() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2", "F1", "F2", "G1", "G2", "H1", "H2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "X2", "C1", "C2", "D1", "D2", "E1", "X2", "F1", "F2", "G1", "G2", "H1", "H2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(Collections.singletonMap("name", table1), Collections.singletonMap("name", table2)));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">1 matched row...</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">B1</td>\n" +
                "<td class=\"fail\">B2<p>Expected</p>\n" +
                "<hr/>X2<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">2 matched rows...</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">E1</td>\n" +
                "<td class=\"fail\">E2<p>Expected</p>\n" +
                "<hr/>X2<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">3 matched rows...</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}