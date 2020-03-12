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

import com.gs.tablasco.verify.KeyedVerifiableTableAdapter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class HideMatchedColumnsTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy()
            .withHideMatchedColumns(true);

    @Test
    public void allColumnsMatch() throws IOException
    {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        this.tableVerifier.verify("name", table, table);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass multi\" title=\"2 matched columns\">...</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void missingAndSurplusRows() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(3,
                "Col 1", "Col 2", "Col 3",
                "A1", "A2", "A3",
                "B1", "B2", "B3",
                "C1", "C2", "C3");
        final VerifiableTable table2 = TableTestUtils.createTable(3,
                "Col 1", "Col 2", "Col 3",
                "B1", "B2", "B9",
                "C1", "C2", "C9",
                "D1", "D2", "D3");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass multi\" title=\"2 matched columns\">...</th>\n" +
                "<th class=\"pass\">Col 3</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">\u00A0</td>\n" +
                "<td class=\"missing\">A3<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "<td class=\"fail\">B3<p>Expected</p>\n" +
                "<hr/>B9<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "<td class=\"fail\">C3<p>Expected</p>\n" +
                "<hr/>C9<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">\u00A0</td>\n" +
                "<td class=\"surplus\">D3<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void multiMatchedColumns() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(8,
                "Col 1", "Col 2", "Col 3", "Col 4", "Col 5", "Col 6", "Col 7", "Col 8",
                "A", "A", "A", "A", "A", "A", "A", "A",
                "B", "B", "B", "B", "B", "B", "B", "B");
        final VerifiableTable table2 = TableTestUtils.createTable(8,
                "Col 1", "Col 2", "Col 3", "Col 4", "Col 5", "Col 6", "Col 7", "Col 8",
                "A", "A", "A", "A", "A", "X", "A", "A",
                "B", "B", "X", "B", "B", "B", "B", "B");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass multi\" title=\"2 matched columns\">...</th>\n" +
                "<th class=\"pass\">Col 3</th>\n" +
                "<th class=\"pass multi\" title=\"2 matched columns\">...</th>\n" +
                "<th class=\"pass\">Col 6</th>\n" +
                "<th class=\"pass multi\" title=\"2 matched columns\">...</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "<td class=\"pass\">A</td>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "<td class=\"fail\">A<p>Expected</p>\n" +
                "<hr/>X<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "<td class=\"fail\">B<p>Expected</p>\n" +
                "<hr/>X<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "<td class=\"pass\">B</td>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void keyColumnIgnored() throws IOException
    {
        VerifiableTable table = new KeyedVerifiableTableAdapter(TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A", "A", "A"), 0);
        this.tableVerifier.verify("name", table, table);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass multi\" title=\"2 matched columns\">...</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A</td>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void matchedRowsAndColumns() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(4,
                "Col 1", "Col 2", "Col 3", "Col 3",
                "A", "A", "A", "A",
                "B", "B", "B", "B",
                "C", "C", "C", "C",
                "D", "D", "D", "D");
        final VerifiableTable table2 = TableTestUtils.createTable(4,
                "Col 1", "Col 2", "Col 3", "Col 3",
                "A", "A", "A", "X",
                "B", "B", "B", "B",
                "C", "C", "C", "C",
                "X", "D", "D", "D");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withHideMatchedRows(true).verify("name", table1, table2));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass multi\" title=\"2 matched columns\">...</th>\n" +
                "<th class=\"pass\">Col 3</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A</td>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "<td class=\"fail\">A<p>Expected</p>\n" +
                "<hr/>X<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"4\">2 matched rows...</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">D<p>Expected</p>\n" +
                "<hr/>X<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">\u00A0</td>\n" +
                "<td class=\"pass\">D</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}