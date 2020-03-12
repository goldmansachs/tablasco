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

public class IgnoreMissingAndSurplusTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy();

    @Test
    public void allRowsMatch() throws IOException
    {
        VerifiableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table, table);
        Assert.assertEquals(
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
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void failsWithSurplusAndToldToIgnoreJustMissing() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");

        TableTestUtils.assertAssertionError(() -> tableVerifier.withIgnoreMissingRows().verify("name", table1, table2));

        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">C1<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"surplus\">C2<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">B1</td>\n" +
                "<td class=\"pass\">B2</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void failsWithMissingAndToldToIgnoreJustSurplus() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");

        TableTestUtils.assertAssertionError(() -> tableVerifier.withIgnoreSurplusRows().verify("name", table1, table2));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">A1<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">A2<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">B1</td>\n" +
                "<td class=\"pass\">B2</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void passesWithSurplusAndMissing() throws IOException
    {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">B1</td>\n" +
                "<td class=\"pass\">B2</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void failsWithMissingSurplusHeader() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 3", "C1", "C2", "B1", "B2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"surplus\">Col 3<p>Surplus</p>\n" +
                "</th>\n" +
                "<th class=\"missing\">Col 2<p>Missing</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">B1</td>\n" +
                "<td class=\"surplus\">B2<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">B2<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void failsWithDifferenceInCommonRow() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B3");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">B1</td>\n" +
                "<td class=\"fail\">B2<p>Expected</p>\n" +
                "<hr/>B3<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void passesWithEmptyExpected() throws IOException
    {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void passesWithEmptyActual() throws IOException
    {
        VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2", "B1", "B2");
        VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2");
        this.tableVerifier.withIgnoreMissingRows().withIgnoreSurplusRows().verify("name", table1, table2);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void ignoreSurplusColumnsPassesWithSurplus() throws IOException
    {
        VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        VerifiableTable actual = TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3");
        this.tableVerifier.withIgnoreSurplusColumns().verify("name", expected, actual);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 3</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "<td class=\"pass\">A3</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void ignoreSurplusColumnsFailsWithMissing() throws IOException
    {
        final VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        final VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withIgnoreSurplusColumns().verify("name", expected, actual));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"missing\">Col 3<p>Missing</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "<td class=\"missing\">A3<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void ignoreMissingColumnsPassesWithMissing() throws IOException
    {
        VerifiableTable expected = TableTestUtils.createTable(3, "Col 1", "Col 2", "Col 3", "A1", "A2", "A3");
        VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        this.tableVerifier.withIgnoreMissingColumns().verify("name", expected, actual);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 3</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "<td class=\"pass\">A3</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void ignoreMissingColumnsFailsWithSurplus() throws IOException
    {
        final VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withIgnoreMissingColumns().verify("name", expected, actual));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"surplus\">Col 3<p>Surplus</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "<td class=\"surplus\">A3<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void ignoreMissingAndSurplusColumnsPasses() throws IOException
    {
        VerifiableTable expected = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        VerifiableTable actual = TableTestUtils.createTable(2, "Col 1", "Col 3", "A1", "A3");
        this.tableVerifier.withIgnoreMissingColumns().withIgnoreSurplusColumns().verify("name", expected, actual);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}