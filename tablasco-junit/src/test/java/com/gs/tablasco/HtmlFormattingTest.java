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

import org.eclipse.collections.impl.factory.Maps;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class HtmlFormattingTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy();

    @Test
    public void nonNumericBreak() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A9", "B1", "B9");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
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
    public void numericBreak() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 10.123);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 20.456);
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withTolerance(0.01d).verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A</td>\n" +
                "<td class=\"fail number\">10.12<p>Expected</p>\n" +
                "<hr/>20.46<p>Actual</p>\n" +
                "<hr/>-10.33 / 102.07%<p>Difference / Variance</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void nonNumericActualNumericExpectedBreak() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 390.0);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", "A2");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withVarianceThreshold(5.0d).verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A</td>\n" +
                "<td class=\"fail\">390<p>Expected</p>\n" +
                "<hr/>A2<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void numericActualNonNumericExpectedBreak() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", "A1");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A", 48.0);
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withTolerance(0.1d).verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A</td>\n" +
                "<td class=\"fail\">A1<p>Expected</p>\n" +
                "<hr/>48<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void outOfOrderColumnPassedCellNonNumeric() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", "A2", "A1");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"outoforder\">Col 2<p>Out of order</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"outoforder\">A2<p>Out of order</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void outOfOrderColumnFailedCellNonNumeric() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", "A3", "A1");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"outoforder\">Col 2<p>Out of order</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">A2<p>Expected</p>\n" +
                "<hr/>A3<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }


    @Test
    public void outOfOrderColumnPassedCellNumeric() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", 25, 30.78);
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withTolerance(0.01d).verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"outoforder\">Col 2<p>Out of order</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"outoforder number\">25<p>Out of order</p>\n" +
                "</td>\n" +
                "<td class=\"pass number\">30.78</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void outOfOrderColumnFailedCellNumeric() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 2", "Col 1", 25.3, 30.78);
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withTolerance(0.01d).verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"outoforder\">Col 2<p>Out of order</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail number\">25<p>Expected</p>\n" +
                "<hr/>25.3<p>Actual</p>\n" +
                "<hr/>-0.3 / 1.2%<p>Difference / Variance</p>\n" +
                "</td>\n" +
                "<td class=\"pass number\">30.78</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void missingSurplusColumnsNonNumeric() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 4", "Col 1", "A2", "A1");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"surplus\">Col 4<p>Surplus</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"missing\">Col 2<p>Missing</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">A2<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "<td class=\"missing\">A2<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +

                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void missingSurplusColumnsNumeric() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 30.78, 25);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 4", "Col 1", 26, 30.78);
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withTolerance(0.01d).verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"surplus\">Col 4<p>Surplus</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"missing\">Col 2<p>Missing</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus number\">26<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"pass number\">30.78</td>\n" +
                "<td class=\"missing number\">25<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }


    @Test
    public void missingSurplusRowsNonNumeric() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "C1", "C2");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
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
                "<td class=\"missing\">A1<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">A2<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }


    @Test
    public void missingSurplusRowsNumeric() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", 345.66, 13.0, 56.44, 45.01);
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", 345.63, 12.8, 56.65, 45.31);
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withTolerance(0.1d).verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass number\">345.6</td>\n" +
                "<td class=\"fail number\">13<p>Expected</p>\n" +
                "<hr/>12.8<p>Actual</p>\n" +
                "<hr/>0.2 / -1.5%<p>Difference / Variance</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus number\">56.6<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"surplus number\">45.3<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing number\">56.4<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing number\">45<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void assertionSummaryWithSuccess() throws IOException
    {
        VerifiableTable table = TableTestUtils.createTable(1, "Col 1", "A1");
        this.tableVerifier.withAssertionSummary(true).verify(Maps.fixedSize.of("name", table), Maps.fixedSize.of("name", table));
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\"/>\n" +
                "<h1>assertionSummaryWithSuccess</h1>\n" +
                "<div>\n" +
                "<h2>Assertions</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">2 right, 0 wrong, 100.0% correct</th>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"assertionSummaryWithSuccess.name\">\n" +
                "<h2>name</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void assertionSummaryWithFailure() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A9", "B1", "B9");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withAssertionSummary(true).verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\"/>\n" +
                "<h1>assertionSummaryWithFailure</h1>\n" +
                "<div>\n" +
                "<h2>Assertions</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"fail\">4 right, 2 wrong, 66.6% correct</th>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"assertionSummaryWithFailure.name\">\n" +
                "<h2>name</h2>\n" +
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
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void assertionSummaryWithMultipleVerify() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(1, "Col 1", "A1");
        final VerifiableTable table2 = TableTestUtils.createTable(1, "Col 1", "A2");
        this.tableVerifier.withAssertionSummary(true).verify(Maps.fixedSize.of("name1", table1), Maps.fixedSize.of("name1", table1));
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withAssertionSummary(true).verify(Maps.fixedSize.of("name2", table1), Maps.fixedSize.of("name2", table2));
            }
        });
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\"/>\n" +
                "<h1>assertionSummaryWithMultipleVerify</h1>\n" +
                "<div>\n" +
                "<h2>Assertions</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">2 right, 0 wrong, 100.0% correct</th>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"assertionSummaryWithMultipleVerify.name1\">\n" +
                "<h2>name1</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div>\n" +
                "<h2>Assertions</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"fail\">1 right, 2 wrong, 33.3% correct</th>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"assertionSummaryWithMultipleVerify.name2\">\n" +
                "<h2>name2</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">A2<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">A1<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void assertionSummaryWithMissingSurplusTables() throws IOException
    {
        final VerifiableTable table = TableTestUtils.createTable(1, "Col 1", "A1");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier.withAssertionSummary(true).verify(
                        Maps.fixedSize.of("name", table, "name2", table), Maps.fixedSize.of("name", table, "name3", table));
            }
        });
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\"/>\n" +
                "<h1>assertionSummaryWithMissingSurplusTables</h1>\n" +
                "<div>\n" +
                "<h2>Assertions</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"fail\">2 right, 4 wrong, 33.3% correct</th>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"assertionSummaryWithMissingSurplusTables.name\">\n" +
                "<h2>name</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"assertionSummaryWithMissingSurplusTables.name2\">\n" +
                "<h2>name2</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"missing\">Col 1<p>Missing</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">A1<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"assertionSummaryWithMissingSurplusTables.name3\">\n" +
                "<h2>name3</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"surplus\">Col 1<p>Surplus</p>\n" +
                "</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">A1<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void assertionSummaryWithHideMatchedRows() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2");
        final VerifiableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C9");
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                tableVerifier
                        .withAssertionSummary(true)
                        .withHideMatchedRows(true)
                        .verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));
            }
        });
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\"/>\n" +
                "<h1>assertionSummaryWithHideMatchedRows</h1>\n" +
                "<div>\n" +
                "<h2>Assertions</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"fail\">7 right, 1 wrong, 87.5% correct</th>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<div id=\"assertionSummaryWithHideMatchedRows.name\">\n" +
                "<h2>name</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 2</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">2 matched rows...</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">C1</td>\n" +
                "<td class=\"fail\">C2<p>Expected</p>\n" +
                "<hr/>C9<p>Actual</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }
}