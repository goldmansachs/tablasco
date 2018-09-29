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

import com.gs.tablasco.investigation.Investigation;
import com.gs.tablasco.investigation.InvestigationLevel;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

public class InvestigationTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy();

    @Test(expected = IllegalArgumentException.class)
    public void keyColumnMismatch()
    {
        Investigation investigation = new SimpleInvestigation(
                "Table",
                TableTestUtils.createTable(2, "A", "K"),
                TableTestUtils.createTable(2, "A", "X"));
        this.tableVerifier.investigate(investigation);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noOtherColumnsMatch()
    {
        Investigation investigation = new SimpleInvestigation(
                "Table",
                TableTestUtils.createTable(2, "A", "K"),
                TableTestUtils.createTable(2, "B", "K"));
        this.tableVerifier.investigate(investigation);
    }

    @Test
    public void investigateFailure() throws IOException
    {
        Investigation investigation = new ComplexInvestigation(Arrays.asList(
                new SimpleInvestigationLevel(
                        "Initial Query",
                        TableTestUtils.createTable(2, "C", "K", "1", "K1", "2", "K2"),
                        TableTestUtils.createTable(2, "C", "K", "9", "K1", "2", "K2")),
                new SimpleInvestigationLevel(
                        "First Drilldown",
                        TableTestUtils.createTable(2, "C", "K", "3", "K3", "9", "K4"),
                        TableTestUtils.createTable(2, "C", "K", "9", "K3", "4", "K4")),
                new SimpleInvestigationLevel(
                        "Second Drilldown",
                        TableTestUtils.createTable(2, "C", "K", "5", "K5", "6", "K6"),
                        TableTestUtils.createTable(2, "C", "K", "5", "K5", "6", "K6"))),
                Arrays.asList(
                        null,
                        Arrays.<Object>asList("K1"),
                        Arrays.<Object>asList("K3", "K4")),
                100);
        try
        {
            this.tableVerifier.investigate(investigation);
        }
        catch (AssertionError e)
        {
            if (!e.getMessage().startsWith("Some tests failed"))
            {
                throw e;
            }
        }
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\">\n" +
                "<i/>\n" +
                "</div>\n" +
                "<h1>Initial Results</h1>\n" +
                "<div id=\"Initial_Results.Initial_Query\">\n" +
                "<h2>Initial Query</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">9<p>Expected</p>\n" +
                "<hr/>1<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">K1</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">1 matched row...</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<h1>Investigation Level 1 (Top 100)</h1>\n" +
                "<div id=\"Investigation_Level_1_Top_100_.First_Drilldown\">\n" +
                "<h2>First Drilldown</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">9<p>Expected</p>\n" +
                "<hr/>3<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">K3</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">4<p>Expected</p>\n" +
                "<hr/>9<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">K4</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<h1>Investigation Level 2 (Top 100)</h1>\n" +
                "<div id=\"Investigation_Level_2_Top_100_.Second_Drilldown\">\n" +
                "<h2>Second Drilldown</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">2 matched rows...</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void missingSurplusColumns() throws IOException
    {
        Investigation investigation = new ComplexInvestigation(Arrays.asList(
                new SimpleInvestigationLevel(
                        "Initial Query",
                        TableTestUtils.createTable(3,
                                "C1", "C2", "K",
                                "1",  "X",  "K1",
                                "2",  "X",  "K2",
                                "3",  "X",  "K3"),
                        TableTestUtils.createTable(2,
                                "C1", "K",
                                "9",  "K1",
                                "2",  "K2")),
                new SimpleInvestigationLevel(
                        "First Drilldown",
                        TableTestUtils.createTable(2,
                                "C1", "K",
                                "9",  "K1",
                                "2",  "K2"),
                        TableTestUtils.createTable(3,
                                "C1", "C2", "K",
                                "1",  "X",  "K1",
                                "2",  "X",  "K2",
                                "3",  "X",  "K3")),
                new SimpleInvestigationLevel(
                        "First Drilldown",
                        TableTestUtils.createTable(2,
                                "C1", "K"),
                        TableTestUtils.createTable(3,
                                "C1", "C2", "K",
                                "1",  "X",  "K1")),
                new SimpleInvestigationLevel(
                        "First Drilldown",
                        TableTestUtils.createTable(2,
                                "C1", "K"),
                        TableTestUtils.createTable(2,
                                "C1", "K"))),
                Arrays.asList(
                        null,
                        Arrays.<Object>asList("K1", "K3"),
                        Arrays.<Object>asList("K1", "K3"),
                        Arrays.<Object>asList("K1")),
                100);
        try
        {
            this.tableVerifier.investigate(investigation);
        }
        catch (AssertionError e)
        {
            if (!e.getMessage().startsWith("Some tests failed"))
            {
                throw e;
            }
        }
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\">\n" +
                "<i/>\n" +
                "</div>\n" +
                "<h1>Initial Results</h1>\n" +
                "<div id=\"Initial_Results.Initial_Query\">\n" +
                "<h2>Initial Query</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C1</th>\n" +
                "<th class=\"surplus\">C2<p>Surplus</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">9<p>Expected</p>\n" +
                "<hr/>1<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"surplus\">X<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">K1</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"3\">1 matched row...</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">3<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"surplus\">X<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"surplus\">K3<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<h1>Investigation Level 1 (Top 100)</h1>\n" +
                "<div id=\"Investigation_Level_1_Top_100_.First_Drilldown\">\n" +
                "<h2>First Drilldown</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C1</th>\n" +
                "<th class=\"missing\">C2<p>Missing</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">1<p>Expected</p>\n" +
                "<hr/>9<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">X<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">K1</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"3\">1 matched row...</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">3<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">X<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">K3<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<h1>Investigation Level 2 (Top 100)</h1>\n" +
                "<div id=\"Investigation_Level_2_Top_100_.First_Drilldown\">\n" +
                "<h2>First Drilldown</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C1</th>\n" +
                "<th class=\"missing\">C2<p>Missing</p>\n" +
                "</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">1<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">X<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing\">K1<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<h1>Investigation Level 3 (Top 100)</h1>\n" +
                "<div id=\"Investigation_Level_3_Top_100_.First_Drilldown\">\n" +
                "<h2>First Drilldown</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C1</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void investigateSuccess() throws IOException
    {
        Investigation investigation = new ComplexInvestigation(Arrays.asList(
                new SimpleInvestigationLevel(
                        "Initial Query",
                        TableTestUtils.createTable(2, "C", "K", "1", "K1", "2", "K2"),
                        TableTestUtils.createTable(2, "C", "K", "1", "K1", "2", "K2")),
                new SimpleInvestigationLevel(
                        "First Drilldown",
                        TableTestUtils.createTable(2, "C", "K", "3", "K3", "9", "K4"),
                        TableTestUtils.createTable(2, "C", "K", "9", "K3", "4", "K4"))),
                Arrays.asList((List<Object>) null),
                100);
        this.tableVerifier.investigate(investigation);
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\">\n" +
                "<i/>\n" +
                "</div>\n" +
                "<h1>Initial Results</h1>\n" +
                "<div id=\"Initial_Results.Initial_Query\">\n" +
                "<h2>Initial Query</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">2 matched rows...</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void investigationFailureIntegerKey() throws IOException
    {
        Investigation investigation = new ComplexInvestigation(Arrays.asList(
                new SimpleInvestigationLevel(
                        "Initial Query",
                        TableTestUtils.createTable(2,
                                "C", "I",
                                "1",  1,
                                "2",  2,
                                "3",  3,
                                "4",  4,
                                "6",  6),
                        TableTestUtils.createTable(2,
                                "C", "I",
                                "9",  2,
                                "3",  3,
                                "4",  9,
                                "5",  5,
                                "6",  6)),
                new SimpleInvestigationLevel(
                        "First Drilldown",
                        TableTestUtils.createTable(1, "K", "K1"),
                        TableTestUtils.createTable(1, "K", "K1"))),
                Arrays.asList(
                        null,
                        Arrays.<Object>asList(1, 2, 9, 4, 5)),
                100);
        try
        {
            this.tableVerifier.investigate(investigation);
        }
        catch (AssertionError e)
        {
            if (!e.getMessage().startsWith("Some tests failed"))
            {
                throw e;
            }
        }
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\">\n" +
                "<i/>\n" +
                "</div>\n" +
                "<h1>Initial Results</h1>\n" +
                "<div id=\"Initial_Results.Initial_Query\">\n" +
                "<h2>Initial Query</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C</th>\n" +
                "<th class=\"pass\">I</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">1<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"surplus number\">1<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">9<p>Expected</p>\n" +
                "<hr/>2<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass number\">2</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">1 matched row...</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">4<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing number\">9<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">4<p>Surplus</p>\n" +
                "</td>\n" +
                "<td class=\"surplus number\">4<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">5<p>Missing</p>\n" +
                "</td>\n" +
                "<td class=\"missing number\">5<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"2\">1 matched row...</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<h1>Investigation Level 1 (Top 100)</h1>\n" +
                "<div id=\"Investigation_Level_1_Top_100_.First_Drilldown\">\n" +
                "<h2>First Drilldown</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"1\">1 matched row...</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
        "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void drilldownLimit() throws IOException
    {
        Investigation investigation = new ComplexInvestigation(Arrays.asList(
                new SimpleInvestigationLevel(
                        "Initial Query",
                        TableTestUtils.createTable(2, "C", "K", "1", "K1", "2", "K2", "3", "K3"),
                        TableTestUtils.createTable(2, "C", "K", "9", "K1", "9", "K2", "9", "K3")),
                new SimpleInvestigationLevel(
                        "First Drilldown",
                        TableTestUtils.createTable(1, "K", "K1"),
                        TableTestUtils.createTable(1, "K", "K1"))),
                Arrays.asList(
                        null,
                        Arrays.<Object>asList("K1", "K2")),
                        2);
        try
        {
            this.tableVerifier.investigate(investigation);
        }
        catch (AssertionError e)
        {
            if (!e.getMessage().startsWith("Some tests failed"))
            {
                throw e;
            }
        }
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\">\n" +
                "<i/>\n" +
                "</div>\n" +
                "<h1>Initial Results</h1>\n" +
                "<div id=\"Initial_Results.Initial_Query\">\n" +
                "<h2>Initial Query</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">C</th>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">9<p>Expected</p>\n" +
                "<hr/>1<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">K1</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">9<p>Expected</p>\n" +
                "<hr/>2<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">K2</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"fail\">9<p>Expected</p>\n" +
                "<hr/>3<p>Actual</p>\n" +
                "</td>\n" +
                "<td class=\"pass\">K3</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<h1>Investigation Level 1 (Top 2)</h1>\n" +
                "<div id=\"Investigation_Level_1_Top_2_.First_Drilldown\">\n" +
                "<h2>First Drilldown</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"1\">1 matched row...</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    @Test
    public void drilldownShortCircuit() throws IOException
    {
        Investigation investigation = new ComplexInvestigation(Arrays.asList(
                new SimpleInvestigationLevel(
                        "Initial Query",
                        TableTestUtils.createTable(1, "K", "K1"),
                        TableTestUtils.createTable(1, "K", "K9")),
                new SimpleInvestigationLevel(
                        "First Drilldown",
                        TableTestUtils.createTable(1, "K", "K1"),
                        TableTestUtils.createTable(1, "K", "K1")),
                new SimpleInvestigationLevel(
                        "Second Drilldown",
                        TableTestUtils.createTable(1, "K", "K1"),
                        TableTestUtils.createTable(1, "K", "K9"))),
                Arrays.asList(
                        null,
                        Arrays.<Object>asList("K1", "K9")),
                100);
        try
        {
            this.tableVerifier.investigate(investigation);
        }
        catch (AssertionError e)
        {
            if (!e.getMessage().startsWith("Some tests failed"))
            {
                throw e;
            }
        }
        Assert.assertEquals(
                "<body>\n" +
                "<div class=\"metadata\">\n" +
                "<i/>\n" +
                "</div>\n" +
                "<h1>Initial Results</h1>\n" +
                "<div id=\"Initial_Results.Initial_Query\">\n" +
                "<h2>Initial Query</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">K1<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">K9<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "<h1>Investigation Level 1 (Top 100)</h1>\n" +
                "<div id=\"Investigation_Level_1_Top_100_.First_Drilldown\">\n" +
                "<h2>First Drilldown</h2>\n" +
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">K</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass multi\" colspan=\"1\">1 matched row...</td>\n" +
                "</tr>\n" +
                "</table>\n" +
                "</div>\n" +
                "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }

    private static class TableCallable implements Callable<VerifiableTable>
    {
        private final VerifiableTable verifiableTable;

        private TableCallable(VerifiableTable verifiableTable)
        {
            this.verifiableTable = verifiableTable;
        }

        @Override
        public VerifiableTable call() throws Exception
        {
            return this.verifiableTable;
        }
    }

    private static class SimpleInvestigation implements Investigation
    {
        private SimpleInvestigationLevel investigationLevel;

        private SimpleInvestigation(String levelDescription, VerifiableTable actualTable, VerifiableTable expectedTable)
        {
            this.investigationLevel = new SimpleInvestigationLevel(levelDescription, actualTable, expectedTable);
        }

        @Override
        public InvestigationLevel getFirstLevel()
        {
            return investigationLevel;
        }

        @Override
        public InvestigationLevel getNextLevel(List<Object> drilldownKeys)
        {
            return null;
        }

        @Override
        public int getRowKeyLimit()
        {
            return 100;
        }
    }

    static class SimpleInvestigationLevel implements InvestigationLevel
    {
        private final String levelDescription;
        private final VerifiableTable actualTable;
        private final VerifiableTable expectedTable;

        SimpleInvestigationLevel(String levelDescription, VerifiableTable actualTable, VerifiableTable expectedTable)
        {
            this.levelDescription = levelDescription;
            this.actualTable = actualTable;
            this.expectedTable = expectedTable;
        }

        @Override
        public Callable<VerifiableTable> getActualResults()
        {
            return new TableCallable(this.actualTable);
        }

        @Override
        public Callable<VerifiableTable> getExpectedResults()
        {
            return new TableCallable(this.expectedTable);
        }

        @Override
        public String getLevelDescription()
        {
            return this.levelDescription;
        }
    }

    private static class ComplexInvestigation implements Investigation
    {
        private int levelIndex = 1;
        private final List<SimpleInvestigationLevel> investigationLevels;
        private final List<List<Object>> expectedKeys;
        private final int drilldownLimit;

        private ComplexInvestigation(List<SimpleInvestigationLevel> investigationLevels, List<List<Object>> expectedKeys, int drilldownLimit)
        {
            this.investigationLevels = investigationLevels;
            this.expectedKeys = expectedKeys;
            this.drilldownLimit = drilldownLimit;
        }

        @Override
        public InvestigationLevel getFirstLevel()
        {
            return this.investigationLevels.get(0);
        }

        @Override
        public InvestigationLevel getNextLevel(List<Object> drilldownKeys)
        {
            Assert.assertEquals(this.expectedKeys.get(this.levelIndex), drilldownKeys);
            final int nextTableIndex = this.levelIndex;
            this.levelIndex++;
            return nextTableIndex < this.investigationLevels.size() ? this.investigationLevels.get(nextTableIndex) : null;
        }

        @Override
        public int getRowKeyLimit()
        {
            return this.drilldownLimit;
        }
    }
}