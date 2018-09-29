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

import com.gs.tablasco.verify.DefaultVerifiableTableAdapter;
import com.gs.tablasco.verify.ListVerifiableTable;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.utility.ArrayIterate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RebaseAndVerifyTest
{
    private static final String ADAPT = "adapt";
    private static final Function<VerifiableTable, VerifiableTable> ACTUAL_ADAPTER = new Function<VerifiableTable, VerifiableTable>()
    {
        @Override
        public VerifiableTable valueOf(VerifiableTable table)
        {
            return new DefaultVerifiableTableAdapter(table)
            {
                @Override
                public Object getValueAt(int rowIndex, int columnIndex)
                {
                    Object valueAt = super.getValueAt(rowIndex, columnIndex);
                    return valueAt == ADAPT ? ADAPT.toUpperCase() : valueAt;
                }
            };
        }
    };

    @Rule
    public final TableTestUtils.TestDescription description = new TableTestUtils.TestDescription();

    private final File expectedDir = new File(TableTestUtils.getOutputDirectory(), "expected");
    private final File outputDir = TableTestUtils.getOutputDirectory();
    private Document outputHtml;
    private File expectedFile;
    private String[] baselineHeaders = new String[] { "foo", "bar" };

    @After
    public void assertMetadata() throws IOException
    {
        int rebaseMetadataCount = 0;
        BufferedReader reader = new BufferedReader(new FileReader(this.expectedFile));
        try
        {
            String line = reader.readLine();
            while (line != null)
            {
                if (line.contains("Metadata Key"))
                {
                    rebaseMetadataCount++;
                }
                line = reader.readLine();
            }
        }
        finally
        {
            reader.close();
        }
        Assert.assertEquals(1, rebaseMetadataCount);

        if (this.outputHtml != null)
        {
            NodeList italics = this.outputHtml.getElementsByTagName("i");
            Assert.assertEquals(1, italics.getLength());
            Assert.assertTrue(italics.item(0).getFirstChild().getNodeValue().contains("Metadata Key"));
        }
    }

    @Test
    public void rebasingOfMultipleDataTypes() throws Exception
    {
        VerifiableTable table = createTypedTable("Hello World!", "11000", 21000.0d, 31000.0f, 41000L, 51000, new Date(Timestamp.valueOf("1993-02-16 23:59:00.0").getTime()));
        this.rebase(Maps.fixedSize.of("tableName", table));
        this.verify(Maps.fixedSize.of("tableName", table), 5.0d);
        this.verifyHtmlCells("Hello World!", "11000", "21,000", "31,000", "41,000", "51,000", "1993-02-16 23:59:00");
    }

    @Test(expected = AssertionError.class)
    public void toleranceNotAppliedToIntegers() throws Exception
    {
        VerifiableTable tableForRebase = createTypedTable(21000.0d, 31000.0f, 41000L, 51000);
        VerifiableTable tableForVerify = createTypedTable(21003.0d, 31003.0f, 41003L, 51003);
        this.rebase(Maps.fixedSize.of("tableName", tableForRebase));
        this.verify(Maps.fixedSize.of("tableName", tableForVerify), 5.0d);
    }

    @Test
    public void specialNumberHandling() throws Exception
    {
        VerifiableTable tableForRebase = TableTestUtils.createTable(6,
                "Double Inf", "Double Neg Inf", "Double NaN", "Float Inf", "Float Neg Inf", "Float NaN",
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN,
                Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN);
        this.rebase(Maps.fixedSize.of("tableName", tableForRebase));
        this.verify(Maps.fixedSize.of("tableName", tableForRebase), 1.0d);
        char infinity = '\u221E';
        this.verifyHtmlCells(String.valueOf(infinity), "-" + infinity, "NaN", String.valueOf(infinity), "-" + infinity, "NaN");
    }

    @Test
    public void withActualAdapter() throws Exception
    {
        VerifiableTable table = TableTestUtils.createTable(2, "Key", "Val", "1", ADAPT);
        this.rebase(Maps.fixedSize.of("tableName", table));
        this.verify(Maps.fixedSize.of("tableName", table), 1.0d);
        this.verifyHtmlCells("1", "ADAPT");
    }

    @Test
    public void formattingAppliedToAllNumbersButToleranceOnlyToFloatingPoint() throws Exception
    {
        VerifiableTable tableForRebase = createTypedTable(21000.0d, 31000.0f, 41000L, 51000);
        VerifiableTable tableForVerify = createTypedTable(21003.0d, 31003.0f, 41000L, 51000);
        this.rebase(Maps.fixedSize.of("tableName", tableForRebase));
        this.verify(Maps.fixedSize.of("tableName", tableForVerify), 5.0d);
        this.verifyHtmlCells("21,003", "31,003", "41,000", "51,000");
    }

    @Test
    public void backslashHandling() throws Exception
    {
        VerifiableTable table = createTypedTable("Foo\\Bar");
        this.rebase(Maps.fixedSize.of("Side-by-side - Nettable\\NonNettable", table));
        this.verify(Maps.fixedSize.of("Side-by-side - Nettable\\NonNettable", table), 1.0d);
    }

    @Test
    public void tableOrderControlledExternally() throws Exception
    {
        Map<String, VerifiableTable> tables = new LinkedHashMap<String, VerifiableTable>();
        tables.put("tableC", createTypedTable("C"));
        tables.put("tableA", createTypedTable("A"));
        tables.put("tableB", createTypedTable("B"));
        this.rebase(tables);
        this.verify(tables, 1.0d);
        Assert.assertEquals(Arrays.asList("tableC", "tableA", "tableB"), getHtmlTagValues("h2"));
    }

    @Test
    public void multipleVerifyCalls() throws Exception
    {
        Map<String, VerifiableTable> tables = new LinkedHashMap<String, VerifiableTable>();
        tables.put("tableC", createTypedTable("C"));
        tables.put("tableA", createTypedTable("A"));
        tables.put("tableB", createTypedTable("B"));
        TableVerifier rebaseWatcher = this.newTableVerifier(1.0d).withRebase();
        rebaseWatcher.starting(this.description.get());
        for (String name : tables.keySet())
        {
            rebaseWatcher.verify(name, tables.get(name));
        }
        this.finishRebase(rebaseWatcher);

        TableVerifier runWatcher = this.newTableVerifier(1.0d);
        runWatcher.starting(this.description.get());
        for (String name : tables.keySet())
        {
            runWatcher.verify(name, tables.get(name));
        }
        runWatcher.succeeded(this.description.get());
        this.outputHtml = TableTestUtils.parseHtml(runWatcher.getOutputFile());

        Assert.assertEquals(Arrays.asList("tableC", "tableA", "tableB"), getHtmlTagValues("h2"));
    }

    @Test
    public void withExcludeSvnHeadersInExpectedResults() throws Exception
    {
        this.baselineHeaders = null;
        VerifiableTable table = TableTestUtils.createTable(2, "Key", "Val", "1", "A");
        this.rebase(Maps.fixedSize.of("tableName", table));
        try (BufferedReader reader = new BufferedReader(new FileReader(this.expectedFile)))
        {
            String firstLine = reader.readLine();
            Assert.assertTrue(firstLine.startsWith("Metadata"));
        }
        this.verify(Maps.fixedSize.of("tableName", table), 1.0d);
        this.verifyHtmlCells("1", "A");
    }

    @Test
    public void svnHeadersAreIgnoredByReader() throws Exception
    {
        VerifiableTable table = TableTestUtils.createTable(2, "Key", "Val", "1", "A");
        this.rebase(Maps.fixedSize.of("tableName", table));
        this.baselineHeaders = null;
        this.verify(Maps.fixedSize.of("tableName", table), 1.0d);
        this.verifyHtmlCells("1", "A");
    }

    private void verifyHtmlCells(String... expectedHtml)
    {
        Assert.assertEquals(Arrays.asList(expectedHtml), getHtmlTagValues("td"));
    }

    private void rebase(Map<String, VerifiableTable> actualForRebasing)
    {
        TableVerifier rebaseWatcher = this.newTableVerifier(1.0d).withRebase();
        rebaseWatcher.starting(this.description.get());
        rebaseWatcher.verify(actualForRebasing);
        this.finishRebase(rebaseWatcher);
    }

    private void finishRebase(final TableVerifier rebaseWatcher)
    {
        TableTestUtils.assertAssertionError(new Runnable()
        {
            @Override
            public void run()
            {
                rebaseWatcher.succeeded(description.get());
            }
        });
        this.expectedFile = rebaseWatcher.getExpectedFile();
    }

    private void verify(Map<String, VerifiableTable> actualForVerification, double tolerance) throws IOException, SAXException
    {
        TableVerifier runWatcher = this.newTableVerifier(tolerance);
        runWatcher.starting(this.description.get());
        runWatcher.verify(actualForVerification);
        runWatcher.succeeded(this.description.get());
        File outputFile = runWatcher.getOutputFile();
        this.outputHtml = TableTestUtils.parseHtml(outputFile);
    }

    private TableVerifier newTableVerifier(double tolerance)
    {
        return new TableVerifier()
                .withExpectedDir(this.expectedDir)
                .withOutputDir(this.outputDir)
                .withFilePerMethod()
                .withTolerance(tolerance)
                .withActualAdapter(ACTUAL_ADAPTER)
                .withMetadata("Metadata Key", "Metadata Value")
                .withBaselineHeaders(this.baselineHeaders);
    }

    private List<String> getHtmlTagValues(String tagName)
    {
        NodeList tdNodes = this.outputHtml.getElementsByTagName(tagName);
        List<String> tdValues = FastList.newList(tdNodes.getLength());
        for (int i = 0; i < tdNodes.getLength(); i++)
        {
            tdValues.add(tdNodes.item(i).getFirstChild().getNodeValue());
        }
        return tdValues;
    }

    // TODO: add test that uses decimals + varying tolerances (for double vs. long comparisons)

    // TODO: do we need timezone test for Dates?

    // TODO: write test for MAX & MIN & Nan of each data type (converting to double in the comparison will not work for big longs)

    private static VerifiableTable createTypedTable(Object... values)
    {
        MutableList<Object> headers = ArrayIterate.collect(values, new Function<Object, Object>()
        {
            @Override
            public Object valueOf(Object object)
            {
                return object.getClass().getSimpleName();
            }
        });
        return new ListVerifiableTable(headers, Arrays.asList(Arrays.<Object>asList(values)));
    }
}