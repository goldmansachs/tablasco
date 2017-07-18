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

package com.gs.tablasco.verify;

import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.junit.*;
import org.junit.rules.TestName;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class MultiTableVerifierTest
{
    private static final CellComparator CELL_COMPARATOR = new ToleranceCellComparator(new CellFormatter(1.0, false));

    @Rule
    public final TestName testName = new TestName();

    private MultiTableVerifier verifier;
    private File resultsFile;
    private int expectedTables;

    @Before
    public void setUp()
    {
        this.resultsFile = new File(TableTestUtils.getOutputDirectory(), MultiTableVerifierTest.class.getSimpleName() + '_' + this.testName.getMethodName() + ".html");
        this.resultsFile.delete();
        ColumnComparators columnComparators = new ColumnComparators.Builder().build();
        this.verifier = new MultiTableVerifier(new IndexMapTableVerifier(columnComparators, true, IndexMapTableVerifier.DEFAULT_BEST_MATCH_THRESHOLD, false, false));
    }

    @After
    public void tearDown() throws IOException, SAXException, NoSuchMethodException
    {
        Class<? extends Throwable> expected = this.getClass().getMethod(this.testName.getMethodName()).getAnnotation(Test.class).expected();
        if (Test.None.class.equals(expected))
        {
            Assert.assertTrue(this.resultsFile.exists());
            Document html = TableTestUtils.parseHtml(this.resultsFile);
            Assert.assertEquals(this.expectedTables, html.getElementsByTagName("table").getLength());
        }
    }

    @Test
    public void missingTable()
    {
        Map<String, ResultTable> results = verifyTables(createTables("assets"), createTables("assets", "liabs"));
        Assert.assertEquals(newPassTable(), results.get("assets").getVerifiedRows());
        Assert.assertEquals(newMissingTable(), results.get("liabs").getVerifiedRows());
        this.expectedTables = 2;
    }

    @Test
    public void surplusTable()
    {
        Map<String, ResultTable> results = this.verifyTables(createTables("assets", "liabs"), createTables("liabs"));
        Assert.assertEquals(newSurplusTable(), results.get("assets").getVerifiedRows());
        Assert.assertEquals(newPassTable(), results.get("liabs").getVerifiedRows());
        this.expectedTables = 2;
    }

    @Test
    public void misnamedTable()
    {
        Map<String, ResultTable> results = this.verifyTables(createTables("assets", "liabs"), createTables("assets", "liabz"));
        Assert.assertEquals(newPassTable(), results.get("assets").getVerifiedRows());
        Assert.assertEquals(newSurplusTable(), results.get("liabs").getVerifiedRows());
        Assert.assertEquals(newMissingTable(), results.get("liabz").getVerifiedRows());
        this.expectedTables = 3;
    }

    @Test(expected = IllegalStateException.class)
    public void noExpectedColumns()
    {
        this.verifyTables(
                Maps.fixedSize.of("table", TableTestUtils.createTable(1, "Col")),
                Maps.fixedSize.of("table", TableTestUtils.createTable(0)));
    }

    @Test(expected = IllegalStateException.class)
    public void noActualColumns()
    {
        this.verifyTables(
                Maps.fixedSize.of("table", TableTestUtils.createTable(0)),
                Maps.fixedSize.of("table", TableTestUtils.createTable(1, "Col")));
    }

    private Map<String, ResultTable> verifyTables(Map<String, VerifiableTable> actualResults, Map<String, VerifiableTable> expectedResults)
    {
        Map<String, ResultTable> results = this.verifier.verifyTables(expectedResults, actualResults);
        HtmlFormatter htmlFormatter = new HtmlFormatter(this.resultsFile, new HtmlOptions(false, HtmlFormatter.DEFAULT_ROW_LIMIT, false, false, false, Collections.<String>emptySet()));
        htmlFormatter.appendResults(this.testName.getMethodName(), results, null);
        return results;
    }

    private MutableList<MutableList<ResultCell>> newMissingTable()
    {
        return FastList.<MutableList<ResultCell>>newListWith(
                FastList.newListWith(ResultCell.createMissingCell(CELL_COMPARATOR.getFormatter(), "Heading")),
                FastList.newListWith(ResultCell.createMissingCell(CELL_COMPARATOR.getFormatter(), "Value")));
    }

    private MutableList<MutableList<ResultCell>> newSurplusTable()
    {
        return FastList.<MutableList<ResultCell>>newListWith(
                FastList.newListWith(ResultCell.createSurplusCell(CELL_COMPARATOR.getFormatter(), "Heading")),
                FastList.newListWith(ResultCell.createSurplusCell(CELL_COMPARATOR.getFormatter(), "Value")));
    }

    private static FastList<MutableList<ResultCell>> newPassTable()
    {
        return FastList.<MutableList<ResultCell>>newListWith(
                FastList.newListWith(ResultCell.createMatchedCell(CELL_COMPARATOR, "Heading", "Heading")),
                FastList.newListWith(ResultCell.createMatchedCell(CELL_COMPARATOR, "Value", "Value")));
    }

    private static Map<String, VerifiableTable> createTables(String... names)
    {
        Map<String, VerifiableTable> tables = UnifiedMap.newMap();
        for (String name : names)
        {
            tables.put(name, new VerifiableTable()
            {
                @Override
                public int getRowCount()
                {
                    return 1;
                }

                @Override
                public int getColumnCount()
                {
                    return 1;
                }

                @Override
                public String getColumnName(int columnIndex)
                {
                    return "Heading";
                }

                @Override
                public Object getValueAt(int rowIndex, int columnIndex)
                {
                    return "Value";
                }
            });
        }
        return tables;
    }

}
