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

package com.gs.tablasco.investigation;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.HtmlFormatter;
import com.gs.tablasco.verify.KeyedVerifiableTableAdapter;
import com.gs.tablasco.verify.Metadata;
import com.gs.tablasco.verify.MultiTableVerifier;
import com.gs.tablasco.verify.ResultCell;
import com.gs.tablasco.verify.ResultTable;
import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.Iterate;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Watson "assists" Sherlock in reconciling two environments for one level of group bys,
 * returning the drilldown keys of breaks only.
 */
class Watson
{
    private final MultiTableVerifier multiTableVerifier;
    private final File outputFile;

    public Watson(File outputFile)
    {
        this.outputFile = outputFile;
        ColumnComparators columnComparators = new ColumnComparators.Builder().withTolerance(1.0).build();
        this.multiTableVerifier = new MultiTableVerifier(columnComparators, new IndexMapTableVerifier(false, IndexMapTableVerifier.DEFAULT_BEST_MATCH_THRESHOLD));
    }

    public List<Object> assist(String levelName, InvestigationLevel nextLevel, int drilldownLimit)
    {
        Twin<VerifiableTable> queryResults = execute(nextLevel);
        VerifiableTable actualResults = new KeyedVerifiableTableAdapter(queryResults.getOne(), queryResults.getOne().getColumnCount() - 1);
        VerifiableTable expectedResults = new KeyedVerifiableTableAdapter(queryResults.getTwo(), queryResults.getTwo().getColumnCount() - 1);

        List<String> actualColumns = getColumns(actualResults);
        List<String> expectedColumns = getColumns(expectedResults);
        if (!Iterate.getLast(actualColumns).equals(Iterate.getLast(expectedColumns)))
        {
            throw new IllegalArgumentException(String.format("Key columns must match at each investigation level [actual: %s, expected: %s]", Iterate.getLast(actualColumns), Iterate.getLast(expectedColumns)));
        }
        Set<String> commonColumns = UnifiedSet.newSet(actualColumns);
        commonColumns.retainAll(expectedColumns);
        if (Math.min(actualColumns.size(), expectedColumns.size()) > 1 && commonColumns.size() < 2)
        {
            throw new IllegalArgumentException(String.format("There must be at least 2 matching columns at each investigation level [actual: %s, expected: %s]", Iterate.getLast(actualColumns), Iterate.getLast(expectedColumns)));
        }

        String levelDescription = nextLevel.getLevelDescription();
        ResultTable results = this.findBreaks(levelDescription, actualResults, expectedResults);
        HtmlFormatter htmlFormatter = new HtmlFormatter(outputFile, Sets.fixedSize.of(levelDescription), false, false, HtmlFormatter.DEFAULT_ROW_LIMIT);
        htmlFormatter.appendResults(levelName, Maps.fixedSize.of(levelDescription, results), Metadata.newEmpty());
        return getRowKeys(results, drilldownLimit);
    }

    private List<Object> getRowKeys(ResultTable results, int drilldownLimit)
    {
        List<Object> rowKeys = FastList.newList();
        List<List<ResultCell>> table = results.getVerifiedRows();
        int rowIndex = 1;
        while (rowIndex < table.size() && rowKeys.size() < drilldownLimit)
        {
            List<ResultCell> values = table.get(rowIndex);
            int passedCount = Iterate.count(values, ResultCell.IS_PASSED_CELL);
            int failedCount = Iterate.count(values, ResultCell.IS_FAILED_CELL);
            if (passedCount == 0 || failedCount > 0)
            {
                ResultCell cell = Iterate.getLast(values);
                rowKeys.add(cell.getActual() == null ? cell.getExpected() : cell.getActual());
            }
            rowIndex++;
        }
        return rowKeys;
    }

    private List<String> getColumns(VerifiableTable table)
    {
        List<String> cols = FastList.newList(table.getColumnCount());
        for (int i = 0; i < table.getColumnCount(); i++)
        {
            cols.add(table.getColumnName(i));
        }
        return cols;
    }

    private ResultTable findBreaks(String tableName, VerifiableTable actual, VerifiableTable expected)
    {
        Map<String, VerifiableTable> actualVerifiableTableResults = Maps.fixedSize.of(tableName, actual);
        Map<String, VerifiableTable> expectedVerifiableTableResults = Maps.fixedSize.of(tableName, expected);
        Map<String, ResultTable> verifyTables = this.multiTableVerifier.verifyTables(expectedVerifiableTableResults, actualVerifiableTableResults);
        return Iterate.getOnly(verifyTables.values());
    }

    private static Twin<VerifiableTable> execute(InvestigationLevel investigationLevel)
    {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try
        {
            Future<VerifiableTable> actualFuture = executorService.submit(investigationLevel.getActualResults());
            Future<VerifiableTable> expectedFuture = executorService.submit(investigationLevel.getExpectedResults());
            return Tuples.twin(actualFuture.get(), expectedFuture.get());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error running queries", e);
        }
        finally
        {
            executorService.shutdown();
        }
    }
}
