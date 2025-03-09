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
import com.gs.tablasco.core.HtmlConfig;
import com.gs.tablasco.core.VerifierConfig;
import com.gs.tablasco.verify.*;

import java.io.File;
import java.util.*;
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

    Watson(File outputFile)
    {
        this.outputFile = outputFile;
        VerifierConfig verifierConfig = new VerifierConfig().withTolerance(1.0).withVerifyRowOrder(false);
        this.multiTableVerifier = new MultiTableVerifier(verifierConfig);
    }

    List<Object> assist(String levelName, InvestigationLevel nextLevel, int drilldownLimit)
    {
        VerifiableTable[] queryResults = execute(nextLevel);
        VerifiableTable actualResults = new KeyedVerifiableTableAdapter(queryResults[0], queryResults[0].getColumnCount() - 1);
        VerifiableTable expectedResults = new KeyedVerifiableTableAdapter(queryResults[1], queryResults[1].getColumnCount() - 1);

        List<String> actualColumns = getColumns(actualResults);
        List<String> expectedColumns = getColumns(expectedResults);
        if (!getLast(actualColumns).equals(getLast(expectedColumns)))
        {
            throw new IllegalArgumentException(String.format("Key columns must match at each investigation level [actual: %s, expected: %s]", getLast(actualColumns), getLast(expectedColumns)));
        }
        Set<String> commonColumns = new HashSet<>(actualColumns);
        commonColumns.retainAll(expectedColumns);
        if (Math.min(actualColumns.size(), expectedColumns.size()) > 1 && commonColumns.size() < 2)
        {
            throw new IllegalArgumentException(String.format("There must be at least 2 matching columns at each investigation level [actual: %s, expected: %s]", getLast(actualColumns), getLast(expectedColumns)));
        }

        String levelDescription = nextLevel.getLevelDescription();
        ResultTable results = this.findBreaks(levelDescription, actualResults, expectedResults);
        HtmlConfig htmlConfig = new HtmlConfig().withHideMatchedRows(true);
        HtmlFormatter htmlFormatter = new HtmlFormatter(this.outputFile, htmlConfig);
        htmlFormatter.appendResults(levelName, Collections.singletonMap(levelDescription, results), Metadata.newEmpty());
        return getRowKeys(results, drilldownLimit);
    }

    private static <T> T getLast(List<T> list)
    {
        return list.get(list.size() - 1);
    }

    private List<Object> getRowKeys(ResultTable results, int drilldownLimit)
    {
        List<Object> rowKeys = new ArrayList<>();
        List<List<ResultCell>> table = results.getVerifiedRows();
        int rowIndex = 1;
        while (rowIndex < table.size() && rowKeys.size() < drilldownLimit)
        {
            List<ResultCell> values = table.get(rowIndex);
            int passedCount = (int) values.stream().filter(ResultCell.IS_PASSED_CELL).count();
            int failedCount = (int) values.stream().filter(ResultCell.IS_FAILED_CELL).count();
            if (passedCount == 0 || failedCount > 0)
            {
                ResultCell cell = getLast(values);
                rowKeys.add(cell.getActual() == null ? cell.getExpected() : cell.getActual());
            }
            rowIndex++;
        }
        return rowKeys;
    }

    private List<String> getColumns(VerifiableTable table)
    {
        List<String> cols = new ArrayList<>(table.getColumnCount());
        for (int i = 0; i < table.getColumnCount(); i++)
        {
            cols.add(table.getColumnName(i));
        }
        return cols;
    }

    private ResultTable findBreaks(String tableName, VerifiableTable actual, VerifiableTable expected)
    {
        Map<String, VerifiableTable> actualVerifiableTableResults = Collections.singletonMap(tableName, actual);
        Map<String, VerifiableTable> expectedVerifiableTableResults = Collections.singletonMap(tableName, expected);
        Map<String, ResultTable> verifyTables = this.multiTableVerifier.verifyTables(expectedVerifiableTableResults, actualVerifiableTableResults);
        return verifyTables.values().iterator().next();
    }

    private static VerifiableTable[] execute(InvestigationLevel investigationLevel)
    {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try
        {
            Future<VerifiableTable> actualFuture = executorService.submit(investigationLevel.getActualResults());
            Future<VerifiableTable> expectedFuture = executorService.submit(investigationLevel.getExpectedResults());
            return new VerifiableTable[] { actualFuture.get(), expectedFuture.get() };
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
