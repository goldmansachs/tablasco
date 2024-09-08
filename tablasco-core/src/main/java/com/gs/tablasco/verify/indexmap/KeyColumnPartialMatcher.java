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

package com.gs.tablasco.verify.indexmap;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.KeyedVerifiableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyColumnPartialMatcher implements PartialMatcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyColumnPartialMatcher.class);
    private final KeyedVerifiableTable actualData;
    private final VerifiableTable expectedData;
    private final ColumnComparators columnComparators;
    private final PartialMatcher keyGroupPartialMatcher;

    KeyColumnPartialMatcher(KeyedVerifiableTable actualData, VerifiableTable expectedData, ColumnComparators columnComparators, PartialMatcher keyGroupPartialMatcher)
    {
        this.actualData = actualData;
        this.expectedData = expectedData;
        this.columnComparators = columnComparators;
        this.keyGroupPartialMatcher = keyGroupPartialMatcher;
    }

    @Override
    public void match(List<UnmatchedIndexMap> allMissingRows, List<UnmatchedIndexMap> allSurplusRows, List<IndexMap> matchedColumns)
    {
        List<IndexMap> keyColumnIndices = this.getKeyColumnIndexMaps(matchedColumns);
        if (keyColumnIndices.isEmpty())
        {
            LOGGER.warn("No key columns found!");
            return;
        }
        Map<RowView, List<UnmatchedIndexMap>> missingByKey = new HashMap<>(allMissingRows.size());
        for (UnmatchedIndexMap expected : allMissingRows)
        {
            ExpectedRowView expectedRowView = new ExpectedRowView(this.expectedData, keyColumnIndices, this.columnComparators, expected.getExpectedIndex());
            missingByKey.computeIfAbsent(expectedRowView, rowView -> new ArrayList<>(4)).add(expected);
        }
        Map<RowView, List<UnmatchedIndexMap>> surplusByKey = new HashMap<>(allSurplusRows.size());
        for (UnmatchedIndexMap actual : allSurplusRows)
        {
            ActualRowView actualRowView = new ActualRowView(this.actualData, keyColumnIndices, this.columnComparators, actual.getActualIndex());
            surplusByKey.computeIfAbsent(actualRowView, rowView -> new ArrayList<>(4)).add(actual);
        }
        missingByKey.forEach((rowView, unmatchedIndexMaps) ->
        {
            List<UnmatchedIndexMap> missing = missingByKey.get(rowView);
            List<UnmatchedIndexMap> surplus = surplusByKey.get(rowView);
            if (missing != null && !missing.isEmpty() && surplus != null && !surplus.isEmpty())
            {
                keyGroupPartialMatcher.match(missing, surplus, matchedColumns);
            }
        });
    }

    private List<IndexMap> getKeyColumnIndexMaps(List<IndexMap> columnIndices)
    {
        List<IndexMap> keyColumns = new ArrayList<>(columnIndices.size());
        for (IndexMap columnIndexMap : columnIndices)
        {
            if (columnIndexMap.isMatched() && this.actualData.isKeyColumn(columnIndexMap.getActualIndex()))
            {
                keyColumns.add(columnIndexMap);
            }
        }
        return keyColumns;
    }
}