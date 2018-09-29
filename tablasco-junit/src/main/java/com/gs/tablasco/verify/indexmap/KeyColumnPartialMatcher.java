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
import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.utility.Iterate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KeyColumnPartialMatcher implements PartialMatcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyColumnPartialMatcher.class);
    public static final Function0<MutableList<UnmatchedIndexMap>> NEW_LIST = new Function0<MutableList<UnmatchedIndexMap>>()
    {
        @Override
        public MutableList<UnmatchedIndexMap> value()
        {
            return FastList.newList(4);
        }
    };
    private final KeyedVerifiableTable actualData;
    private final VerifiableTable expectedData;
    private final ColumnComparators columnComparators;
    private final PartialMatcher keyGroupPartialMatcher;

    public KeyColumnPartialMatcher(KeyedVerifiableTable actualData, VerifiableTable expectedData, ColumnComparators columnComparators, PartialMatcher keyGroupPartialMatcher)
    {
        this.actualData = actualData;
        this.expectedData = expectedData;
        this.columnComparators = columnComparators;
        this.keyGroupPartialMatcher = keyGroupPartialMatcher;
    }

    @Override
    public void match(MutableList<UnmatchedIndexMap> allMissingRows, MutableList<UnmatchedIndexMap> allSurplusRows, MutableList<IndexMap> matchedColumns)
    {
        List<IndexMap> keyColumnIndices = this.getKeyColumnIndexMaps(matchedColumns);
        if (keyColumnIndices.isEmpty())
        {
            LOGGER.warn("No key columns found!");
            return;
        }
        MutableMap<RowView, MutableList<UnmatchedIndexMap>> missingByKey = UnifiedMap.newMap(allMissingRows.size());
        for (UnmatchedIndexMap expected : allMissingRows)
        {
            ExpectedRowView expectedRowView = new ExpectedRowView(this.expectedData, keyColumnIndices, this.columnComparators, expected.getExpectedIndex());
            missingByKey.getIfAbsentPut(expectedRowView, NEW_LIST).add(expected);
        }
        MutableMap<RowView, MutableList<UnmatchedIndexMap>> surplusByKey = UnifiedMap.newMap(allSurplusRows.size());
        for (UnmatchedIndexMap actual : allSurplusRows)
        {
            ActualRowView actualRowView = new ActualRowView(this.actualData, keyColumnIndices, this.columnComparators, actual.getActualIndex());
            surplusByKey.getIfAbsentPut(actualRowView, NEW_LIST).add(actual);
        }
        for (RowView rowView : missingByKey.keysView())
        {
            MutableList<UnmatchedIndexMap> missing = missingByKey.get(rowView);
            MutableList<UnmatchedIndexMap> surplus = surplusByKey.get(rowView);
            if (Iterate.notEmpty(missing) && Iterate.notEmpty(surplus))
            {
                this.keyGroupPartialMatcher.match(missing, surplus, matchedColumns);
            }
        }
    }

    private List<IndexMap> getKeyColumnIndexMaps(List<IndexMap> columnIndices)
    {
        List<IndexMap> keyColumns = FastList.newList(columnIndices.size());
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