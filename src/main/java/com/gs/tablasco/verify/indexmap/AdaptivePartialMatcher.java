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
import com.gs.tablasco.verify.CellComparator;
import com.gs.tablasco.verify.ColumnComparators;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.block.factory.Comparators;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.tuple.Tuples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

public class AdaptivePartialMatcher implements PartialMatcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AdaptivePartialMatcher.class);

    private final VerifiableTable actualData;
    private final VerifiableTable expectedData;
    private final ColumnComparators columnComparators;
    private final long bestMatchThreshold;

    public AdaptivePartialMatcher(VerifiableTable actualData, VerifiableTable expectedData, ColumnComparators columnComparators, int bestMatchThreshold)
    {
        this.actualData = actualData;
        this.expectedData = expectedData;
        this.columnComparators = columnComparators;
        this.bestMatchThreshold = (long) bestMatchThreshold;
    }

    @Override
    public void match(MutableList<UnmatchedIndexMap> allMissingRows, MutableList<UnmatchedIndexMap> allSurplusRows, MutableList<IndexMap> matchedColumns)
    {
        this.groupAndMatch(allMissingRows, allSurplusRows, matchedColumns, null, 0);
    }

    private void groupAndMatch(MutableList<UnmatchedIndexMap> missingRows, MutableList<UnmatchedIndexMap> surplusRows, MutableList<IndexMap> matchedColumns, MutableList<IndexMap> columnsOrderedBySelectivity, int columnIndex)
    {
        if ((long) missingRows.size() * (long) surplusRows.size() <= this.bestMatchThreshold)
        {
            LOGGER.debug("Matching {} missing and {} surplus rows using best-match algorithm", missingRows.size(), surplusRows.size());
            new BestMatchPartialMatcher(this.actualData, this.expectedData, this.columnComparators).match(missingRows, surplusRows, matchedColumns);
            return;
        }
        MutableList<IndexMap> initializedColumnsOrderedBySelectivity = columnsOrderedBySelectivity;
        if (columnIndex == 0)
        {
            initializedColumnsOrderedBySelectivity = getColumnsOrderedBySelectivity(missingRows, surplusRows, matchedColumns);
        }
        if (columnIndex >= initializedColumnsOrderedBySelectivity.size())
        {
            LOGGER.info("Matching remaining {} missing and {} surplus rows using best-match algorithm", missingRows.size(), surplusRows.size());
            new BestMatchPartialMatcher(this.actualData, this.expectedData, this.columnComparators).match(missingRows, surplusRows, matchedColumns);
            return;
        }
        IndexMap column = initializedColumnsOrderedBySelectivity.get(columnIndex);
        LOGGER.info("Grouping by '{}' column", this.actualData.getColumnName(column.getActualIndex()));
        CellComparator expectedComparator = this.columnComparators.getComparator(expectedData.getColumnName(column.getExpectedIndex()));
        MutableListMultimap<String, UnmatchedIndexMap> missingRowsByColumn = missingRows.groupBy(Functions.chain(expectedValueFunction(column), expectedComparator.getFormatter()));
        CellComparator actualComparator = this.columnComparators.getComparator(actualData.getColumnName(column.getActualIndex()));
        MutableListMultimap<String, UnmatchedIndexMap> surplusRowsByColumn = surplusRows.groupBy(Functions.chain(actualValueFunction(column), actualComparator.getFormatter()));
        for (String key : missingRowsByColumn.keysView())
        {
            LOGGER.debug("Matching '{}'", key);
            MutableList<UnmatchedIndexMap> missingByKey = missingRowsByColumn.get(key);
            MutableList<UnmatchedIndexMap> surplusByKey = surplusRowsByColumn.get(key);
            if (surplusByKey != null)
            {
                groupAndMatch(missingByKey, surplusByKey, matchedColumns, initializedColumnsOrderedBySelectivity, columnIndex + 1);
            }
        }
    }

    private Function<UnmatchedIndexMap, Object> actualValueFunction(final IndexMap column)
    {
        return new Function<UnmatchedIndexMap, Object>()
        {
            @Override
            public Object valueOf(UnmatchedIndexMap object)
            {
                return AdaptivePartialMatcher.this.actualData.getValueAt(object.getActualIndex(), column.getActualIndex());
            }
        };
    }

    private Function<UnmatchedIndexMap, Object> expectedValueFunction(final IndexMap column)
    {
        return new Function<UnmatchedIndexMap, Object>()
        {
            @Override
            public Object valueOf(UnmatchedIndexMap object)
            {
                return AdaptivePartialMatcher.this.expectedData.getValueAt(object.getExpectedIndex(), column.getExpectedIndex());
            }
        };
    }

    private MutableList<IndexMap> getColumnsOrderedBySelectivity(MutableList<UnmatchedIndexMap> allMissingRows, MutableList<UnmatchedIndexMap> allSurplusRows, MutableList<IndexMap> columnIndices)
    {
        LOGGER.info("Calculating column selectivity");
        MutableList<Pair<IndexMap, Integer>> columnSelectivities = Lists.mutable.of();
        for (IndexMap column : columnIndices)
        {
            CellComparator expectedComparator = this.columnComparators.getComparator(expectedData.getColumnName(column.getExpectedIndex()));
            Set<String> expectedValues = getColumnValues(allMissingRows, Functions.chain(expectedValueFunction(column), expectedComparator.getFormatter()));
            CellComparator actualComparator = this.columnComparators.getComparator(actualData.getColumnName(column.getActualIndex()));
            Set<String> actualValues = getColumnValues(allSurplusRows, Functions.chain(actualValueFunction(column), actualComparator.getFormatter()));
            actualValues.retainAll(expectedValues);
            int selectivity = actualValues.size();
            if (selectivity > 0)
            {
                columnSelectivities.add(Tuples.pair(column, Integer.valueOf(selectivity)));
            }
        }
        return columnSelectivities
                .sortThis(Comparators.reverse(Comparators.byFunction(Functions.<Integer>secondOfPair())))
                .collect(Functions.<IndexMap>firstOfPair());
    }

    private static Set<String> getColumnValues(MutableList<UnmatchedIndexMap> rows, Function<UnmatchedIndexMap, String> valueFunction)
    {
        if (!CellComparator.isFloatingPoint(valueFunction.valueOf(rows.getFirst())))
        {
            Set<String> values = UnifiedSet.newSet();
            rows.collect(valueFunction, values);
            return values;
        }
        return Collections.emptySet();
    }
}
