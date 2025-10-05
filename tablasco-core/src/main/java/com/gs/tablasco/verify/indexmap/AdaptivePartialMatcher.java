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
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdaptivePartialMatcher implements PartialMatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdaptivePartialMatcher.class);

    private final VerifiableTable actualData;
    private final VerifiableTable expectedData;
    private final ColumnComparators columnComparators;
    private final long bestMatchThreshold;

    AdaptivePartialMatcher(
            VerifiableTable actualData,
            VerifiableTable expectedData,
            ColumnComparators columnComparators,
            int bestMatchThreshold) {
        this.actualData = actualData;
        this.expectedData = expectedData;
        this.columnComparators = columnComparators;
        this.bestMatchThreshold = bestMatchThreshold;
    }

    @Override
    public void match(
            List<UnmatchedIndexMap> allMissingRows,
            List<UnmatchedIndexMap> allSurplusRows,
            List<IndexMap> matchedColumns) {
        this.groupAndMatch(allMissingRows, allSurplusRows, matchedColumns, null, 0);
    }

    private void groupAndMatch(
            List<UnmatchedIndexMap> missingRows,
            List<UnmatchedIndexMap> surplusRows,
            List<IndexMap> matchedColumns,
            List<IndexMap> columnsOrderedBySelectivity,
            int columnIndex) {
        if ((long) missingRows.size() * (long) surplusRows.size() <= this.bestMatchThreshold) {
            LOGGER.debug(
                    "Matching {} missing and {} surplus rows using best-match algorithm",
                    missingRows.size(),
                    surplusRows.size());
            new BestMatchPartialMatcher(this.actualData, this.expectedData, this.columnComparators)
                    .match(missingRows, surplusRows, matchedColumns);
            return;
        }
        final List<IndexMap> initializedColumnsOrderedBySelectivity = columnIndex == 0
                ? getColumnsOrderedBySelectivity(missingRows, surplusRows, matchedColumns)
                : columnsOrderedBySelectivity;
        if (columnIndex >= initializedColumnsOrderedBySelectivity.size()) {
            LOGGER.info(
                    "Matching remaining {} missing and {} surplus rows using best-match algorithm",
                    missingRows.size(),
                    surplusRows.size());
            new BestMatchPartialMatcher(this.actualData, this.expectedData, this.columnComparators)
                    .match(missingRows, surplusRows, matchedColumns);
            return;
        }
        IndexMap column = initializedColumnsOrderedBySelectivity.get(columnIndex);
        LOGGER.info("Grouping by '{}' column", this.actualData.getColumnName(column.getActualIndex()));

        CellComparator expectedComparator =
                this.columnComparators.getComparator(expectedData.getColumnName(column.getExpectedIndex()));
        Map<String, List<UnmatchedIndexMap>> missingRowsByColumn = new HashMap<>();
        missingRows.forEach(unmatchedIndexMap -> {
            String formatted = getFormattedExpected(column, expectedComparator, unmatchedIndexMap);
            missingRowsByColumn.compute(formatted, addToRows(unmatchedIndexMap));
        });

        CellComparator actualComparator =
                this.columnComparators.getComparator(actualData.getColumnName(column.getActualIndex()));
        Map<String, List<UnmatchedIndexMap>> surplusRowsByColumn = new HashMap<>();
        surplusRows.forEach(unmatchedIndexMap -> {
            Object actual = this.actualData.getValueAt(unmatchedIndexMap.getActualIndex(), column.getActualIndex());
            String formatted = actualComparator.getFormatter().format(actual);
            surplusRowsByColumn.compute(formatted, addToRows(unmatchedIndexMap));
        });

        missingRowsByColumn.forEach((key, unmatchedIndexMaps) -> {
            LOGGER.debug("Matching '{}'", key);
            List<UnmatchedIndexMap> missingByKey = missingRowsByColumn.get(key);
            List<UnmatchedIndexMap> surplusByKey = surplusRowsByColumn.get(key);
            if (surplusByKey != null) {
                groupAndMatch(
                        missingByKey,
                        surplusByKey,
                        matchedColumns,
                        initializedColumnsOrderedBySelectivity,
                        columnIndex + 1);
            }
        });
    }

    private String getFormattedExpected(
            IndexMap column, CellComparator expectedComparator, UnmatchedIndexMap unmatchedIndexMap) {
        Object expected = this.expectedData.getValueAt(unmatchedIndexMap.getExpectedIndex(), column.getExpectedIndex());
        return expectedComparator.getFormatter().format(expected);
    }

    private static BiFunction<String, List<UnmatchedIndexMap>, List<UnmatchedIndexMap>> addToRows(
            UnmatchedIndexMap unmatchedIndexMap) {
        return (key, unmatchedIndexMaps) -> {
            unmatchedIndexMaps = unmatchedIndexMaps == null ? new ArrayList<>() : unmatchedIndexMaps;
            unmatchedIndexMaps.add(unmatchedIndexMap);
            return unmatchedIndexMaps;
        };
    }

    private Function<UnmatchedIndexMap, Object> actualValueFunction(final IndexMap column) {
        return unmatchedIndexMap -> AdaptivePartialMatcher.this.actualData.getValueAt(
                unmatchedIndexMap.getActualIndex(), column.getActualIndex());
    }

    private Function<UnmatchedIndexMap, Object> expectedValueFunction(final IndexMap column) {
        return unmatchedIndexMap -> AdaptivePartialMatcher.this.expectedData.getValueAt(
                unmatchedIndexMap.getExpectedIndex(), column.getExpectedIndex());
    }

    private List<IndexMap> getColumnsOrderedBySelectivity(
            List<UnmatchedIndexMap> allMissingRows,
            List<UnmatchedIndexMap> allSurplusRows,
            List<IndexMap> columnIndices) {
        LOGGER.info("Calculating column selectivity");
        List<ColumnSelectivity> columnSelectivities = new ArrayList<>();
        for (IndexMap column : columnIndices) {
            CellComparator expectedComparator =
                    this.columnComparators.getComparator(expectedData.getColumnName(column.getExpectedIndex()));
            Set<String> expectedValues = getColumnValues(
                    allMissingRows, expectedValueFunction(column).andThen(expectedComparator.getFormatter()));
            CellComparator actualComparator =
                    this.columnComparators.getComparator(actualData.getColumnName(column.getActualIndex()));
            Set<String> actualValues = getColumnValues(
                    allSurplusRows, actualValueFunction(column).andThen(actualComparator.getFormatter()));
            actualValues.retainAll(expectedValues);
            int selectivity = actualValues.size();
            if (selectivity > 0) {
                columnSelectivities.add(new ColumnSelectivity(column, selectivity));
            }
        }
        return columnSelectivities.stream()
                .sorted(Comparator.comparing(ColumnSelectivity::getSelectivity).reversed())
                .map(ColumnSelectivity::getColumn)
                .collect(Collectors.toList());
    }

    private static Set<String> getColumnValues(
            List<UnmatchedIndexMap> rows, Function<UnmatchedIndexMap, String> valueFunction) {
        if (!CellComparator.isFloatingPoint(valueFunction.apply(rows.get(0)))) {
            return rows.stream().map(valueFunction).collect(Collectors.toSet());
        }
        // todo: is this ever hit?
        return Collections.emptySet();
    }

    private static class ColumnSelectivity {
        private final IndexMap column;
        private final int selectivity;

        ColumnSelectivity(IndexMap column, int selectivity) {
            this.column = column;
            this.selectivity = selectivity;
        }

        public IndexMap getColumn() {
            return column;
        }

        int getSelectivity() {
            return selectivity;
        }
    }
}
