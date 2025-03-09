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
import com.gs.tablasco.verify.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMapTableVerifier implements SingleTableVerifier {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexMapTableVerifier.class);
    public static final int DEFAULT_BEST_MATCH_THRESHOLD = 1000000;
    public static final long DEFAULT_PARTIAL_MATCH_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5L);
    private final ColumnComparators columnComparators;
    private final boolean verifyRowOrder;
    private final int bestMatchThreshold;
    private final boolean ignoreSurplusRows;
    private final boolean ignoreMissingRows;
    private final boolean ignoreSurplusColumns;
    private final boolean ignoreMissingColumns;
    private final long partialMatchTimeoutMillis;

    public IndexMapTableVerifier(ColumnComparators columnComparators, boolean verifyRowOrder, int bestMatchThreshold) {
        this(columnComparators, verifyRowOrder, bestMatchThreshold, false, false);
    }

    public IndexMapTableVerifier(
            ColumnComparators columnComparators,
            boolean verifyRowOrder,
            int bestMatchThreshold,
            boolean ignoreSurplusRows,
            boolean ignoreMissingRows) {
        this(
                columnComparators,
                verifyRowOrder,
                bestMatchThreshold,
                ignoreSurplusRows,
                ignoreMissingRows,
                false,
                false,
                DEFAULT_PARTIAL_MATCH_TIMEOUT_MILLIS);
    }

    public IndexMapTableVerifier(
            ColumnComparators columnComparators,
            boolean verifyRowOrder,
            int bestMatchThreshold,
            boolean ignoreSurplusRows,
            boolean ignoreMissingRows,
            boolean ignoreSurplusColumns,
            boolean ignoreMissingColumns,
            long partialMatchTimeoutMillis) {
        this.columnComparators = columnComparators;
        this.verifyRowOrder = verifyRowOrder;
        this.bestMatchThreshold = bestMatchThreshold;
        this.ignoreSurplusRows = ignoreSurplusRows;
        this.ignoreMissingRows = ignoreMissingRows;
        this.ignoreSurplusColumns = ignoreSurplusColumns;
        this.ignoreMissingColumns = ignoreMissingColumns;
        this.partialMatchTimeoutMillis = partialMatchTimeoutMillis;
    }

    @Override
    public ResultTable verify(VerifiableTable actualData, VerifiableTable expectedData) {
        if (actualData == null) {
            return new ResultTable(
                    new boolean[expectedData.getColumnCount()],
                    toListOfRows(
                            expectedData,
                            (columnName, value) -> ResultCell.createMissingCell(
                                    columnComparators.getComparator(columnName).getFormatter(), value)));
        }
        if (expectedData == null) {
            return new ResultTable(
                    new boolean[actualData.getColumnCount()],
                    toListOfRows(
                            actualData,
                            (columnName, value) -> ResultCell.createSurplusCell(
                                    columnComparators.getComparator(columnName).getFormatter(), value)));
        }

        LOGGER.info(
                "Verifying {} col {} row actual and {} col {} row expected tables",
                actualData.getColumnCount(),
                actualData.getRowCount(),
                expectedData.getColumnCount(),
                expectedData.getRowCount());

        LOGGER.debug("Generating column indices");
        List<IndexMap> columnIndices =
                getColumnIndices(actualData, expectedData, columnComparators.getDefaultComparator());
        identifyOutOfOrderIndices(columnIndices, 0);

        boolean[] keyColumns = new boolean[columnIndices.size()];
        for (int i = 0; i < keyColumns.length; i++) {
            keyColumns[i] = actualData instanceof KeyedVerifiableTable
                    && ((KeyedVerifiableTable) actualData)
                            .isKeyColumn(columnIndices.get(i).getActualIndex());
        }

        List<List<ResultCell>> results = new ArrayList<>(actualData.getRowCount() + 1);
        verifyHeaders(columnIndices, results, actualData, expectedData, columnComparators.getDefaultComparator());

        LOGGER.debug("Starting Happy Path");
        collectMatchingRows(columnIndices, results, actualData, expectedData, columnComparators);
        int happyPathSize = results.size() - 1; // minus headers
        if (happyPathSize == actualData.getRowCount() && happyPathSize == expectedData.getRowCount()) {
            LOGGER.debug("(Happily) Done!");
            return new ResultTable(keyColumns, results);
        }
        LOGGER.debug("Matched {} rows", happyPathSize);
        int firstUnMatchedIndex = happyPathSize;

        LOGGER.debug("Starting Reverse Happy Path (tm)");
        List<List<ResultCell>> reversePathResults = new ArrayList<>(actualData.getRowCount() - happyPathSize);
        collectReverseMatchingRows(
                columnIndices, reversePathResults, actualData, expectedData, columnComparators, firstUnMatchedIndex);
        int lastUnMatchedOffset = reversePathResults.size();
        LOGGER.debug("Matched {} rows", lastUnMatchedOffset);

        LOGGER.debug("Generating row indices from index {}.", firstUnMatchedIndex);
        ActualRowIterator actualRowIterator = new ActualRowIterator(
                actualData, columnIndices, columnComparators, firstUnMatchedIndex, lastUnMatchedOffset);
        ExpectedRowIterator expectedRowIterator = new ExpectedRowIterator(
                expectedData, columnIndices, columnComparators, firstUnMatchedIndex, lastUnMatchedOffset);
        IndexMapGenerator<RowView> rowGenerator =
                new IndexMapGenerator<>(expectedRowIterator, actualRowIterator, firstUnMatchedIndex);
        rowGenerator.generate();
        List<IndexMap> allMatchedRows = rowGenerator.getMatched();
        LOGGER.debug("Matched a further {} rows using row hashing", allMatchedRows.size());
        List<UnmatchedIndexMap> allMissingRows = rowGenerator.getMissing();
        List<UnmatchedIndexMap> allSurplusRows = rowGenerator.getSurplus();

        List<IndexMap> matchedColumns =
                columnIndices.stream().filter(IndexMap::isMatched).collect(Collectors.toList());
        LOGGER.debug("Partial-matching {} missing and {} surplus rows", allMissingRows.size(), allSurplusRows.size());
        PartialMatcher partialMatcher =
                new AdaptivePartialMatcher(actualData, expectedData, columnComparators, this.bestMatchThreshold);
        if (actualData instanceof KeyedVerifiableTable) {
            partialMatcher = new KeyColumnPartialMatcher(
                    (KeyedVerifiableTable) actualData, expectedData, columnComparators, partialMatcher);
        }
        if (this.partialMatchTimeoutMillis > 0) {
            partialMatcher = new TimeBoundPartialMatcher(partialMatcher, this.partialMatchTimeoutMillis);
        }
        partialMatcher.match(allMissingRows, allSurplusRows, matchedColumns);

        LOGGER.debug("Merging partial-matches and remaining missing/surplus");
        List<IndexMap> finalRowIndices = allMatchedRows;
        mergePartialMatches(finalRowIndices, allMissingRows, allSurplusRows);

        // todo: fix transitive bug in compareTo() and use finalRowIndices.sortThis()
        finalRowIndices = new ArrayList<>(new TreeSet<>(finalRowIndices));
        if (this.verifyRowOrder) {
            LOGGER.debug("Looking for out of order rows");
            identifyOutOfOrderIndices(finalRowIndices, firstUnMatchedIndex);
        }

        LOGGER.debug("Generating final results");
        buildResults(
                columnIndices,
                finalRowIndices,
                results,
                reversePathResults,
                actualData,
                expectedData,
                columnComparators);
        LOGGER.debug("Done");

        return new ResultTable(keyColumns, results);
    }

    private List<List<ResultCell>> toListOfRows(
            VerifiableTable verifiableTable, BiFunction<String, Object, ResultCell> cellFunction) {
        List<List<ResultCell>> results = new ArrayList<>(verifiableTable.getRowCount() + 1);
        List<ResultCell> headers = new ArrayList<>(verifiableTable.getColumnCount());
        for (int ci = 0; ci < verifiableTable.getColumnCount(); ci++) {
            String columnName = verifiableTable.getColumnName(ci);
            headers.add(cellFunction.apply(columnName, columnName));
        }
        results.add(headers);
        for (int ri = 0; ri < verifiableTable.getRowCount(); ri++) {
            List<ResultCell> row = new ArrayList<>(verifiableTable.getColumnCount());
            for (int ci = 0; ci < verifiableTable.getColumnCount(); ci++) {
                String columnName = verifiableTable.getColumnName(ci);
                row.add(cellFunction.apply(columnName, verifiableTable.getValueAt(ri, ci)));
            }
            results.add(row);
        }
        return results;
    }

    private static void verifyHeaders(
            List<IndexMap> columnIndices,
            List<List<ResultCell>> results,
            VerifiableTable actualData,
            VerifiableTable expectedData,
            CellComparator comparator) {
        List<ResultCell> verifiedHeaders = new ArrayList<>(columnIndices.size());
        for (IndexMap column : columnIndices) {
            if (column.isMissing()) {
                Object expected = expectedData.getColumnName(column.getExpectedIndex());
                verifiedHeaders.add(ResultCell.createMissingCell(comparator.getFormatter(), expected));
            } else if (column.isSurplus()) {
                Object actual = actualData.getColumnName(column.getActualIndex());
                verifiedHeaders.add(ResultCell.createSurplusCell(comparator.getFormatter(), actual));
            } else {
                Object actual = actualData.getColumnName(column.getActualIndex());
                Object expected = expectedData.getColumnName(column.getExpectedIndex());
                ResultCell cell = ResultCell.createMatchedCell(comparator, actual, expected);
                if (column.isOutOfOrder()) {
                    cell = ResultCell.createOutOfOrderCell(comparator.getFormatter(), actual);
                }
                verifiedHeaders.add(cell);
            }
        }
        results.add(verifiedHeaders);
    }

    private static void collectMatchingRows(
            List<IndexMap> columnIndices,
            List<List<ResultCell>> results,
            VerifiableTable actualData,
            VerifiableTable expectedData,
            ColumnComparators columnComparators) {
        int minRowCount = Math.min(actualData.getRowCount(), expectedData.getRowCount());
        for (int rowIndex = 0; rowIndex < minRowCount; rowIndex++) {
            List<ResultCell> row = new ArrayList<>(columnIndices.size());
            if (!checkRowMatches(
                    columnIndices, results, actualData, expectedData, columnComparators, rowIndex, rowIndex, row)) {
                return;
            }
        }
    }

    private static void collectReverseMatchingRows(
            List<IndexMap> columnIndices,
            List<List<ResultCell>> reverseHappyPathResults,
            VerifiableTable actualData,
            VerifiableTable expectedData,
            ColumnComparators columnComparators,
            int firstUnMatchedIndex) {
        int actualIndex = actualData.getRowCount() - 1;
        int expectedIndex = expectedData.getRowCount() - 1;
        int minActualIndex = firstUnMatchedIndex + 1;
        int minExpectedIndex = firstUnMatchedIndex + 1;
        while (expectedIndex >= minExpectedIndex && actualIndex >= minActualIndex) {
            List<ResultCell> row = new ArrayList<>(columnIndices.size());

            if (!checkRowMatches(
                    columnIndices,
                    reverseHappyPathResults,
                    actualData,
                    expectedData,
                    columnComparators,
                    actualIndex,
                    expectedIndex,
                    row)) {
                return;
            }
            expectedIndex--;
            actualIndex--;
        }
    }

    private static boolean checkRowMatches(
            List<IndexMap> columnIndices,
            List<List<ResultCell>> results,
            VerifiableTable actualData,
            VerifiableTable expectedData,
            ColumnComparators columnComparators,
            int actualIndex,
            int expectedIndex,
            List<ResultCell> row) {
        for (IndexMap column : columnIndices) {
            if (column.isMissing()) {
                CellComparator comparator =
                        columnComparators.getComparator(expectedData.getColumnName(column.getExpectedIndex()));
                Object expected = expectedData.getValueAt(expectedIndex, column.getExpectedIndex());
                row.add(ResultCell.createMissingCell(comparator.getFormatter(), expected));
            } else if (column.isSurplus()) {
                CellComparator comparator =
                        columnComparators.getComparator(actualData.getColumnName(column.getActualIndex()));
                Object actual = actualData.getValueAt(actualIndex, column.getActualIndex());
                row.add(ResultCell.createSurplusCell(comparator.getFormatter(), actual));
            } else {
                CellComparator comparator =
                        columnComparators.getComparator(expectedData.getColumnName(column.getExpectedIndex()));
                Object actual = actualData.getValueAt(actualIndex, column.getActualIndex());
                Object expected = expectedData.getValueAt(expectedIndex, column.getExpectedIndex());
                ResultCell cell = ResultCell.createMatchedCell(comparator, actual, expected);
                if (cell.isMatch()) {
                    if (column.isOutOfOrder()) {
                        cell = ResultCell.createOutOfOrderCell(comparator.getFormatter(), actual);
                    }
                    row.add(cell);
                } else {
                    return false;
                }
            }
        }
        results.add(row);
        return true;
    }

    private static void identifyOutOfOrderIndices(List<IndexMap> indexMaps, int nextExpectedIndex) {
        Set<Integer> expectedIndices = new HashSet<>(indexMaps.size());
        for (IndexMap indexMap : indexMaps) {
            if (!indexMap.isSurplus()) {
                expectedIndices.add(indexMap.getExpectedIndex());
            }
        }
        for (IndexMap im : indexMaps) {
            if (!im.isSurplus()) {
                expectedIndices.remove(im.getExpectedIndex());
                if (im.getExpectedIndex() == nextExpectedIndex) {
                    while (!expectedIndices.contains(nextExpectedIndex) && !expectedIndices.isEmpty()) {
                        nextExpectedIndex++;
                    }
                } else {
                    im.setOutOfOrder();
                }
            }
        }
    }

    private void buildResults(
            List<IndexMap> columnIndices,
            List<IndexMap> finalRowIndices,
            List<List<ResultCell>> results,
            List<List<ResultCell>> reverseResults,
            VerifiableTable actualData,
            VerifiableTable expectedData,
            ColumnComparators columnComparators) {
        for (IndexMap rowIndexMap : finalRowIndices) {
            List<ResultCell> row = new ArrayList<>();
            if (rowIndexMap.isMissing()) {
                if (!this.ignoreMissingRows) {
                    for (IndexMap colIndexMap : columnIndices) {
                        addMissingRecord(rowIndexMap, row, colIndexMap, expectedData, columnComparators);
                    }
                }
            } else if (rowIndexMap.isSurplus()) {
                if (!this.ignoreSurplusRows) {
                    for (IndexMap colIndexMap : columnIndices) {
                        addSurplusRecord(rowIndexMap, row, colIndexMap, actualData, columnComparators);
                    }
                }
            } else {
                for (IndexMap colIndexMap : columnIndices) {
                    addMatchedRecord(rowIndexMap, row, colIndexMap, actualData, expectedData, columnComparators);
                }
            }

            if (!row.isEmpty()) {
                results.add(row);
            }
        }
        for (int i = reverseResults.size() - 1; i >= 0; i--) {
            results.add(reverseResults.get(i));
        }
    }

    private static void addSurplusRecord(
            IndexMap rowIndexMap,
            List<ResultCell> row,
            IndexMap colIndexMap,
            VerifiableTable actualData,
            ColumnComparators columnComparators) {
        if (rowIndexMap.getActualIndex() >= 0 && colIndexMap.getActualIndex() >= 0) {
            CellComparator comparator =
                    columnComparators.getComparator(actualData.getColumnName(colIndexMap.getActualIndex()));
            Object displayValue = actualData.getValueAt(rowIndexMap.getActualIndex(), colIndexMap.getActualIndex());
            row.add(ResultCell.createSurplusCell(comparator.getFormatter(), displayValue));
        } else {
            row.add(ResultCell.createSurplusCell(
                    columnComparators.getDefaultComparator().getFormatter(), ""));
        }
    }

    private static void addMissingRecord(
            IndexMap rowIndexMap,
            List<ResultCell> row,
            IndexMap colIndexMap,
            VerifiableTable expectedData,
            ColumnComparators columnComparators) {
        if (rowIndexMap.getExpectedIndex() >= 0 && colIndexMap.getExpectedIndex() >= 0) {
            CellComparator comparator =
                    columnComparators.getComparator(expectedData.getColumnName(colIndexMap.getExpectedIndex()));
            Object displayValue =
                    expectedData.getValueAt(rowIndexMap.getExpectedIndex(), colIndexMap.getExpectedIndex());
            row.add(ResultCell.createMissingCell(comparator.getFormatter(), displayValue));
        } else {
            row.add(ResultCell.createMissingCell(
                    columnComparators.getDefaultComparator().getFormatter(), ""));
        }
    }

    private static void addMatchedRecord(
            IndexMap rowIndexMap,
            List<ResultCell> row,
            IndexMap colIndexMap,
            VerifiableTable actualData,
            VerifiableTable expectedData,
            ColumnComparators columnComparators) {
        if (colIndexMap.isMissing()) {
            addMissingRecord(rowIndexMap, row, colIndexMap, expectedData, columnComparators);
        } else if (colIndexMap.isSurplus()) {
            addSurplusRecord(rowIndexMap, row, colIndexMap, actualData, columnComparators);
        } else {
            CellComparator comparator =
                    columnComparators.getComparator(expectedData.getColumnName(colIndexMap.getExpectedIndex()));
            Object actual = actualData.getValueAt(rowIndexMap.getActualIndex(), colIndexMap.getActualIndex());
            Object expected = expectedData.getValueAt(rowIndexMap.getExpectedIndex(), colIndexMap.getExpectedIndex());
            ResultCell comparisonResult = ResultCell.createMatchedCell(comparator, actual, expected);
            boolean outOfOrder = rowIndexMap.isOutOfOrder() || colIndexMap.isOutOfOrder();
            // todo: modify comparator to handle out-of-order state internally
            if (outOfOrder && comparisonResult.isMatch()) {
                comparisonResult = ResultCell.createOutOfOrderCell(comparator.getFormatter(), actual);
            }
            row.add(comparisonResult);
        }
    }

    private static void mergePartialMatches(
            List<IndexMap> finalRowIndices,
            List<UnmatchedIndexMap> allMissingRows,
            List<UnmatchedIndexMap> allSurplusRows) {
        Set<IndexMap> partiallyMatchedSurplus = new HashSet<>();
        for (UnmatchedIndexMap expected : allMissingRows) {
            UnmatchedIndexMap actual = expected.getBestMutualMatch();
            if (actual == null) {
                finalRowIndices.add(expected);
            } else {
                // todo: can we avoi newing up another index map - update expected in place?
                finalRowIndices.add(new IndexMap(expected.getExpectedIndex(), actual.getActualIndex()));
                partiallyMatchedSurplus.add(actual);
            }
        }

        for (IndexMap indexMap : allSurplusRows) {
            if (!partiallyMatchedSurplus.contains(indexMap)) {
                finalRowIndices.add(indexMap);
            }
        }
    }

    private List<IndexMap> getColumnIndices(
            VerifiableTable actualData, VerifiableTable expectedData, CellComparator comparator) {
        List<String> expectedHeadings = getHeadings(expectedData, comparator);
        List<String> actualHeadings = getHeadings(actualData, comparator);
        IndexMapGenerator<String> columnGenerator =
                new IndexMapGenerator<>(expectedHeadings.iterator(), actualHeadings.iterator(), 0);
        columnGenerator.generate();
        List<IndexMap> all = columnGenerator.getAll();
        return all.stream()
                .filter((indexMap) -> !indexMap.isMissing() || !ignoreMissingColumns)
                .filter((indexMap) -> !indexMap.isSurplus() || !ignoreSurplusColumns)
                .collect(Collectors.toList());
    }

    private static List<String> getHeadings(VerifiableTable table, CellComparator comparator) {
        List<String> headings = new ArrayList<>();
        for (int i = 0; i < table.getColumnCount(); i++) {
            headings.add(comparator.getFormatter().format(table.getColumnName(i)));
        }
        return headings;
    }
}
