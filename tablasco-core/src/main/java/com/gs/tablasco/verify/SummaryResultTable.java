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

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.w3c.dom.Element;

public class SummaryResultTable implements FormattableTable, Serializable {
    private final Map<String, SummaryResult> resultsByKey = new TreeMap<>();
    private int passedCellCount;
    private int totalCellCount;
    private List<ResultCell> headers;

    public SummaryResultTable(ResultTable resultTable) {
        this.headers = resultTable.getHeaders();
        this.passedCellCount += resultTable.getPassedCellCount();
        this.totalCellCount += resultTable.getTotalCellCount();
        List<List<ResultCell>> verifiedRows = resultTable.getVerifiedRows();
        for (int i = 1; i < verifiedRows.size(); i++) {
            List<ResultCell> verifiedRow = verifiedRows.get(i);
            String key = getKey(verifiedRow);
            SummaryResult summaryResult = this.resultsByKey.get(key);
            if (summaryResult == null) {
                summaryResult = new SummaryResult(key, this.headers.size());
                this.resultsByKey.put(key, summaryResult);
            }
            summaryResult.addRow(verifiedRow);
            summaryResult.addCardinality(verifiedRow);
            summaryResult.totalRows++;
        }
    }

    public SummaryResultTable merge(SummaryResultTable resultTable) {
        List<ResultCell> nextHeaders = resultTable.getHeaders();
        if (this.headers == null || nextHeaders.size() > this.headers.size()) {
            this.headers = nextHeaders;
        }
        this.passedCellCount += resultTable.getPassedCellCount();
        this.totalCellCount += resultTable.getTotalCellCount();

        for (Map.Entry<String, SummaryResult> entry :
                resultTable.getResultsByKey().entrySet()) {
            SummaryResult summaryResult = entry.getValue();
            SummaryResult mergedResult = this.resultsByKey.get(entry.getKey());
            if (mergedResult == null) {
                mergedResult = new SummaryResult(summaryResult);
                this.resultsByKey.put(entry.getKey(), mergedResult);
            } else {
                for (List<ResultCell> resultCells : summaryResult.firstFew) {
                    mergedResult.addRow(resultCells);
                }
                mergedResult.mergeCardinalities(summaryResult.columnCardinalityList);
                mergedResult.totalRows += summaryResult.totalRows;
            }
        }
        return this;
    }

    @Override
    public boolean isSuccess() {
        return this.passedCellCount == this.totalCellCount;
    }

    @Override
    public int getPassedCellCount() {
        return this.passedCellCount;
    }

    @Override
    public int getTotalCellCount() {
        return this.totalCellCount;
    }

    Map<String, SummaryResult> getResultsByKey() {
        return this.resultsByKey;
    }

    /*
     * Returns a string key for a row used to group rows by break type and sort according to requirements
     *  - All cells are pass:    returns "0"
     *  - All cells are missing: returns "1"
     *  - All cells are surplus: returns "2"
     *  - All cells are fail:    returns "300110" (where 00110 corresponds to pass/fail cells in the row)
     */
    private String getKey(List<ResultCell> verifiedRow) {
        int passCount = 0;
        int surpCount = 0;
        int failCount = 0;
        StringBuilder failedKey = new StringBuilder().append('3');
        for (ResultCell resultCell : verifiedRow) {
            switch (resultCell.getCssClass()) {
                case "pass":
                    passCount++;
                    failedKey.append('0');
                    break;
                case "fail":
                    failCount++;
                    failedKey.append('1');
                    break;
                case "surplus":
                    surpCount++;
                    break;
            }
        }
        if (failCount > 0) {
            return failedKey.toString();
        }
        if (passCount > 0) {
            return "0";
        }
        return surpCount > 0 ? "2" : "1";
    }

    @Override
    public List<ResultCell> getHeaders() {
        return this.headers;
    }

    @Override
    public int getMatchedColumnsAhead(int col) {
        return 0;
    }

    @Override
    public void appendTo(
            final String testName, final String tableName, final Element table, final HtmlOptions htmlOptions) {
        HtmlFormatter.appendHeaderRow(table, this, htmlOptions);
        int index = 0;
        for (String key : this.getResultsByKey().keySet()) {
            SummaryResult summaryResult = getResultsByKey().get(key);
            HtmlFormatter.appendSpanningRow(table, SummaryResultTable.this, "blank_row", null, null);

            for (List<ResultCell> resultCells : summaryResult.getFirstFewRows()) {
                HtmlFormatter.appendDataRow(table, SummaryResultTable.this, null, null, resultCells, htmlOptions);
            }
            int remainingRows = summaryResult.getRemainingRowCount();
            if (remainingRows > 0) {
                String summaryRowId = HtmlFormatter.toHtmlId(testName, tableName) + ".summaryRow" + index;
                String summaryText;
                if ("0".equals(key)) {
                    summaryText = ResultCell.adaptOnCount(remainingRows, " more matched row");
                } else {
                    summaryText = ResultCell.adaptOnCount(remainingRows, " more break") + " like this";
                }
                HtmlFormatter.appendSpanningRow(
                        table,
                        SummaryResultTable.this,
                        "summary",
                        NumberFormat.getInstance().format(remainingRows) + summaryText + "...",
                        "toggleVisibility('" + summaryRowId + "')");
                HtmlFormatter.appendDataRow(
                        table,
                        SummaryResultTable.this,
                        summaryRowId,
                        "display:none",
                        summaryResult.getSummaryCardinalityRow(),
                        htmlOptions);
            }
            index++;
        }
    }

    private static class SummaryResult implements Serializable {
        private static final int MAX_NUMBER_OF_FIRST_FEW_ROWS = 3;
        private static final int MAXIMUM_CARDINALITY_TO_COUNT = 20;
        private final List<List<ResultCell>> firstFew = new ArrayList<>();
        private int totalRows;
        private final String key;
        private final List<ColumnCardinality> columnCardinalityList;

        private SummaryResult(String key, int numberOfColumns) {
            this.key = key;
            this.columnCardinalityList = IntStream.rangeClosed(1, numberOfColumns)
                    .mapToObj(value -> createColumnCardinality())
                    .collect(Collectors.toList());
        }

        private SummaryResult(SummaryResult summaryResult) {
            this.firstFew.addAll(summaryResult.firstFew);
            this.totalRows = summaryResult.totalRows;
            this.key = summaryResult.key;
            this.columnCardinalityList = summaryResult.columnCardinalityList;
        }

        void addRow(List<ResultCell> verifiedRow) {
            if (this.firstFew.size() < MAX_NUMBER_OF_FIRST_FEW_ROWS) {
                this.firstFew.add(verifiedRow);
            }
        }

        void addCardinality(List<ResultCell> verifiedRow) {
            for (int rowIndex = 0; rowIndex < verifiedRow.size(); rowIndex++) {
                ResultCell resultCell = verifiedRow.get(rowIndex);
                SummaryResult.this.columnCardinalityList.get(rowIndex).addOccurrence(resultCell.getSummary());
            }
        }

        void mergeCardinalities(final List<ColumnCardinality> columnCardinalities) {
            for (int columnIndex = 0; columnIndex < columnCardinalities.size(); columnIndex++) {
                ColumnCardinality columnCardinality = columnCardinalities.get(columnIndex);
                SummaryResult.this.columnCardinalityList.get(columnIndex).merge(columnCardinality);
            }
        }

        private List<List<ResultCell>> getFirstFewRows() {
            return this.firstFew;
        }

        private int getRemainingRowCount() {
            return this.totalRows - this.firstFew.size();
        }

        private List<ColumnCardinality> getRemainingCardinalities() {
            final List<ColumnCardinality> remainingCardinalities = new ArrayList<>(this.columnCardinalityList);
            this.firstFew.forEach(row -> {
                for (int index = 0; index < row.size(); index++) {
                    ResultCell cell = row.get(index);
                    remainingCardinalities.get(index).removeOccurrence(cell.getSummary());
                }
            });
            return remainingCardinalities;
        }

        private List<ResultCell> getSummaryCardinalityRow() {
            return getRemainingCardinalities().stream()
                    .map(columnCardinality ->
                            ResultCell.createSummaryCell(MAXIMUM_CARDINALITY_TO_COUNT, columnCardinality))
                    .collect(Collectors.toList());
        }

        @Override
        public String toString() {
            Map<Object, Object> map = new LinkedHashMap<>();
            map.put("firstFew", this.firstFew.size());
            map.put("totalRows", this.totalRows);
            return map.toString();
        }

        private ColumnCardinality createColumnCardinality() {
            return new ColumnCardinality(MAX_NUMBER_OF_FIRST_FEW_ROWS + MAXIMUM_CARDINALITY_TO_COUNT);
        }
    }
}
