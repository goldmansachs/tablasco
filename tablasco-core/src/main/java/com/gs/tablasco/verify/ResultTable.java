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

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ResultTable implements FormattableTable {
    private final List<List<ResultCell>> tableCells;
    private final int[] matchedColumnsAhead;
    private final int totalCellCount;
    private final int passedCellCount;

    public ResultTable(boolean[] keyColumns, List<List<ResultCell>> tableCells) {
        this.tableCells = tableCells;
        List<ResultCell> headers = tableCells.get(0);
        boolean[] matchedColumns = new boolean[headers.size()];
        Arrays.fill(matchedColumns, true);
        int total = 0;
        int passed = 0;
        Predicate<ResultCell> isMatched = ResultCell.IS_PASSED_CELL;
        Predicate<ResultCell> dataIsMatched = ResultCell.IS_FAILED_CELL.negate();
        for (List<ResultCell> row : tableCells) {
            for (int col = 0; col < row.size(); col++) {
                ResultCell cell = row.get(col);
                total++;
                boolean isPassed = ResultCell.IS_PASSED_CELL.test(cell);
                passed += isPassed ? 1 : 0;
                matchedColumns[col] &= !keyColumns[col] && isMatched.test(cell);
            }
            isMatched = dataIsMatched;
        }
        this.matchedColumnsAhead = new int[matchedColumns.length];
        for (int i = 0; i < this.matchedColumnsAhead.length; i++) {
            this.matchedColumnsAhead[i] = getMatchedColumnsAhead(i, matchedColumns);
        }
        this.totalCellCount = total;
        this.passedCellCount = passed;
    }

    private static int getMatchedColumnsAhead(int col, boolean[] matchedColumns) {
        int matchedColumnsAhead = 0;
        if (matchedColumns[col]) {
            for (int i = col + 1; i < matchedColumns.length; i++) {
                if (matchedColumns[i]) {
                    matchedColumnsAhead++;
                } else {
                    return matchedColumnsAhead;
                }
            }
        }
        return matchedColumnsAhead;
    }

    public boolean isSuccess() {
        return this.totalCellCount == this.passedCellCount;
    }

    @Override
    public int getTotalCellCount() {
        return this.totalCellCount;
    }

    @Override
    public int getPassedCellCount() {
        return this.passedCellCount;
    }

    public List<List<ResultCell>> getVerifiedRows() {
        return this.tableCells;
    }

    @Override
    public List<ResultCell> getHeaders() {
        return this.getVerifiedRows().get(0);
    }

    @Override
    public int getMatchedColumnsAhead(int column) {
        return this.matchedColumnsAhead[column];
    }

    @Override
    public void appendTo(String testName, String tableName, Element table, HtmlOptions htmlOptions) {
        List<List<ResultCell>> results = this.getVerifiedRows();
        HtmlFormatter.appendHeaderRow(table, this, htmlOptions);

        int matchedRows = 0;
        int dataRowIndex = 1;
        int rowsAppended = 0;
        while (dataRowIndex < results.size() && rowsAppended < htmlOptions.getHtmlRowLimit()) {
            List<ResultCell> row = results.get(dataRowIndex);
            if (htmlOptions.isHideMatchedRowsFor(tableName)) {
                int failedCount =
                        (int) row.stream().filter(ResultCell.IS_FAILED_CELL).count();
                int passedCount =
                        (int) row.stream().filter(ResultCell.IS_PASSED_CELL).count();
                if (failedCount == 0 && passedCount > 0) {
                    matchedRows++;
                } else {
                    if (matchedRows > 0) {
                        HtmlFormatter.appendMultiMatchedRow(
                                table, this.getHeaders().size(), matchedRows);
                        rowsAppended++;
                    }
                    matchedRows = 0;
                    if (rowsAppended >= htmlOptions.getHtmlRowLimit()) {
                        break;
                    }
                    HtmlFormatter.appendDataRow(table, this, null, null, row, htmlOptions);
                    rowsAppended++;
                }
            } else {
                HtmlFormatter.appendDataRow(table, this, null, null, row, htmlOptions);
                rowsAppended++;
            }
            dataRowIndex++;
        }
        if (htmlOptions.isHideMatchedRowsFor(tableName) && matchedRows > 0) {
            HtmlFormatter.appendMultiMatchedRow(table, this.getHeaders().size(), matchedRows);
        }
        int remainingRows = results.size() - dataRowIndex;
        if (remainingRows > 0) {
            Document document = table.getOwnerDocument();
            Element tr = document.createElement("tr");
            table.appendChild(tr);
            Element td = document.createElement("td");
            td.setAttribute("class", this.isSuccess() ? "pass multi" : "fail multi");
            td.setAttribute("colspan", String.valueOf(this.getHeaders().size()));
            td.appendChild(
                    document.createTextNode(remainingRows + (remainingRows > 1 ? " more rows..." : " more row...")));
            tr.appendChild(td);
        }
    }
}
