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

import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.block.procedure.primitive.ObjectIntProcedure;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.utility.Iterate;
import org.eclipse.collections.impl.utility.ListIterate;
import org.w3c.dom.Element;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SummaryResultTable implements FormattableTable, Serializable
{
    private final Map<String, SummaryResult> resultsByKey = new TreeMap<>();
    private int passedCellCount;
    private int totalCellCount;
    private List<ResultCell> headers;

    public SummaryResultTable() {}

    public SummaryResultTable(ResultTable resultTable)
    {
        this.headers = resultTable.getHeaders();
        this.passedCellCount += resultTable.getPassedCellCount();
        this.totalCellCount += resultTable.getTotalCellCount();
        List<List<ResultCell>> verifiedRows = resultTable.getVerifiedRows();
        for (int i = 1; i < verifiedRows.size(); i++)
        {
            List<ResultCell> verifiedRow = verifiedRows.get(i);
            String key = getKey(verifiedRow);
            SummaryResult summaryResult = this.resultsByKey.get(key);
            if (summaryResult == null)
            {
                summaryResult = new SummaryResult(key, this.headers.size());
                this.resultsByKey.put(key, summaryResult);
            }
            summaryResult.addRow(verifiedRow);
            summaryResult.addCardinality(verifiedRow);
            summaryResult.totalRows++;
            if ("1".equals(key))
            {
                summaryResult.addSurplusCells(this.headers);
            }
            if ("2".equals(key))
            {
                summaryResult.addMissingCells(this.headers);
            }
        }
    }

    public SummaryResultTable merge(SummaryResultTable resultTable)
    {
        List<ResultCell> nextHeaders = resultTable.getHeaders();
        if (this.headers == null || nextHeaders.size() > this.headers.size())
        {
            this.headers = nextHeaders;
        }
        this.passedCellCount += resultTable.getPassedCellCount();
        this.totalCellCount += resultTable.getTotalCellCount();

        for (Map.Entry<String, SummaryResult> entry : resultTable.getResultsByKey().entrySet())
        {
            SummaryResult summaryResult = entry.getValue();
            SummaryResult mergedResult = this.resultsByKey.get(entry.getKey());
            if (mergedResult == null)
            {
                mergedResult = new SummaryResult(summaryResult);
                this.resultsByKey.put(entry.getKey(), mergedResult);
            }
            else
            {
                for (List<ResultCell> resultCells : summaryResult.firstFew)
                {
                    mergedResult.addRow(resultCells);
                }
                mergedResult.mergeCardinalities(summaryResult.columnCardinalityList);
                mergedResult.totalRows += summaryResult.totalRows;
            }
            if ("1".equals(entry.getKey()))
            {
                mergedResult.addSurplusCells(this.headers);
            }
            if ("2".equals(entry.getKey()))
            {
                mergedResult.addMissingCells(this.headers);
            }
        }
        return this;
    }

    @Override
    public boolean isSuccess()
    {
        return this.passedCellCount == this.totalCellCount;
    }

    @Override
    public int getPassedCellCount()
    {
        return this.passedCellCount;
    }

    @Override
    public int getTotalCellCount()
    {
        return this.totalCellCount;
    }

    Map<String, SummaryResult> getResultsByKey()
    {
        return this.resultsByKey;
    }

    /*
     * Returns a string key for a row used to group rows by break type and sort according to requirements
     *  - All cells are pass:    returns "0"
     *  - All cells are missing: returns "1"
     *  - All cells are surplus: returns "2"
     *  - All cells are fail:    returns "300110" (where 00110 corresponds to pass/fail cells in the row)
     */
    private String getKey(List<ResultCell> verifiedRow)
    {
        int passCount = 0;
        int surpCount = 0;
        int failCount = 0;
        StringBuilder failedKey = new StringBuilder().append('3');
        for (ResultCell resultCell : verifiedRow)
        {
            switch (resultCell.getCssClass())
            {
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
        if (failCount > 0)
        {
            return failedKey.toString();
        }
        if (passCount > 0)
        {
            return "0";
        }
        return surpCount > 0 ? "2" : "1";
    }

    @Override
    public List<ResultCell> getHeaders()
    {
        return this.headers;
    }

    @Override
    public int getMatchedColumnsAhead(int col)
    {
        return 0;
    }

    @Override
    public void appendTo(final String testName, final String tableName, final Element table, final HtmlOptions htmlOptions)
    {
        HtmlFormatter.appendHeaderRow(table, this, htmlOptions);
        Iterate.forEachWithIndex(this.getResultsByKey().keySet(), new ObjectIntProcedure<String>()
        {
            @Override
            public void value(String key, int index)
            {
                SummaryResult summaryResult = getResultsByKey().get(key);
                HtmlFormatter.appendSpanningRow(table, SummaryResultTable.this, "blank_row", null, null);

                for (List<ResultCell> resultCells : summaryResult.getFirstFewRows())
                {
                    HtmlFormatter.appendDataRow(table, SummaryResultTable.this, null, null, resultCells, htmlOptions);
                }
                int remainingRows = summaryResult.getRemainingRowCount();
                if (remainingRows > 0)
                {
                    String summaryRowId = HtmlFormatter.toHtmlId(testName, tableName) + ".summaryRow" + index;
                    String summaryText;
                    if ("0".equals(key))
                    {
                        summaryText = ResultCell.adaptOnCount(remainingRows, " more matched row");
                    }
                    else
                    {
                        summaryText = ResultCell.adaptOnCount(remainingRows, " more break") + " like this";
                    }
                    HtmlFormatter.appendSpanningRow(table, SummaryResultTable.this, "summary", NumberFormat.getInstance().format(remainingRows) + summaryText + "...", "toggleVisibility('" + summaryRowId + "')");
                    HtmlFormatter.appendDataRow(table, SummaryResultTable.this, summaryRowId, "display:none", summaryResult.getSummaryCardinalityRow(), htmlOptions);
                }
            }
        });
    }

    private static class SummaryResult implements Serializable
    {
        private static final int MAX_NUMBER_OF_FIRST_FEW_ROWS = 3;
        private static final int MAXIMUM_CARDINALITY_TO_COUNT = 20;
        private final List<List<ResultCell>> firstFew = FastList.newList();
        private int totalRows;
        private final String key;
        private final MutableList<ColumnCardinality> columnCardinalityList;

        private SummaryResult(String key, int numberOfColumns)
        {
            this.key = key;
            this.columnCardinalityList = Lists.mutable.withNValues(numberOfColumns, new Function0<ColumnCardinality>()
            {
                @Override
                public ColumnCardinality value()
                {
                    return createColumnCardinality();
                }
            });
        }

        private SummaryResult(SummaryResult summaryResult)
        {
            this.firstFew.addAll(summaryResult.firstFew);
            this.totalRows = summaryResult.totalRows;
            this.key = summaryResult.key;
            this.columnCardinalityList = summaryResult.columnCardinalityList;
        }

        void addRow(List<ResultCell> verifiedRow)
        {
            if (this.firstFew.size() < MAX_NUMBER_OF_FIRST_FEW_ROWS)
            {
                this.firstFew.add(verifiedRow);
            }
        }

        void addCardinality(List<ResultCell> verifiedRow)
        {
            ListIterate.forEachWithIndex(verifiedRow, new ObjectIntProcedure<ResultCell>()
            {
                @Override
                public void value(ResultCell resultCell, int index)
                {
                    SummaryResult.this.columnCardinalityList.get(index).add(resultCell.getSummary());
                }
            });
        }

        void mergeCardinalities(final List<ColumnCardinality> columnCardinalities)
        {
            ListIterate.forEachWithIndex(columnCardinalities, new ObjectIntProcedure<ColumnCardinality>()
            {
                @Override
                public void value(ColumnCardinality columnCardinality, final int index)
                {
                    SummaryResult.this.columnCardinalityList.get(index).merge(columnCardinality);
                }
            });
        }

        private List<List<ResultCell>> getFirstFewRows()
        {
            return this.firstFew;
        }

        private int getRemainingRowCount()
        {
            return this.totalRows - this.firstFew.size();
        }

        private MutableList<ColumnCardinality> getRemainingCardinalities()
        {
            final MutableList<ColumnCardinality> remainingCardinalities = this.columnCardinalityList.clone();
            ListIterate.forEach(this.firstFew, new Procedure<List<ResultCell>>()
            {
                @Override
                public void value(List<ResultCell> row)
                {
                    ListIterate.forEachWithIndex(row, new ObjectIntProcedure<ResultCell>()
                    {
                        @Override
                        public void value(ResultCell cell, int index)
                        {
                            remainingCardinalities.get(index).remove(cell.getSummary());
                        }
                    });
                }
            });
            return remainingCardinalities;
        }

        private List<ResultCell> getSummaryCardinalityRow()
        {
            return ListIterate.collect(getRemainingCardinalities(), new Function<ColumnCardinality, ResultCell>()
            {
                @Override
                public ResultCell valueOf(final ColumnCardinality columnCardinality)
                {
                    return ResultCell.createSummaryCell(MAXIMUM_CARDINALITY_TO_COUNT, columnCardinality);
                }
            });
        }

        @Override
        public String toString()
        {
            return Maps.fixedSize.of("firstFew", this.firstFew.size(), "totalRows", this.totalRows).toString();
        }

        private void addSurplusCells(List<ResultCell> headers)
        {
            for (int i = 0; i < headers.size(); i++)
            {
                if ("surplus".equals(headers.get(i).getCssClass()))
                {
                    for (List<ResultCell> resultCells : this.firstFew)
                    {
                        if (resultCells.size() != headers.size())
                        {
                            CellFormatter formatter = resultCells.get(0).formatter;
                            resultCells.add(i, ResultCell.createSurplusCell(formatter, null));
                        }
                    }
                    if (this.columnCardinalityList.size() != headers.size())
                    {
                        this.columnCardinalityList.add(i, createColumnCardinality());
                    }
                }
            }
        }

        private void addMissingCells(List<ResultCell> headers)
        {
            for (int i = 0; i < headers.size(); i++)
            {
                if ("missing".equals(headers.get(i).getCssClass()))
                {
                    for (List<ResultCell> resultCells : this.firstFew)
                    {
                        if (resultCells.size() != headers.size())
                        {
                            CellFormatter formatter = resultCells.get(0).formatter;
                            resultCells.add(i, ResultCell.createMissingCell(formatter, null));
                        }
                    }
                    if (this.columnCardinalityList.size() != headers.size())
                    {
                        this.columnCardinalityList.add(i, createColumnCardinality());
                    }
                }
            }
        }

        private ColumnCardinality createColumnCardinality()
        {
            return new ColumnCardinality(MAX_NUMBER_OF_FIRST_FEW_ROWS + MAXIMUM_CARDINALITY_TO_COUNT);
        }
    }
}
