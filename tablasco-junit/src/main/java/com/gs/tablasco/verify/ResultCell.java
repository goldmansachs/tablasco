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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Bean which holds result information for each cell in the grid, including formatted strings for actual and expected values and a
 * CellState indicating whether the cell is matched, unmatched, missing, or surplus.
 */

public abstract class ResultCell implements Serializable
{
    public static final Predicate<ResultCell> IS_FAILED_CELL = FailedCell.class::isInstance;
    public static final Predicate<ResultCell> IS_PASSED_CELL = PassedCell.class::isInstance;

    final CellFormatter formatter;

    protected ResultCell(CellFormatter formatter)
    {
        this.formatter = formatter;
    }

    public abstract Object getExpected();

    public abstract Object getActual();

    public abstract String getCssClass();

    public abstract Object getSummary();

    public static ResultCell createMatchedCell(CellComparator cellComparator, Object actual, Object expected)
    {
        if (cellComparator.equals(actual, expected))
        {
            return new PassedCell(cellComparator.getFormatter(), actual);
        }
        return new FailedCell(cellComparator.getFormatter(), actual, expected);
    }

    public static ResultCell createMissingCell(CellFormatter formatter, Object expected)
    {
        return new MissingCell(formatter, expected);
    }

    public static ResultCell createSurplusCell(CellFormatter formatter, Object actual)
    {
        return new SurplusCell(formatter, actual);
    }

    static ResultCell createCustomCell(String contents, String cssClass)
    {
        return createCustomCell(null, contents, cssClass);
    }

    static ResultCell createCustomCell(String title, String contents, String cssClass)
    {
        return new CustomCell(title, contents, cssClass);
    }

    public static ResultCell createOutOfOrderCell(CellFormatter formatter, Object actualAndExpected)
    {
        return new OutOfOrderCell(formatter, actualAndExpected);
    }

    static ResultCell createSummaryCell(int maximumCardinalityToCount, ColumnCardinality columnCardinality)
    {
        return new SummaryCell(maximumCardinalityToCount, columnCardinality);
    }

    static Element createNodeWithText(Document document, String tagName, String content)
    {
        return createNodeWithText(document, tagName, content, null);
    }

    static Element createNodeWithText(Document document, String tagName, String content, String cssClass)
    {
        Element element = document.createElement(tagName);
        if (cssClass != null)
        {
            element.setAttribute("class", cssClass);
        }
        element.appendChild(document.createTextNode(content));
        return element;
    }

    private static Element createCell(Document document, String className, boolean headerRow, boolean isNumeric, Node... content)
    {
        Element td = document.createElement(headerRow ? "th" : "td");
        td.setAttribute("class", className + (isNumeric ? " number" : ""));
        for (Node node : content)
        {
            td.appendChild(node);
        }
        return td;
    }

    static String adaptOnCount(int count, String s)
    {
        return count > 1 ? s + 's' : s;
    }

    public boolean isMatch()
    {
        return false;
    }

    public abstract Node createCell(Document document, boolean isHeaderRow);

    @Override
    public final boolean equals(Object o)
    {
        return this.toString().equals(String.valueOf(o));
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException("equals() is only implemented for tests");
    }

    private static class PassedCell extends OutOfOrderCell
    {
        private PassedCell(CellFormatter formatter, Object actualAndExpected)
        {
            super(formatter, actualAndExpected);
        }

        @Override
        public boolean isMatch()
        {
            return true;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, CellFormatter.isNumber(this.actualAndExpected),
                    document.createTextNode(this.formatter.format(this.actualAndExpected)));
        }

        @Override
        public String getCssClass()
        {
            return "pass";
        }

        @Override
        public Object getSummary()
        {
            return this.formatter.format(this.actualAndExpected);
        }
    }

    private static class FailedCell extends ResultCell
    {
        private final Object actual;
        private final Object expected;

        private FailedCell(CellFormatter formatter, Object actual, Object expected)
        {
            super(formatter);
            this.actual = actual;
            this.expected = expected;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + "{actual=" + this.formatter.format(this.actual) + ", expected=" + this.formatter.format(this.expected) + '}';
        }

        @Override
        public Object getExpected()
        {
            return this.expected;
        }

        @Override
        public Object getActual()
        {
            return this.actual;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            boolean isActualAndExpectedNumber = CellFormatter.isNumber(this.actual) && CellFormatter.isNumber(this.expected);
            if (isActualAndExpectedNumber)
            {
                String difference = this.formatter.format(ToleranceCellComparator.getDifference(this.actual, this.expected));
                String variance = this.formatter.format(VarianceCellComparator.getVariance(this.actual, this.expected));
                return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, true,
                        document.createTextNode(this.formatter.format(this.expected)),
                        ResultCell.createNodeWithText(document, "p", "Expected"),
                        document.createElement("hr"),
                        document.createTextNode(this.formatter.format(this.actual)),
                        ResultCell.createNodeWithText(document, "p", "Actual"),
                        document.createElement("hr"),
                        document.createTextNode(difference + " / " + variance + '%'),
                        ResultCell.createNodeWithText(document, "p", "Difference / Variance"));
            }
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, false,
                    document.createTextNode(this.formatter.format(this.expected)),
                    ResultCell.createNodeWithText(document, "p", "Expected"),
                    document.createElement("hr"),
                    document.createTextNode(this.formatter.format(this.actual)),
                    ResultCell.createNodeWithText(document, "p", "Actual"));
        }

        @Override
        public String getCssClass()
        {
            return "fail";
        }

        @Override
        public Object getSummary()
        {
            Map<String, String> summary = new LinkedHashMap<>(2);
            summary.put("Expected", this.formatter.format(this.expected));
            summary.put("Actual", this.formatter.format(this.actual));
            return summary;
        }
    }

    private static class MissingCell extends ResultCell
    {
        private final Object expected;

        private MissingCell(CellFormatter formatter, Object expected)
        {
            super(formatter);
            this.expected = expected;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + "{expected=" + this.formatter.format(this.expected) + '}';
        }

        @Override
        public Object getExpected()
        {
            return this.expected;
        }

        @Override
        public Object getActual()
        {
            return null;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, CellFormatter.isNumber(this.expected),
                    document.createTextNode(this.formatter.format(this.expected)),
                    ResultCell.createNodeWithText(document, "p", "Missing"));
        }

        @Override
        public String getCssClass()
        {
            return "missing";
        }

        @Override
        public Object getSummary()
        {
            return this.formatter.format(this.expected);
        }
    }

    private static class SurplusCell extends ResultCell
    {
        private final Object actual;

        private SurplusCell(CellFormatter formatter, Object actual)
        {
            super(formatter);
            this.actual = actual;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + "{actual=" + this.formatter.format(this.actual) + '}';
        }

        @Override
        public Object getExpected()
        {
            return null;
        }

        @Override
        public Object getActual()
        {
            return this.actual;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, CellFormatter.isNumber(this.actual),
                    document.createTextNode(this.formatter.format(this.actual)),
                    ResultCell.createNodeWithText(document, "p", "Surplus"));
        }

        @Override
        public String getCssClass()
        {
            return "surplus";
        }

        @Override
        public Object getSummary()
        {
            return this.formatter.format(this.actual);
        }
    }

    private static class OutOfOrderCell extends ResultCell
    {
        final Object actualAndExpected;

        private OutOfOrderCell(CellFormatter formatter, Object actualAndExpected)
        {
            super(formatter);
            this.actualAndExpected = actualAndExpected;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + "{actualAndExpected=" + this.formatter.format(this.actualAndExpected) + '}';
        }

        @Override
        public Object getExpected()
        {
            return this.actualAndExpected;
        }

        @Override
        public Object getActual()
        {
            return this.actualAndExpected;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, CellFormatter.isNumber(this.actualAndExpected),
                    document.createTextNode(this.formatter.format(this.actualAndExpected)),
                    ResultCell.createNodeWithText(document, "p", "Out of order"));
        }

        @Override
        public String getCssClass()
        {
            return "outoforder";
        }

        @Override
        public Object getSummary()
        {
            return this.formatter.format(this.actualAndExpected);
        }
    }

    private static class SummaryCell extends ResultCell
    {
        private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
        private int maximumCardinalityToCount;
        private ColumnCardinality columnCardinality;

        private SummaryCell(int maximumCardinalityToCount, ColumnCardinality columnCardinality)
        {
            super(null);
            this.maximumCardinalityToCount = maximumCardinalityToCount;
            this.columnCardinality = columnCardinality;
        }

        @Override
        public Object getExpected()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getActual()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getCssClass()
        {
            return "summary";
        }

        @Override
        public String getSummary()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Node createCell(final Document document, boolean isHeaderRow)
        {
            final Element node = document.createElement("td");
            node.setAttribute("class", this.getCssClass() + " small");
            if (this.columnCardinality.isFull() || this.columnCardinality.getDistinctCount() > this.maximumCardinalityToCount)
            {
                node.appendChild(ResultCell.createNodeWithText(document, "span", ">" + NUMBER_FORMAT.format(this.maximumCardinalityToCount) + " distinct values", "italic"));
            }
            else
            {
                this.columnCardinality.forEachWithOccurrences((value, occurrences) -> {
                    if (value instanceof Map)
                    {
                        Map valueMap = (Map) value;
                        valueMap.forEach((k, v) -> {
                            node.appendChild(ResultCell.createNodeWithText(document, "span", k + " ", "grey"));
                            node.appendChild(getValueNode(document, v));
                        });
                    }
                    else
                    {
                        node.appendChild(getValueNode(document, value));
                    }
                    node.appendChild(ResultCell.createNodeWithText(document, "span", "- ", "grey"));
                    node.appendChild(ResultCell.createNodeWithText(document, "span", NUMBER_FORMAT.format(occurrences) + adaptOnCount(occurrences, " row"), "italic blue"));
                    node.appendChild(document.createElement("br"));
                });
            }
            return node;
        }

        private Node getValueNode(Document document, Object value)
        {
            return document.createTextNode(value + " ");
        }
    }

    private static class CustomCell extends ResultCell
    {
        private final String title;
        private final String cell;
        private final String cssClass;

        private CustomCell(String title, String cell, String cssClass)
        {
            super(null);
            this.title = title;
            this.cell = cell;
            this.cssClass = cssClass;
        }

        @Override
        public Object getExpected()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getActual()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            Element th = document.createElement(isHeaderRow ? "th" : "td");
            th.setAttribute("class", this.getCssClass());
            if (this.title != null)
            {
                th.setAttribute("title", this.title);
            }
            th.appendChild(document.createTextNode(this.cell));
            return th;
        }

        @Override
        public String getCssClass()
        {
            return this.cssClass;
        }

        @Override
        public String getSummary()
        {
            if (this.title != null)
            {
                return (this.title + ": " + this.cell);
            }
            return this.cell;
        }
    }
}
