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

import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.utility.StringIterate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HtmlFormatter
{
    public static final int DEFAULT_ROW_LIMIT = 10000;
    private static final LazyValue<DocumentBuilder> DOCUMENT_BUILDER = new LazyValue<DocumentBuilder>()
    {
        @Override
        protected DocumentBuilder initialize()
        {
            try
            {
                return DocumentBuilderFactory.newInstance().newDocumentBuilder();
            }
            catch (ParserConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
    };
    private static final LazyValue<Transformer> TRANSFORMER = new LazyValue<Transformer>()
    {
        @Override
        protected Transformer initialize()
        {
            try
            {
                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                return transformer;
            }
            catch (TransformerConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
    };
    private static final UnifiedSet<File> INITIALIZED_FILES = UnifiedSet.newSet();

    private final File outputFile;
    private final HtmlOptions htmlOptions;

    public HtmlFormatter(File outputFile, Set<String> tablesToHideMatchedRows, boolean displayAssertionSummary, boolean hideMatchedColumns, int htmlRowLimit)
    {
        this.outputFile = outputFile;
        this.htmlOptions = new HtmlOptions(tablesToHideMatchedRows, displayAssertionSummary, hideMatchedColumns, htmlRowLimit);
    }

    private Document initialize(Metadata metadata)
    {
        if (INITIALIZED_FILES.add(this.outputFile) && this.outputFile.exists() && !this.outputFile.delete())
        {
            throw new RuntimeException("Cannot delete output file " + this.outputFile.getName());
        }
        if (this.outputFile.exists())
        {
            try
            {
                return DOCUMENT_BUILDER.value().parse(this.outputFile);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error loading " + this.outputFile, e);
            }
        }
        return createNewDocument(metadata);
    }

    private static void ensurePathExists(File outputFile)
    {
        File path = outputFile.getParentFile();
        if (!path.exists() && !path.mkdirs())
        {
            throw new RuntimeException("Unable to create output directories for " + outputFile);
        }
    }

    private static Document createNewDocument(Metadata metadata)
    {
        Document document = DOCUMENT_BUILDER.value().newDocument();
        Element html = document.createElement("html");
        document.appendChild(html);

        Element head = document.createElement("head");
        html.appendChild(head);

        Element script = document.createElement("script");
        script.appendChild(document.createTextNode(getVisibilityFunction()));
        head.appendChild(script);

        Element style = document.createElement("style");
        style.setAttribute("type", "text/css");
        style.appendChild(document.createTextNode(getCSSDefinitions()));
        head.appendChild(style);

        Element meta = document.createElement("meta");
        meta.setAttribute("http-equiv", "Content-type");
        meta.setAttribute("content", "text/html;charset=UTF-8");
        head.appendChild(meta);

        head.appendChild(ResultCell.createNodeWithText(document, "title", "Test Results"));

        Element body = document.createElement("body");
        html.appendChild(body);

        Element div = document.createElement("div");
        div.setAttribute("class", "metadata");
        if (metadata != null)
        {
            div.appendChild(ResultCell.createNodeWithText(document, "i", metadata.toString()));
        }
        body.appendChild(div);

        return document;
    }

    public void appendResults(String testName, Map<String, ? extends FormattableTable> results, Metadata metadata)
    {
        this.appendResults(testName, results, metadata, 1);
    }

    public void appendResults(String testName, Map<String, ? extends FormattableTable> results, Metadata metadata, int verifyCount)
    {
        Document dom = this.initialize(metadata);
        ensurePathExists(this.outputFile);
        try (OutputStream outputStream = new FileOutputStream(this.outputFile))
        {
            appendResults(testName, results, metadata, verifyCount, dom, outputStream);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void appendResults(String testName, Map<String, ? extends FormattableTable> results, Metadata metadata, int verifyCount, Document dom, OutputStream outputStream) throws TransformerException, UnsupportedEncodingException
    {
        if (dom == null)
        {
            dom = createNewDocument(metadata);
        }
        Node body = dom.getElementsByTagName("body").item(0);
        if (verifyCount == 1)
        {
            body.appendChild(ResultCell.createNodeWithText(dom, "h1", testName));
        }

        if (this.htmlOptions.isDisplayAssertionSummary())
        {
            appendAssertionSummary(testName, results, body);
        }
        for (Map.Entry<String, ? extends FormattableTable> namedTable : results.entrySet())
        {
            appendResults(testName, namedTable.getKey(), namedTable.getValue(), body, true);
        }
        TRANSFORMER.value().transform(new DOMSource(dom), new StreamResult(new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))));
    }

    private void appendAssertionSummary(String testName, Map<String, ? extends FormattableTable> results, Node htmlBody)
    {
        int right = 0;
        int total = 0;
        for (FormattableTable table : results.values())
        {
            right += table.getPassedCellCount();
            total += table.getTotalCellCount();
        }
        double pctCorrect = Math.floor(1000.0 * right / total) / 10;
        String cellText = String.format("%d right, %d wrong, %.1f", right, total - right, pctCorrect) + "% correct";
        ResultCell cell = ResultCell.createCustomCell(cellText, right == total ? "pass" : "fail");
        appendResults(testName, "Assertions", new ResultTable(new boolean[] { true }, Collections.singletonList(Collections.singletonList(cell))), htmlBody, false);
    }

    private void appendResults(String testName, String tableName, FormattableTable resultTable, Node htmlBody, boolean withDivId)
    {
        Element table = getTableElement(testName, tableName, htmlBody, withDivId);
        resultTable.appendTo(testName, tableName, table, this.htmlOptions);
    }

    private Element getTableElement(String testName, String tableName, Node htmlBody, boolean withDivId)
    {
        Document document = htmlBody.getOwnerDocument();
        Element div = document.createElement("div");
        if (withDivId)
        {
            div.setAttribute("id", toHtmlId(testName, tableName));
        }
        htmlBody.appendChild(div);

        div.appendChild(ResultCell.createNodeWithText(document, "h2", tableName));

        Element table = document.createElement("table");
        table.setAttribute("border", "1");
        table.setAttribute("cellspacing", "0");
        div.appendChild(table);
        return table;
    }

    static void appendMultiMatchedRow(Element table, int colspan, int matchedRows)
    {
        Document document = table.getOwnerDocument();
        Element tr = document.createElement("tr");
        table.appendChild(tr);
        Element td = document.createElement("td");
        td.setAttribute("class", "pass multi");
        td.setAttribute("colspan", String.valueOf(colspan));
        td.appendChild(document.createTextNode(matchedRows + ResultCell.adaptOnCount(matchedRows, " matched row") + "..."));
        tr.appendChild(td);
    }

    static void appendDataRow(Element table, FormattableTable resultTable, String rowId, String rowStyle, List<ResultCell> resultCells, HtmlOptions htmlOptions)
    {
        Element tr = table.getOwnerDocument().createElement("tr");
        if (rowId != null)
        {
            tr.setAttribute("id", rowId);
        }
        if (rowStyle != null)
        {
            tr.setAttribute("style", rowStyle);
        }
        table.appendChild(tr);
        for (int col = 0; col < resultCells.size(); col++)
        {
            int matchedAhead = resultTable.getMatchedColumnsAhead(col);
            ResultCell resultCell = resultCells.get(col);
            if (htmlOptions.isHideMatchedColumns() && matchedAhead > 0)
            {
                resultCell = ResultCell.createCustomCell("\u00A0", resultCell.getCssClass());
                col += matchedAhead;
            }
            Node cell = resultCell.createCell(tr.getOwnerDocument(), false);
            tr.appendChild(cell);
        }
    }

    static void appendHeaderRow(Node table, FormattableTable resultTable, HtmlOptions htmlOptions)
    {
        final Element tr = table.getOwnerDocument().createElement("tr");
        table.appendChild(tr);
        List<ResultCell> headers = resultTable.getHeaders();
        for (int col = 0; col < headers.size(); col++)
        {
            int matchedAhead = resultTable.getMatchedColumnsAhead(col);
            ResultCell resultCell;
            if (htmlOptions.isHideMatchedColumns() && matchedAhead > 0)
            {
                resultCell = ResultCell.createCustomCell(String.format("%d matched columns", matchedAhead + 1), "...", "pass multi");
                col += matchedAhead;
            }
            else
            {
                resultCell = headers.get(col);
            }
            Node cell = resultCell.createCell(tr.getOwnerDocument(), true);
            tr.appendChild(cell);
        }
    }

    static void appendSpanningRow(Node table, FormattableTable resultTable, String cssClass, String data, String onDataClick)
    {
        Document document = table.getOwnerDocument();

        Element tr = document.createElement("tr");
        if (onDataClick != null)
        {
            tr.setAttribute("onclick", onDataClick);
        }
        table.appendChild(tr);

        Element td = document.createElement("td");
        td.setAttribute("class", cssClass);
        td.setAttribute("colspan", String.valueOf(resultTable.getHeaders().size()));
        if (data != null)
        {
            Element nodeWithText = ResultCell.createNodeWithText(document, "a", data, "link");
            td.appendChild(nodeWithText);
        }
        tr.appendChild(td);
    }

    static String toHtmlId(String testName, String tableName)
    {
        if (StringIterate.isEmpty(tableName))
        {
            return testName;
        }
        return testName.replaceAll("\\W+", "_") + '.' + tableName.replaceAll("\\W+", "_");
    }

    private static String getVisibilityFunction()
    {
        return "\n" +
                "function toggleVisibility(id){\n" +
                "var summary = document.getElementById(id);\n" +
                "if (summary.style.display === 'none') {\n" +
                "summary.style.display = 'table-row';\n" +
                "} else {\n" +
                "summary.style.display = 'none';\n" +
                "}\n" +
                "}\n";
    }

    private static String getCSSDefinitions()
    {
        return "\n" +
                "* { padding: 0;margin: 0; }\n" +
                "body { color: black; padding: 4px; font-family: Verdana, Geneva, sans-serif; }\n" +
                "table { border-collapse: collapse; border: 0px; margin-bottom: 12px; }\n" +
                "th { font-weight: bold; }\n" +
                "td, th { white-space: nowrap; border: 1px solid black; vertical-align: top; font-size: small; padding: 2px; }\n" +
                ".pass { background-color: #c0ffc0; }\n" +
                ".fail { background-color: #ff8080; }\n" +
                ".outoforder { background-color: #d0b0ff; }\n" +
                ".missing { background-color: #cccccc; }\n" +
                ".surplus { background-color: #ffffcc; }\n" +
                ".summary { background-color: #f3f6f8; }\n" +
                ".number { text-align: right; }\n" +
                ".metadata { margin-bottom: 12px; }\n" +
                ".multi { font-style: italic; }\n" +
                ".blank_row { height: 10px; border: 0px; background-color: #ffffff; }\n" +
                ".grey { color: #999999; }\n" +
                ".blue { color: blue; }\n" +
                ".italic { font-style: italic; }\n" +
                ".link { color: blue; text-decoration: underline; cursor:pointer; font-style: italic }\n" +
                ".small { font-size: x-small; }\n" +
                "hr { border: 0px; color: black; background-color: black; height: 1px; margin: 2px 0px 2px 0px; }\n" +
                "p { font-style: italic; font-size: x-small; color: blue; padding: 3px 0 0 0; }\n" +
                "h1 { font-size: medium; margin-bottom: 4px; }\n" +
                "h2 { font-size: small; margin-bottom: 4px; }\n";
    }

    static abstract class LazyValue<T>
    {
        private T value;

        protected abstract T initialize();

        public T value()
        {
            if (value == null)
            {
                value = initialize();
            }
            return value;
        }
    }
}
