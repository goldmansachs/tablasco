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

import com.gs.tablasco.core.HtmlConfig;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HtmlFormatter {
    public static final int DEFAULT_ROW_LIMIT = 10000;
    private static final LazyValue<DocumentBuilder> DOCUMENT_BUILDER = new LazyValue<>() {
        @Override
        protected DocumentBuilder initialize() {
            try {
                return DocumentBuilderFactory.newInstance().newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
    };
    private static final LazyValue<Transformer> TRANSFORMER = new LazyValue<>() {
        @Override
        protected Transformer initialize() {
            try {
                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                return transformer;
            } catch (TransformerConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
    };
    private static final Set<File> INITIALIZED_FILES = new HashSet<>();

    private final File outputFile;
    private final HtmlOptions htmlOptions;
    private final Set<File> initializedFiles;

    public HtmlFormatter(File outputFile, HtmlConfig htmlConfig) {
        this(outputFile, htmlConfig, INITIALIZED_FILES);
    }

    public HtmlFormatter(File outputFile, HtmlConfig htmlConfig, Set<File> initializedFiles) {
        this.outputFile = outputFile;
        this.htmlOptions = new HtmlOptions(
                htmlConfig.isShowAssertionSummary(),
                htmlConfig.getHtmlRowLimit(),
                htmlConfig.isHideMatchedTables(),
                htmlConfig.isHideMatchedRows(),
                htmlConfig.isHideMatchedColumns(),
                htmlConfig.isSummarizedResults(),
                htmlConfig.getTablesToAlwaysShowMatchedRowsFor());
        this.initializedFiles = initializedFiles;
    }

    private Document initialize(Metadata metadata) {
        if (initializedFiles.add(this.outputFile) && this.outputFile.exists() && !this.outputFile.delete()) {
            throw new RuntimeException("Cannot delete output file " + this.outputFile.getName());
        }
        if (this.outputFile.exists()) {
            try {
                return DOCUMENT_BUILDER.value().parse(this.outputFile);
            } catch (Exception e) {
                throw new RuntimeException("Error loading " + this.outputFile, e);
            }
        }
        return createNewDocument(metadata);
    }

    private static void ensurePathExists(File outputFile) {
        File path = outputFile.getParentFile();
        if (!path.exists() && !path.mkdirs()) {
            throw new RuntimeException("Unable to create output directories for " + outputFile);
        }
    }

    private static Document createNewDocument(Metadata metadata) {
        try (InputStream in = HtmlFormatter.class.getResourceAsStream("/tablasco.html")) {
            Document document = DOCUMENT_BUILDER.value().parse(in);
            if (metadata != null) {
                Element div = getTagById(document, "div", "tablasco-metadata");
                div.appendChild(ResultCell.createNodeWithText(document, "i", metadata.toString()));
            }
            return document;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (SAXException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Element getTagById(Document document, String tagName, String elementId) {
        NodeList list = document.getElementsByTagName(tagName);
        for (int i = 0; i < list.getLength(); i++) {
            Node div1 = list.item(i);
            if (div1 instanceof Element) {
                Element div = (Element) div1;
                if (elementId.equals(div.getAttribute("id"))) {
                    return div;
                }
            }
        }
        throw new IllegalStateException("No div with id " + elementId + " found");
    }

    public void appendResults(String testName, Map<String, ResultTable> results, Metadata metadata) {
        Map<String, FormattableTable> resultsToFormat = new LinkedHashMap<>();
        for (String name : results.keySet()) {
            ResultTable resultTable = results.get(name);
            boolean dontFormat = this.htmlOptions.isHideMatchedTables() && resultTable.isSuccess();
            if (!dontFormat) {
                resultsToFormat.put(
                        name,
                        this.htmlOptions.isSummarizedResults() ? new SummaryResultTable(resultTable) : resultTable);
            }
        }
        if (!resultsToFormat.isEmpty()) {
            Document dom = this.initialize(metadata);
            ensurePathExists(this.outputFile);
            try (OutputStream outputStream = Files.newOutputStream(this.outputFile.toPath())) {
                appendResults(testName, resultsToFormat, metadata, dom, outputStream);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void appendResults(
            String testName,
            Map<String, ? extends FormattableTable> results,
            Metadata metadata,
            Document dom,
            OutputStream outputStream)
            throws TransformerException {
        if (dom == null) {
            dom = createNewDocument(metadata);
        }

        Node titleNode = getTagById(dom, "h1", "tablasco-title");
        titleNode.setTextContent(testName);

        if (this.htmlOptions.isDisplayAssertionSummary()) {
            Node summaryNode = getTagById(dom, "div", "tablasco-summary");
            appendAssertionSummary(testName, results, summaryNode);
        }

        Node resultsNode = getTagById(dom, "div", "tablasco-results");
        for (Map.Entry<String, ? extends FormattableTable> namedTable : results.entrySet()) {
            appendResults(testName, namedTable.getKey(), namedTable.getValue(), resultsNode, true);
        }
        TRANSFORMER
                .value()
                .transform(
                        new DOMSource(dom),
                        new StreamResult(
                                new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))));
    }

    private void appendAssertionSummary(
            String testName, Map<String, ? extends FormattableTable> results, Node summaryNode) {
        int right = 0;
        int total = 0;
        for (FormattableTable table : results.values()) {
            right += table.getPassedCellCount();
            total += table.getTotalCellCount();
        }
        double pctCorrect = Math.floor(1000.0 * right / total) / 10;
        String cellText = String.format("%d right, %d wrong, %.1f", right, total - right, pctCorrect) + "% correct";
        ResultCell cell = ResultCell.createCustomCell(cellText, right == total ? "pass" : "fail");
        appendResults(
                testName,
                "Assertions",
                new ResultTable(new boolean[] {true}, Collections.singletonList(Collections.singletonList(cell))),
                summaryNode,
                false);
    }

    private void appendResults(
            String testName, String tableName, FormattableTable resultTable, Node resultsNode, boolean withDivId) {
        Element table = getTableElement(testName, tableName, resultsNode, withDivId);
        resultTable.appendTo(testName, tableName, table, this.htmlOptions);
    }

    private Element getTableElement(String testName, String tableName, Node resultsNode, boolean withDivId) {
        Document document = resultsNode.getOwnerDocument();
        Element div = document.createElement("div");
        if (withDivId) {
            div.setAttribute("id", toHtmlId(testName, tableName));
        }
        resultsNode.appendChild(div);

        div.appendChild(ResultCell.createNodeWithText(document, "h2", tableName));

        Element table = document.createElement("table");
        div.appendChild(table);
        return table;
    }

    static void appendMultiMatchedRow(Element table, int colspan, int matchedRows) {
        Document document = table.getOwnerDocument();
        Element tr = document.createElement("tr");
        table.appendChild(tr);
        Element td = document.createElement("td");
        td.setAttribute("class", "pass multi");
        td.setAttribute("colspan", String.valueOf(colspan));
        td.appendChild(
                document.createTextNode(matchedRows + ResultCell.adaptOnCount(matchedRows, " matched row") + "..."));
        tr.appendChild(td);
    }

    static void appendDataRow(
            Element table,
            FormattableTable resultTable,
            String rowId,
            String rowStyle,
            List<ResultCell> resultCells,
            HtmlOptions htmlOptions) {
        Element tr = table.getOwnerDocument().createElement("tr");
        if (rowId != null) {
            tr.setAttribute("id", rowId);
        }
        if (rowStyle != null) {
            tr.setAttribute("style", rowStyle);
        }
        table.appendChild(tr);
        for (int col = 0; col < resultCells.size(); col++) {
            int matchedAhead = resultTable.getMatchedColumnsAhead(col);
            ResultCell resultCell = resultCells.get(col);
            if (htmlOptions.isHideMatchedColumns() && matchedAhead > 0) {
                resultCell = ResultCell.createCustomCell("\u00A0", resultCell.getCssClass());
                col += matchedAhead;
            }
            Node cell = resultCell.createCell(tr.getOwnerDocument(), false);
            tr.appendChild(cell);
        }
    }

    static void appendHeaderRow(Node table, FormattableTable resultTable, HtmlOptions htmlOptions) {
        final Element tr = table.getOwnerDocument().createElement("tr");
        table.appendChild(tr);
        List<ResultCell> headers = resultTable.getHeaders();
        for (int col = 0; col < headers.size(); col++) {
            int matchedAhead = resultTable.getMatchedColumnsAhead(col);
            ResultCell resultCell;
            if (htmlOptions.isHideMatchedColumns() && matchedAhead > 0) {
                resultCell = ResultCell.createCustomCell(
                        String.format("%d matched columns", matchedAhead + 1), "...", "pass multi");
                col += matchedAhead;
            } else {
                resultCell = headers.get(col);
            }
            Node cell = resultCell.createCell(tr.getOwnerDocument(), true);
            tr.appendChild(cell);
        }
    }

    static void appendSpanningRow(
            Node table, FormattableTable resultTable, String cssClass, String data, String onDataClick) {
        Document document = table.getOwnerDocument();

        Element tr = document.createElement("tr");
        if (onDataClick != null) {
            tr.setAttribute("onclick", onDataClick);
        }
        table.appendChild(tr);

        Element td = document.createElement("td");
        td.setAttribute("class", cssClass);
        td.setAttribute("colspan", String.valueOf(resultTable.getHeaders().size()));
        if (data != null) {
            Element nodeWithText = ResultCell.createNodeWithText(document, "a", data, "link");
            td.appendChild(nodeWithText);
        }
        tr.appendChild(td);
    }

    static String toHtmlId(String testName, String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return testName;
        }
        return testName.replaceAll("\\W+", "_") + '.' + tableName.replaceAll("\\W+", "_");
    }

    abstract static class LazyValue<T> {
        private T value;

        protected abstract T initialize();

        public T value() {
            if (value == null) {
                value = initialize();
            }
            return value;
        }
    }
}
