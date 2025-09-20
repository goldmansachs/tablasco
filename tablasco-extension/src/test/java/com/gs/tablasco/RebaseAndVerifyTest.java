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

package com.gs.tablasco;

import static org.junit.jupiter.api.Assertions.*;

import com.gs.tablasco.verify.DefaultVerifiableTableAdapter;
import com.gs.tablasco.verify.ListVerifiableTable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class RebaseAndVerifyTest {
    private static final String ADAPT = "adapt";
    private static final Function<VerifiableTable, VerifiableTable> ACTUAL_ADAPTER = new Function<>() {
        @Override
        public VerifiableTable apply(VerifiableTable table) {
            return new DefaultVerifiableTableAdapter(table) {
                @Override
                public Object getValueAt(int rowIndex, int columnIndex) {
                    Object valueAt = super.getValueAt(rowIndex, columnIndex);
                    return valueAt == ADAPT ? ADAPT.toUpperCase() : valueAt;
                }
            };
        }
    };

    @RegisterExtension
    public final TableTestUtils.TestExtensionContext extensionContext = new TableTestUtils.TestExtensionContext();

    private final File expectedDir = new File(TableTestUtils.getOutputDirectory(), "expected");
    private final File outputDir = TableTestUtils.getOutputDirectory();
    private Document outputHtml;
    private File expectedFile;
    private String[] baselineHeaders = new String[] {"foo", "bar"};

    @AfterEach
    void assertMetadata() throws IOException {
        int rebaseMetadataCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(this.expectedFile))) {
            String line = reader.readLine();
            while (line != null) {
                if (line.contains("Metadata Key")) {
                    rebaseMetadataCount++;
                }
                line = reader.readLine();
            }
        }
        assertEquals(1, rebaseMetadataCount);

        if (this.outputHtml != null) {
            NodeList italics = this.outputHtml.getElementsByTagName("i");
            assertEquals(1, italics.getLength());
            assertTrue(italics.item(0).getFirstChild().getNodeValue().contains("Metadata Key"));
        }
    }

    @Test
    void rebasingOfMultipleDataTypes() throws Exception {
        VerifiableTable table = createTypedTable(
                "Hello World!",
                "11000",
                21000.0d,
                31000.0f,
                41000L,
                51000,
                new Date(Timestamp.valueOf("1993-02-16 23:59:00.0").getTime()));
        this.rebase(Collections.singletonMap("tableName", table));
        this.verify(Collections.singletonMap("tableName", table), 5.0d);
        this.verifyHtmlCells("Hello World!", "11000", "21,000", "31,000", "41,000", "51,000", "1993-02-16 23:59:00");
    }

    @Test
    void toleranceNotAppliedToIntegers() {
        assertThrows(AssertionError.class, () -> {
            VerifiableTable tableForRebase = createTypedTable(21000.0d, 31000.0f, 41000L, 51000);
            VerifiableTable tableForVerify = createTypedTable(21003.0d, 31003.0f, 41003L, 51003);
            this.rebase(Collections.singletonMap("tableName", tableForRebase));
            this.verify(Collections.singletonMap("tableName", tableForVerify), 5.0d);
        });
    }

    @Test
    void specialNumberHandling() throws Exception {
        VerifiableTable tableForRebase = TableTestUtils.createTable(
                6,
                "Double Inf",
                "Double Neg Inf",
                "Double NaN",
                "Float Inf",
                "Float Neg Inf",
                "Float NaN",
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                Double.NaN,
                Float.POSITIVE_INFINITY,
                Float.NEGATIVE_INFINITY,
                Float.NaN);
        this.rebase(Collections.singletonMap("tableName", tableForRebase));
        this.verify(Collections.singletonMap("tableName", tableForRebase), 1.0d);
        char infinity = '∞';
        this.verifyHtmlCells(
                String.valueOf(infinity), "-" + infinity, "NaN", String.valueOf(infinity), "-" + infinity, "NaN");
    }

    @Test
    void withActualAdapter() throws Exception {
        VerifiableTable table = TableTestUtils.createTable(2, "Key", "Val", "1", ADAPT);
        this.rebase(Collections.singletonMap("tableName", table));
        this.verify(Collections.singletonMap("tableName", table), 1.0d);
        this.verifyHtmlCells("1", "ADAPT");
    }

    @Test
    void formattingAppliedToAllNumbersButToleranceOnlyToFloatingPoint() throws Exception {
        VerifiableTable tableForRebase = createTypedTable(21000.0d, 31000.0f, 41000L, 51000);
        VerifiableTable tableForVerify = createTypedTable(21003.0d, 31003.0f, 41000L, 51000);
        this.rebase(Collections.singletonMap("tableName", tableForRebase));
        this.verify(Collections.singletonMap("tableName", tableForVerify), 5.0d);
        this.verifyHtmlCells("21,003", "31,003", "41,000", "51,000");
    }

    @Test
    void backslashHandling() throws Exception {
        VerifiableTable table = createTypedTable("Foo\\Bar");
        this.rebase(Collections.singletonMap("Side-by-side - Nettable\\NonNettable", table));
        this.verify(Collections.singletonMap("Side-by-side - Nettable\\NonNettable", table), 1.0d);
    }

    @Test
    void tableOrderControlledExternally() throws Exception {
        Map<String, VerifiableTable> tables = new LinkedHashMap<>();
        tables.put("tableC", createTypedTable("C"));
        tables.put("tableA", createTypedTable("A"));
        tables.put("tableB", createTypedTable("B"));
        this.rebase(tables);
        this.verify(tables, 1.0d);
        assertEquals(Arrays.asList("tableC", "tableA", "tableB"), getHtmlTagValues("h2"));
    }

    @Test
    void multipleVerifyCalls() throws Exception {
        Map<String, VerifiableTable> tables = new LinkedHashMap<>();
        tables.put("tableC", createTypedTable("C"));
        tables.put("tableA", createTypedTable("A"));
        tables.put("tableB", createTypedTable("B"));
        TableVerifier rebaseWatcher = this.newTableVerifier(1.0d).withRebase();
        rebaseWatcher.beforeEach(this.extensionContext.get());
        for (String name : tables.keySet()) {
            rebaseWatcher.verify(name, tables.get(name));
        }
        this.finishRebase(rebaseWatcher);

        TableVerifier runWatcher = this.newTableVerifier(1.0d);
        runWatcher.beforeEach(this.extensionContext.get());
        for (String name : tables.keySet()) {
            runWatcher.verify(name, tables.get(name));
        }
        runWatcher.afterEach(this.extensionContext.get());
        this.outputHtml = TableTestUtils.parseHtml(runWatcher.getOutputFile());

        assertEquals(Arrays.asList("tableC", "tableA", "tableB"), getHtmlTagValues("h2"));
    }

    @Test
    void withExcludeSvnHeadersInExpectedResults() throws Exception {
        this.baselineHeaders = null;
        VerifiableTable table = TableTestUtils.createTable(2, "Key", "Val", "1", "A");
        this.rebase(Collections.singletonMap("tableName", table));
        try (BufferedReader reader = new BufferedReader(new FileReader(this.expectedFile))) {
            String firstLine = reader.readLine();
            assertTrue(firstLine.startsWith("Metadata"));
        }
        this.verify(Collections.singletonMap("tableName", table), 1.0d);
        this.verifyHtmlCells("1", "A");
    }

    @Test
    void svnHeadersAreIgnoredByReader() throws Exception {
        VerifiableTable table = TableTestUtils.createTable(2, "Key", "Val", "1", "A");
        this.rebase(Collections.singletonMap("tableName", table));
        this.baselineHeaders = null;
        this.verify(Collections.singletonMap("tableName", table), 1.0d);
        this.verifyHtmlCells("1", "A");
    }

    private void verifyHtmlCells(String... expectedHtml) {
        assertEquals(Arrays.asList(expectedHtml), getHtmlTagValues("td"));
    }

    private void rebase(Map<String, VerifiableTable> actualForRebasing) {
        TableVerifier rebaseWatcher = this.newTableVerifier(1.0d).withRebase();
        rebaseWatcher.beforeEach(this.extensionContext.get());
        rebaseWatcher.verify(actualForRebasing);
        this.finishRebase(rebaseWatcher);
    }

    private void finishRebase(final TableVerifier rebaseWatcher) {
        TableTestUtils.assertAssertionError(() -> rebaseWatcher.afterEach(extensionContext.get()));
        this.expectedFile = rebaseWatcher.getExpectedFile();
    }

    private void verify(Map<String, VerifiableTable> actualForVerification, double tolerance) throws Exception {
        TableVerifier runWatcher = this.newTableVerifier(tolerance);
        runWatcher.beforeEach(this.extensionContext.get());
        runWatcher.verify(actualForVerification);
        runWatcher.afterEach(this.extensionContext.get());
        File outputFile = runWatcher.getOutputFile();
        this.outputHtml = TableTestUtils.parseHtml(outputFile);
    }

    private TableVerifier newTableVerifier(double tolerance) {
        return new TableVerifier()
                .withExpectedDir(this.expectedDir)
                .withOutputDir(this.outputDir)
                .withFilePerMethod()
                .withTolerance(tolerance)
                .withActualAdapter(ACTUAL_ADAPTER)
                .withMetadata("Metadata Key", "Metadata Value")
                .withBaselineHeaders(this.baselineHeaders);
    }

    private List<String> getHtmlTagValues(String tagName) {
        NodeList tdNodes = this.outputHtml.getElementsByTagName(tagName);
        List<String> tdValues = new ArrayList<>(tdNodes.getLength());
        for (int i = 0; i < tdNodes.getLength(); i++) {
            tdValues.add(tdNodes.item(i).getFirstChild().getNodeValue());
        }
        return tdValues;
    }

    // TODO: add test that uses decimals + varying tolerances (for double vs. long comparisons)

    // TODO: do we need timezone test for Dates?

    // TODO: write test for MAX & MIN & Nan of each data type (converting to double in the comparison will not work for
    // big longs)

    private static VerifiableTable createTypedTable(Object... values) {
        List<Object> headers = Arrays.stream(values)
                .map((Function<Object, Object>) object -> object.getClass().getSimpleName())
                .collect(Collectors.toList());
        return new ListVerifiableTable(headers, Collections.singletonList(Arrays.asList(values)));
    }
}
