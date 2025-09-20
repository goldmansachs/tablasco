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

import static org.junit.jupiter.api.Assertions.*;

import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.core.HtmlConfig;
import com.gs.tablasco.core.VerifierConfig;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class MultiTableVerifierTest {
    private static final CellComparator CELL_COMPARATOR = new ToleranceCellComparator(new CellFormatter(1.0, false));

    public String testName;

    private MultiTableVerifier verifier;
    private File resultsFile;
    private int expectedTables;
    private boolean expectingException = false;

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        Optional<Method> testMethod = testInfo.getTestMethod();
        testMethod.ifPresent(method -> this.testName = method.getName());
        this.resultsFile = new File(
                TableTestUtils.getOutputDirectory(),
                MultiTableVerifierTest.class.getSimpleName() + '_' + this.testName + ".html");
        Files.deleteIfExists(this.resultsFile.toPath());
        this.verifier = new MultiTableVerifier(new VerifierConfig().withVerifyRowOrder(true));
    }

    @AfterEach
    void tearDown() throws IOException, SAXException {
        if (!expectingException) {
            assertTrue(this.resultsFile.exists());
            Document html = TableTestUtils.parseHtml(this.resultsFile);
            assertEquals(this.expectedTables, html.getElementsByTagName("table").getLength());
        }
    }

    @Test
    void missingTable() {
        Map<String, ResultTable> results = verifyTables(createTables("assets"), createTables("assets", "liabs"));
        assertEquals(newPassTable(), results.get("assets").getVerifiedRows());
        assertEquals(newMissingTable(), results.get("liabs").getVerifiedRows());
        this.expectedTables = 2;
    }

    @Test
    void surplusTable() {
        Map<String, ResultTable> results = this.verifyTables(createTables("assets", "liabs"), createTables("liabs"));
        assertEquals(newSurplusTable(), results.get("assets").getVerifiedRows());
        assertEquals(newPassTable(), results.get("liabs").getVerifiedRows());
        this.expectedTables = 2;
    }

    @Test
    void misnamedTable() {
        Map<String, ResultTable> results =
                this.verifyTables(createTables("assets", "liabs"), createTables("assets", "liabz"));
        assertEquals(newPassTable(), results.get("assets").getVerifiedRows());
        assertEquals(newSurplusTable(), results.get("liabs").getVerifiedRows());
        assertEquals(newMissingTable(), results.get("liabz").getVerifiedRows());
        this.expectedTables = 3;
    }

    @Test
    void noExpectedColumns() {
        this.expectingException = true;
        assertThrows(IllegalStateException.class, () -> {
            this.verifyTables(
                    Collections.singletonMap("table", TableTestUtils.createTable(1, "Col")),
                    Collections.singletonMap("table", TableTestUtils.createTable(0)));
        });
    }

    @Test
    void noActualColumns() {
        this.expectingException = true;
        assertThrows(IllegalStateException.class, () -> {
            this.verifyTables(
                    Collections.singletonMap("table", TableTestUtils.createTable(0)),
                    Collections.singletonMap("table", TableTestUtils.createTable(1, "Col")));
        });
    }

    private Map<String, ResultTable> verifyTables(
            Map<String, VerifiableTable> actualResults, Map<String, VerifiableTable> expectedResults) {
        Map<String, ResultTable> results = this.verifier.verifyTables(expectedResults, actualResults);
        HtmlFormatter htmlFormatter = new HtmlFormatter(this.resultsFile, new HtmlConfig());
        htmlFormatter.appendResults(this.testName, results, null);
        return results;
    }

    private List<List<ResultCell>> newMissingTable() {
        return Arrays.asList(
                Collections.singletonList(ResultCell.createMissingCell(CELL_COMPARATOR.getFormatter(), "Heading")),
                Collections.singletonList(ResultCell.createMissingCell(CELL_COMPARATOR.getFormatter(), "Value")));
    }

    private List<List<ResultCell>> newSurplusTable() {
        return Arrays.asList(
                Collections.singletonList(ResultCell.createSurplusCell(CELL_COMPARATOR.getFormatter(), "Heading")),
                Collections.singletonList(ResultCell.createSurplusCell(CELL_COMPARATOR.getFormatter(), "Value")));
    }

    private static List<List<ResultCell>> newPassTable() {
        return Arrays.asList(
                Collections.singletonList(ResultCell.createMatchedCell(CELL_COMPARATOR, "Heading", "Heading")),
                Collections.singletonList(ResultCell.createMatchedCell(CELL_COMPARATOR, "Value", "Value")));
    }

    private static Map<String, VerifiableTable> createTables(String... names) {
        Map<String, VerifiableTable> tables = new HashMap<>();
        for (String name : names) {
            tables.put(name, new VerifiableTable() {
                @Override
                public int getRowCount() {
                    return 1;
                }

                @Override
                public int getColumnCount() {
                    return 1;
                }

                @Override
                public String getColumnName(int columnIndex) {
                    return "Heading";
                }

                @Override
                public Object getValueAt(int rowIndex, int columnIndex) {
                    return "Value";
                }
            });
        }
        return tables;
    }
}
