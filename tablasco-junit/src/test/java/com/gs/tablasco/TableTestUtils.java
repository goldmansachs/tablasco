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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.gs.tablasco.core.Tables;
import com.gs.tablasco.verify.DefaultVerifiableTableAdapter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class TableTestUtils {
    static final VerifiableTable ACTUAL = new TestTable("First", "Last", "Age")
            .withRow("Barry", "White", 21.3)
            .withRow("Oscar", "White", 7.6);
    static final VerifiableTable ACTUAL_2 = new TestTable("First", "Last", "Age").withRow("Elliot", "White", 3.8);
    static final VerifiableTable ACTUAL_3 =
            new TestTable("Name", "Age", "Weight", "Height").withRow("Elliot", 1.1, 1.02, 1.5);
    static final String TABLE_NAME = "peopleTable";
    private static final DocumentBuilder DOCUMENT_BUILDER;

    static {
        try {
            DOCUMENT_BUILDER = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    static String getHtml(TableVerifier verifier) throws IOException {
        return Files.readString(verifier.getOutputFile().toPath(), StandardCharsets.UTF_8);
    }

    /*
     * Use TestTable instead
     */
    public static VerifiableTable createTable(int cols, Object... values) {
        List<List<?>> headersAndRows = new ArrayList<>();
        headersAndRows.add(Arrays.asList(values).subList(0, cols));
        int start = cols;
        while (start < values.length) {
            headersAndRows.add(Arrays.asList(values).subList(start, start + cols));
            start += cols;
        }
        // wrapping just to get coverage on default table adapter
        return new DefaultVerifiableTableAdapter(Tables.fromList(headersAndRows)) {};
    }

    public static Document parseHtml(File resultsFile) throws IOException, SAXException {
        return DOCUMENT_BUILDER.parse(resultsFile);
    }

    public static File getOutputDirectory() {
        return new File("target");
    }

    public static File getExpectedDirectory() {
        return new File("src/test/resources");
    }

    static List<NamedTable> toNamedTables(String n1, VerifiableTable t1) {
        List<NamedTable> list = new ArrayList<>();
        list.add(new NamedTable(n1, t1));
        return list;
    }

    static List<NamedTable> toNamedTables(String n1, VerifiableTable t1, String n2, VerifiableTable t2) {
        List<NamedTable> list = toNamedTables(n1, t1);
        list.add(new NamedTable(n2, t2));
        return list;
    }

    static List<NamedTable> toNamedTables(
            String n1, VerifiableTable t1, String n2, VerifiableTable t2, String n3, VerifiableTable t3) {
        List<NamedTable> list = toNamedTables(n1, t1, n2, t2);
        list.add(new NamedTable(n3, t3));
        return list;
    }

    public static class TestExtensionContext implements BeforeEachCallback {
        private ExtensionContext description;

        @Override
        public void beforeEach(ExtensionContext context) {
            this.description = context;
        }

        public ExtensionContext get() {
            return this.description;
        }
    }

    public static void assertAssertionError(Runnable runnable) {
        AssertionError assertionError = null;
        try {
            runnable.run();
        } catch (AssertionError e) {
            assertionError = e;
        }
        assertNotNull(assertionError, "Expected AssertionError");
    }
}
