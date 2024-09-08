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

import com.gs.tablasco.verify.DefaultVerifiableTableAdapter;
import com.gs.tablasco.verify.ListVerifiableTable;
import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableTestUtils
{
    static final VerifiableTable ACTUAL = new TestTable("First", "Last", "Age")
            .withRow("Barry", "White", 21.3)
            .withRow("Oscar", "White", 7.6);
    static final VerifiableTable ACTUAL_2 = new TestTable("First", "Last", "Age")
            .withRow("Elliot", "White", 3.8);
    static final VerifiableTable ACTUAL_3 = new TestTable("Name", "Age", "Weight", "Height")
            .withRow("Elliot", 1.1, 1.02, 1.5);
    static final String TABLE_NAME = "peopleTable";
    private static final DocumentBuilder DOCUMENT_BUILDER;
    static
    {
        try
        {
            DOCUMENT_BUILDER = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        }
        catch (ParserConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    static String getHtml(TableVerifier verifier, String tag) throws IOException
    {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(verifier.getOutputFile().toPath()), StandardCharsets.UTF_8)))
        {
            StringBuilder html = new StringBuilder();
            boolean foundTable = false;
            String line = reader.readLine();
            while (line != null)
            {
                line = line.trim();
                //System.out.println(line);
                if (line.startsWith("</" + tag))
                {
                    html.append(line);
                    return cleanHtmlForAssertion(html);
                }
                if (line.startsWith('<' + tag))
                {
                    foundTable = true;
                }
                if (foundTable)
                {
                    html.append(line).append('\n');
                }
                line = reader.readLine();
            }
        }
        return null;
    }

    private static String cleanHtmlForAssertion(StringBuilder html) {
        // Changes in Transformer post Java 8 seem to have caused some formatting differences.
        // This seems like the easiest/quickest for given it is just for test assertions (vs complete re-write)
        return html.toString()
                .replaceAll("[\n\r]+", "\n")
                .replaceAll(" class=\"surplus\">\n", " class=\"surplus\">")
                .replaceAll(" class=\"surplus number\">\n", " class=\"surplus number\">")
                .replaceAll("\n<p>Surplus</p>", "<p>Surplus</p>")
                .replaceAll(" class=\"missing\">\n", " class=\"missing\">")
                .replaceAll(" class=\"missing number\">\n", " class=\"missing number\">")
                .replaceAll("\n<p>Missing</p>", "<p>Missing</p>")
                .replaceAll(" class=\"fail\">\n", " class=\"fail\">")
                .replaceAll(" class=\"fail number\">\n", " class=\"fail number\">")
                .replaceAll(" class=\"outoforder\">\n", " class=\"outoforder\">")
                .replaceAll(" class=\"outoforder number\">\n", " class=\"outoforder number\">")
                .replaceAll(" class=\"summary small\">\n", " class=\"summary small\">")
                .replaceAll("\n<p>", "<p>")
                .replaceAll("<hr/>\n", "<hr/>")
                ;
    }

    /**
     * @deprecated use TestTable instead
     */
    @Deprecated
    public static VerifiableTable createTable(int cols, Object... values)
    {
        List<List<Object>> headersAndRows = new ArrayList<>();
        headersAndRows.add(Arrays.asList(values).subList(0, cols));
        int start = cols;
        while (start < values.length)
        {
            headersAndRows.add(Arrays.asList(values).subList(start, start + cols));
            start += cols;
        }
        // wrapping just to get coverage on default table adapter
        return new DefaultVerifiableTableAdapter(new ListVerifiableTable(headersAndRows))
        {
        };
    }

    public static Document parseHtml(File resultsFile) throws IOException, SAXException
    {
        return DOCUMENT_BUILDER.parse(resultsFile);
    }

    public static File getOutputDirectory()
    {
        return new File("target");
    }

    public static File getExpectedDirectory()
    {
        return new File("src/test/resources");
    }

    static List<NamedTable> toNamedTables(String n1, VerifiableTable t1)
    {
        List<NamedTable> list = new ArrayList<>();
        list.add(new NamedTable(n1, t1));
        return list;
    }

    static List<NamedTable> toNamedTables(String n1, VerifiableTable t1, String n2, VerifiableTable t2)
    {
        List<NamedTable> list = toNamedTables(n1, t1);
        list.add(new NamedTable(n2, t2));
        return list;
    }

    static List<NamedTable> toNamedTables(String n1, VerifiableTable t1, String n2, VerifiableTable t2, String n3, VerifiableTable t3)
    {
        List<NamedTable> list = toNamedTables(n1, t1, n2, t2);
        list.add(new NamedTable(n3, t3));
        return list;
    }

    public static class TestDescription extends TestWatcher
    {
        private Description description;

        @Override
        protected void starting(Description description)
        {
            this.description = description;
        }

        public Description get()
        {
            return this.description;
        }
    }

    public static void assertAssertionError(Runnable runnable)
    {
        AssertionError assertionError = null;
        try
        {
            runnable.run();
        }
        catch (AssertionError e)
        {
            assertionError = e;
        }
        Assert.assertNotNull("Expected AssertionError", assertionError);
    }
}
