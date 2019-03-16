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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TableTestUtils
{
    static final VerifiableTable ACTUAL = new ListVerifiableTable(
            Arrays.<Object>asList("First", "Last", "Age"),
            Arrays.asList(
                    Arrays.asList("Barry", "White", 21.3),
                    Arrays.asList("Oscar", "White", 7.6)));
    static final VerifiableTable ACTUAL_2 = new ListVerifiableTable(
            Arrays.<Object>asList("First", "Last", "Age"),
            Collections.singletonList(
                    Arrays.asList("Elliot", "White", 3.8)));
    static final VerifiableTable ACTUAL_3 = new ListVerifiableTable(
            Arrays.<Object>asList("Name", "Age", "Weight", "Height"),
            Collections.singletonList(
                    Arrays.asList("Elliot", 1.1, 1.02, 1.5)));
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
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(verifier.getOutputFile()), StandardCharsets.UTF_8)))
        {
            StringBuilder html = new StringBuilder();
            boolean foundTable = false;
            String line = reader.readLine();
            while (line != null)
            {
                //System.out.println(line);
                if (line.startsWith("</" + tag))
                {
                    html.append(line);
                    return html.toString();
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

    static Map<String, VerifiableTable> doubletonMap(String n1, VerifiableTable t1, String n2, VerifiableTable t2) {

        Map<String, VerifiableTable> map = new LinkedHashMap<>();
        map.put(n1, t1);
        map.put(n2, t2);
        return map;
    }

    static Map<String, VerifiableTable> tripletonMap(String n1, VerifiableTable t1, String n2, VerifiableTable t2, String n3, VerifiableTable t3)
    {
        Map<String, VerifiableTable> map = doubletonMap(n1, t1, n2, t2);
        map.put(n3, t3);
        return map;
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
        try
        {
            runnable.run();
            Assert.fail("Expected AssertionError");
        }
        catch (AssertionError ignored) {}
    }
}
