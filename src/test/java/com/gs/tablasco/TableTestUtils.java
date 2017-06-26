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
import org.eclipse.collections.impl.list.fixed.ArrayAdapter;
import org.eclipse.collections.impl.list.mutable.FastList;
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
import java.util.Arrays;
import java.util.List;

public class TableTestUtils
{
    static final VerifiableTable ACTUAL = new ListVerifiableTable(
            Arrays.<Object>asList("First", "Last", "Age"),
            Arrays.asList(
                    Arrays.<Object>asList("Barry", "White", 21.3),
                    Arrays.<Object>asList("Oscar", "White", 7.6)));
    static final VerifiableTable ACTUAL_2 = new ListVerifiableTable(
            Arrays.<Object>asList("First", "Last", "Age"),
            Arrays.asList(
                    Arrays.<Object>asList("Elliot", "White", 3.8)));
    static final VerifiableTable ACTUAL_3 = new ListVerifiableTable(
            Arrays.<Object>asList("Name", "Age", "Weight", "Height"),
            Arrays.asList(
                    Arrays.<Object>asList("Elliot", 1.1, 1.02, 1.5)));
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
    };

    static String getHtml(TableVerifier verifier, String tag) throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(verifier.getOutputFile()), "UTF-8"));
        try
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
        finally
        {
            reader.close();
        }
        return null;
    }

    public static VerifiableTable createTable(int cols, Object... values)
    {
        List<List<Object>> headersAndRows = FastList.<List<Object>>newListWith(ArrayAdapter.adapt(values).subList(0, cols));
        int start = cols;
        while (start < values.length)
        {
            headersAndRows.add(ArrayAdapter.adapt(values).subList(start, start + cols));
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
        catch (AssertionError e) {}
    }
}
