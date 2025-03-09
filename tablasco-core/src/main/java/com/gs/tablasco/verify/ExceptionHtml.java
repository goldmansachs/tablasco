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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class ExceptionHtml
{
    public static void create(File resultsFile, Throwable reason)
    {
        try
        {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element html = document.createElement("html");
            document.appendChild(html);

            Element head = document.createElement("head");
            html.appendChild(head);

            Element meta = document.createElement("meta");
            meta.setAttribute("http-equiv", "Content-type");
            meta.setAttribute("content", "text/html;charset=UTF-8");
            head.appendChild(meta);

            head.appendChild(createNodeWithText(document, "title", "Error Message for: " + resultsFile.getName()));

            Element body = document.createElement("body");

            String errorMessage = stackTraceToString(reason);

            Element pre = createNodeWithText(document, "pre", errorMessage);

            body.appendChild(pre);

            html.appendChild(body);

            writeDocument(document, resultsFile);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Could not format exception", e);
        }
    }

    private static void writeDocument(Document document, File resultsFile) throws TransformerException, IOException
    {
        File parentDir = resultsFile.getParentFile();
        if (!parentDir.exists() && !parentDir.mkdirs())
        {
            throw new IllegalStateException("Unable to create results directory:" + parentDir);
        }
        Transformer trans = TransformerFactory.newInstance().newTransformer();
        trans.setOutputProperty(OutputKeys.METHOD, "xml");
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        trans.setOutputProperty(OutputKeys.INDENT, "yes");
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(resultsFile.toPath()), StandardCharsets.UTF_8)))
        {
            trans.transform(new DOMSource(document), new StreamResult(writer));
        }
    }

    private static Element createNodeWithText(Document document, String tagName, String content)
    {
        Element element = document.createElement(tagName);
        element.appendChild(document.createTextNode(content));
        return element;
    }

    static String stackTraceToString(Throwable e) throws UnsupportedEncodingException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, false, StandardCharsets.UTF_8);
        stackTraceToString(e, out);
        out.close();
        return bytes.toString(StandardCharsets.UTF_8);
    }

    private static void stackTraceToString(Throwable e, PrintStream out)
    {
        String prefix = "";
        Throwable cause = e;
        while (cause != null)
        {
            out.println(prefix + cause);
            for (StackTraceElement ste : cause.getStackTrace())
            {
                out.println("    " + ste.toString());
            }

            cause = cause.getCause();
            prefix = "Caused by: ";
        }
    }

    private ExceptionHtml()
    {
    }
}