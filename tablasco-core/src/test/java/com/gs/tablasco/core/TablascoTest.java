package com.gs.tablasco.core;

import com.gs.tablasco.NamedTable;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ListVerifiableTable;
import com.gs.tablasco.verify.ResultTable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;
public class TablascoTest {

    private static final VerifiableTable T1 = new ListVerifiableTable(
            Arrays.asList("key", "value"), Collections.singletonList(Arrays.asList("a", 1)));
    private static final VerifiableTable T2 = new ListVerifiableTable(
            Arrays.asList("key", "value", "surplus"), Collections.singletonList(Arrays.asList("a", 2, "x")));

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void loadBaseline() {
    }

    @Test
    public void saveBaseline() {
    }

    @Test
    public void verifyTables() throws IOException {
        Tablasco tablasco = new Tablasco(new VerifierConfig().withIgnoreSurplusColumns(), new HtmlConfig().withHideMatchedColumns(true), "myTest");
        Map<String, ResultTable> verifiedTables = tablasco.verifyTables(
                Collections.singletonList(new NamedTable("table1", T1)),
                Collections.singletonList(new NamedTable("table1", T2)));
        Path path = this.temporaryFolder.newFile("results.html").toPath();
        tablasco.writeResults(path, verifiedTables);
        Assert.assertEquals(
                "<html>\n" +
                "    <head>\n" +
                "        <script>\n" +
                "function toggleVisibility(id){\n" +
                "var summary = document.getElementById(id);\n" +
                "if (summary.style.display === 'none') {\n" +
                "summary.style.display = 'table-row';\n" +
                "} else {\n" +
                "summary.style.display = 'none';\n" +
                "}\n" +
                "}\n" +
                "</script>\n" +
                "        <style type=\"text/css\">\n" +
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
                "h2 { font-size: small; margin-bottom: 4px; }\n" +
                "</style>\n" +
                "        <meta content=\"text/html;charset=UTF-8\" http-equiv=\"Content-type\"/>\n" +
                "        <title>Test Results</title>\n" +
                "    </head>\n" +
                "    <body>\n" +
                "        <div class=\"metadata\"/>\n" +
                "        <h1>myTest</h1>\n" +
                "        <div id=\"myTest.table1\">\n" +
                "            <h2>table1</h2>\n" +
                "            <table border=\"1\" cellspacing=\"0\">\n" +
                "                <tr>\n" +
                "                    <th class=\"pass\">key</th>\n" +
                "                    <th class=\"pass\">value</th>\n" +
                "                </tr>\n" +
                "                <tr>\n" +
                "                    <td class=\"pass\">a</td>\n" +
                "                    <td class=\"fail number\">\n" +
                "                        1\n" +
                "                        <p>Expected</p>\n" +
                "                        <hr/>\n" +
                "                        2\n" +
                "                        <p>Actual</p>\n" +
                "                        <hr/>\n" +
                "                        -1 / 100%\n" +
                "                        <p>Difference / Variance</p>\n" +
                "                    </td>\n" +
                "                </tr>\n" +
                "            </table>\n" +
                "        </div>\n" +
                "    </body>\n" +
                "</html>\n", new String(Files.readAllBytes(path), StandardCharsets.UTF_8).replaceAll("[\n\r]+", "\n"));
    }

    @Test
    public void writeResults() {
    }
}