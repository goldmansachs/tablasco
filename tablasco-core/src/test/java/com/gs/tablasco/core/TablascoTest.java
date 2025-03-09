package com.gs.tablasco.core;

import com.gs.tablasco.NamedTable;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ListVerifiableTable;
import com.gs.tablasco.verify.ResultTable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TablascoTest {

    private static final VerifiableTable T1 =
            new ListVerifiableTable(Arrays.asList("key", "value"), Collections.singletonList(Arrays.asList("a", 1)));
    private static final VerifiableTable T2 = new ListVerifiableTable(
            Arrays.asList("key", "value", "surplus"), Collections.singletonList(Arrays.asList("a", 2, "x")));

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void loadBaseline() {}

    @Test
    public void saveBaseline() {}

    @Test
    public void verifyTables() throws IOException {
        Tablasco tablasco = new Tablasco(
                new VerifierConfig().withIgnoreSurplusColumns(),
                new HtmlConfig().withHideMatchedColumns(true),
                "myTest");
        Map<String, ResultTable> verifiedTables = tablasco.verifyTables(
                Collections.singletonList(new NamedTable("table1", T1)),
                Collections.singletonList(new NamedTable("table1", T2)));
        Path path = this.temporaryFolder.newFile("results.html").toPath();
        tablasco.writeResults(path, verifiedTables);
        Assert.assertEquals(
                """
                        <html>
                            <head>
                                <script>
                        function toggleVisibility(id){
                        var summary = document.getElementById(id);
                        if (summary.style.display === 'none') {
                        summary.style.display = 'table-row';
                        } else {
                        summary.style.display = 'none';
                        }
                        }
                        </script>
                                <style type="text/css">
                        * { padding: 0;margin: 0; }
                        body { color: black; padding: 4px; font-family: Verdana, Geneva, sans-serif; }
                        table { border-collapse: collapse; border: 0px; margin-bottom: 12px; }
                        th { font-weight: bold; }
                        td, th { white-space: nowrap; border: 1px solid black; vertical-align: top; font-size: small; padding: 2px; }
                        .pass { background-color: #c0ffc0; }
                        .fail { background-color: #ff8080; }
                        .outoforder { background-color: #d0b0ff; }
                        .missing { background-color: #cccccc; }
                        .surplus { background-color: #ffffcc; }
                        .summary { background-color: #f3f6f8; }
                        .number { text-align: right; }
                        .metadata { margin-bottom: 12px; }
                        .multi { font-style: italic; }
                        .blank_row { height: 10px; border: 0px; background-color: #ffffff; }
                        .grey { color: #999999; }
                        .blue { color: blue; }
                        .italic { font-style: italic; }
                        .link { color: blue; text-decoration: underline; cursor:pointer; font-style: italic }
                        .small { font-size: x-small; }
                        hr { border: 0px; color: black; background-color: black; height: 1px; margin: 2px 0px 2px 0px; }
                        p { font-style: italic; font-size: x-small; color: blue; padding: 3px 0 0 0; }
                        h1 { font-size: medium; margin-bottom: 4px; }
                        h2 { font-size: small; margin-bottom: 4px; }
                        </style>
                                <meta content="text/html;charset=UTF-8" http-equiv="Content-type"/>
                                <title>Test Results</title>
                            </head>
                            <body>
                                <div class="metadata"/>
                                <h1>myTest</h1>
                                <div id="myTest.table1">
                                    <h2>table1</h2>
                                    <table border="1" cellspacing="0">
                                        <tr>
                                            <th class="pass">key</th>
                                            <th class="pass">value</th>
                                        </tr>
                                        <tr>
                                            <td class="pass">a</td>
                                            <td class="fail number">
                                                1
                                                <p>Expected</p>
                                                <hr/>
                                                2
                                                <p>Actual</p>
                                                <hr/>
                                                -1 / 100%
                                                <p>Difference / Variance</p>
                                            </td>
                                        </tr>
                                    </table>
                                </div>
                            </body>
                        </html>
                        """,
                new String(Files.readAllBytes(path), StandardCharsets.UTF_8).replaceAll("[\n\r]+", "\n"));
    }

    @Test
    public void writeResults() {}
}
