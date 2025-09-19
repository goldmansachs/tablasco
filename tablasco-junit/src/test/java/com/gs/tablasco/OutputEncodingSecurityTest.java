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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

public class OutputEncodingSecurityTest {
    @Rule
    public final TableVerifier tableVerifier =
            new TableVerifier().withFilePerMethod().withMavenDirectoryStrategy();

    @Test
    void htmlTagsAreEncoded() throws IOException {
        final VerifiableTable table1 = TableTestUtils.createTable(
                1,
                "Col",
                "<script language=\"javascript\">alert(\"boo\")</script>",
                "<script language=\"javascript\">alert(\"boo\")</script>");
        final VerifiableTable table2 = TableTestUtils.createTable(
                1,
                "Col",
                "<script language=\"javascript\">alert(\"boo\")</script>",
                "<script language=\"javascript\">alert(\"foo\")</script>");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify("name", table1, table2));
        assertEquals(
                """
                        <table border="1" cellspacing="0">
                        <tr>
                        <th class="pass">Col</th>
                        </tr>
                        <tr>
                        <td class="pass">&lt;script language="javascript"&gt;alert("boo")&lt;/script&gt;</td>
                        </tr>
                        <tr>
                        <td class="surplus">&lt;script language="javascript"&gt;alert("foo")&lt;/script&gt;<p>Surplus</p>
                        </td>
                        </tr>
                        <tr>
                        <td class="missing">&lt;script language="javascript"&gt;alert("boo")&lt;/script&gt;<p>Missing</p>
                        </td>
                        </tr>
                        </table>""",
                TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}
