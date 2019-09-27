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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class OutputEncodingSecurityTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy();

    @Test
    public void htmlTagsAreEncoded() throws IOException
    {
        final VerifiableTable table1 = TableTestUtils.createTable(1, "Col", "<script language=\"javascript\">alert(\"boo\")</script>", "<script language=\"javascript\">alert(\"boo\")</script>");
        final VerifiableTable table2 = TableTestUtils.createTable(1, "Col", "<script language=\"javascript\">alert(\"boo\")</script>", "<script language=\"javascript\">alert(\"foo\")</script>");
        TableTestUtils.assertAssertionError(() -> tableVerifier.verify(Collections.singletonMap("name", table1), Collections.singletonMap("name", table2)));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">&lt;script language=\"javascript\"&gt;alert(\"boo\")&lt;/script&gt;</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"surplus\">&lt;script language=\"javascript\"&gt;alert(\"foo\")&lt;/script&gt;<p>Surplus</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"missing\">&lt;script language=\"javascript\"&gt;alert(\"boo\")&lt;/script&gt;<p>Missing</p>\n" +
                "</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}