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

import org.eclipse.collections.impl.factory.Maps;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class IgnoreColumnsTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy();

    @Test
    public void ignoreColumns() throws IOException
    {
        VerifiableTable table1 = TableTestUtils.createTable(4, "Col 1", "Col 2", "Col 3", "Col 4", "A1", "A2", "A3", "A4");
        VerifiableTable table2 = TableTestUtils.createTable(4, "Col 1", "Col 2", "Col 3", "Col 4", "A1", "XX", "A3", "XX");
        this.tableVerifier.withIgnoreColumns("Col 2", "Col 4").verify(Maps.fixedSize.of("name", table1), Maps.fixedSize.of("name", table2));

        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                "<tr>\n" +
                "<th class=\"pass\">Col 1</th>\n" +
                "<th class=\"pass\">Col 3</th>\n" +
                "</tr>\n" +
                "<tr>\n" +
                "<td class=\"pass\">A1</td>\n" +
                "<td class=\"pass\">A3</td>\n" +
                "</tr>\n" +
                "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}