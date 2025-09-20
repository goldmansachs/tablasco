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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.gs.tablasco.VerifiableTable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class ListVerifiableTableTest {
    @Test
    void testHeaderTypes() throws Exception {
        // old usages prior to allowing String headers
        List<Object> headersAsObjects = Collections.singletonList("Col");
        List<List<Object>> headersAndDataAsObjects = Arrays.asList(headersAsObjects, Collections.singletonList("Val"));
        assertEquals(
                1,
                new ListVerifiableTable(headersAndDataAsObjects).getRowCount(),
                "Test constructor with headers and rows in one List<List<Object>>");
        assertEquals(
                2,
                new ListVerifiableTable(headersAsObjects, headersAndDataAsObjects).getRowCount(),
                "Test constructor with headers as List<Object>");
        List<String> headersAsStrings = Collections.singletonList("Col");
        assertEquals(
                2,
                new ListVerifiableTable(headersAsStrings, headersAndDataAsObjects).getRowCount(),
                "Test cast that used to be necessary for headers as List<String>");

        // allow passing string headers
        assertEquals(
                2,
                new ListVerifiableTable(headersAsStrings, headersAndDataAsObjects).getRowCount(),
                "Test headers as List<String> can now be passed in as-is");
    }

    @Test
    void createList() {
        VerifiableTable table = ListVerifiableTable.create(Arrays.asList(Arrays.asList("A", "B"), Arrays.asList(1, 2)));
        assertEquals(2, table.getColumnCount());
        assertEquals(1, table.getRowCount());
        assertEquals("A", table.getColumnName(0));
        assertEquals("B", table.getColumnName(1));
        assertEquals(1, table.getValueAt(0, 0));
        assertEquals(2, table.getValueAt(0, 1));
    }

    @Test
    void createList_headersNotStrings() {
        assertThrows(IllegalArgumentException.class, () -> {
            ListVerifiableTable.create(Arrays.asList(Arrays.asList('A', 'B'), Arrays.asList(1, 2)));
        });
    }

    @Test
    void createList_wrongRowSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            ListVerifiableTable.create(
                    Arrays.asList(Arrays.asList("A", "B"), Arrays.asList(1, 2), Collections.singletonList(3)));
        });
    }

    @Test
    void createHeadersAndList() {
        VerifiableTable table =
                ListVerifiableTable.create(Arrays.asList("A", "B"), Collections.singletonList(Arrays.asList(1, 2)));
        assertEquals(2, table.getColumnCount());
        assertEquals(1, table.getRowCount());
        assertEquals("A", table.getColumnName(0));
        assertEquals("B", table.getColumnName(1));
        assertEquals(1, table.getValueAt(0, 0));
        assertEquals(2, table.getValueAt(0, 1));
    }

    @Test
    void createHeadersAndList_wrongRowSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            ListVerifiableTable.create(
                    Arrays.asList("A", "B"), Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5)));
        });
    }

    @Test
    void createHeadersAndIterable() {
        VerifiableTable table =
                ListVerifiableTable.create(Arrays.asList("A", "B"), Collections.singleton(Arrays.asList(1, 2)));
        assertEquals(2, table.getColumnCount());
        assertEquals(1, table.getRowCount());
        assertEquals("A", table.getColumnName(0));
        assertEquals("B", table.getColumnName(1));
        assertEquals(1, table.getValueAt(0, 0));
        assertEquals(2, table.getValueAt(0, 1));
    }

    @Test
    void createHeadersAndIterable_wrongRowSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            ListVerifiableTable.create(
                    Arrays.asList("A", "B"),
                    new LinkedHashSet<>(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5))));
        });
    }
}
