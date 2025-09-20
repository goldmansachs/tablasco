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

package com.gs.tablasco.adapters;

import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.TableVerifier;
import com.gs.tablasco.VerifiableTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TableAdaptersTest {

    @RegisterExtension
    private final TableVerifier verifier = new TableVerifier()
            .withExpectedDir(TableTestUtils.getExpectedDirectory())
            .withOutputDir(TableTestUtils.getOutputDirectory())
            .withFilePerClass()
            .withHideMatchedRows(true)
            .withHideMatchedTables(true);

    @Test
    void testAllRows() {
        this.verify(
                TableTestUtils.createTable(1, "C", 1, 2, 3, 4, 5),
                TableAdapters.withRows(TableTestUtils.createTable(1, "C", 1, 2, 3, 4, 5), i -> true));
    }

    @Test
    void testNoRows() {
        VerifiableTable table = TableTestUtils.createTable(1, "C", 1, 2, 3, 4, 5);
        this.verify(TableTestUtils.createTable(1, "C"), TableAdapters.withRows(table, i -> false));
    }

    @Test
    void testSomeRows() {
        VerifiableTable table = TableTestUtils.createTable(1, "C", 1, 2, 3, 4, 5);
        this.verify(
                TableTestUtils.createTable(1, "C", 2, 4),
                TableAdapters.withRows(table, i -> (Integer) table.getValueAt(i, 0) % 2 == 0));
    }

    @Test
    void testAllColumns() {
        this.verify(
                TableTestUtils.createTable(5, "C1", "C2", "C3", "C4", "C5"),
                TableAdapters.withColumns(TableTestUtils.createTable(5, "C1", "C2", "C3", "C4", "C5"), name -> true));
    }

    @Test
    void testSomeColumns() {
        this.verify(
                TableTestUtils.createTable(3, "C1", "C3", "C5"),
                TableAdapters.withColumns(
                        TableTestUtils.createTable(5, "C1", "C2", "C3", "C4", "C5"), name -> name.matches("C[135]")));
    }

    @Test
    void composition1() {
        VerifiableTable table = TableTestUtils.createTable(2, "C1", "C2", 1, 2, 3, 4);
        VerifiableTable rowFilter =
                TableAdapters.withRows(TableAdapters.withColumns(table, name -> name.equals("C2")), i -> i > 0);
        this.verify(TableTestUtils.createTable(1, "C2", 4), rowFilter);
    }

    @Test
    void composition2() {
        VerifiableTable table = TableTestUtils.createTable(2, "C1", "C2", 1, 2, 3, 4);
        VerifiableTable columnFilter =
                TableAdapters.withColumns(TableAdapters.withRows(table, i -> i > 0), name -> name.equals("C2"));
        this.verify(TableTestUtils.createTable(1, "C2", 4), columnFilter);
    }

    private void verify(VerifiableTable expected, VerifiableTable adaptedActual) {
        this.verifier.verify("table", adaptedActual, expected);
    }
}
