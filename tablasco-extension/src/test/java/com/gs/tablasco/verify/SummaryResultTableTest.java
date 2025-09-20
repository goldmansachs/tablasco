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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class SummaryResultTableTest {
    @Test
    void isSerializable() throws IOException, ClassNotFoundException {
        CellComparator cellComparator = new ToleranceCellComparator(new CellFormatter(1.0, false));
        List<ResultCell> row = Arrays.asList(
                ResultCell.createMatchedCell(cellComparator, "A", "A"),
                ResultCell.createMatchedCell(cellComparator, "A", "B"),
                ResultCell.createMissingCell(cellComparator.getFormatter(), "A"),
                ResultCell.createSurplusCell(cellComparator.getFormatter(), "A"),
                ResultCell.createOutOfOrderCell(cellComparator.getFormatter(), "A"));
        SummaryResultTable table = new SummaryResultTable(new ResultTable(new boolean[5], Arrays.asList(row, row)));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(out)) {
            objectOutputStream.writeObject(table);
        }
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()))) {
            SummaryResultTable tableOverWire = (SummaryResultTable) objectInputStream.readObject();
            assertFalse(tableOverWire.isSuccess());
            assertEquals(10, tableOverWire.getTotalCellCount());
        }
    }

    @Test
    void merge() {
        CellComparator cellComparator = new ToleranceCellComparator(new CellFormatter(1.0, false));
        SummaryResultTable table1 = new SummaryResultTable(new ResultTable(
                new boolean[2],
                Arrays.asList(
                        List.of(ResultCell.createMatchedCell(cellComparator, "Key", "Val")),
                        List.of(ResultCell.createMatchedCell(cellComparator, "A", "A")))));
        SummaryResultTable table2 = new SummaryResultTable(new ResultTable(
                new boolean[2],
                Arrays.asList(
                        List.of(ResultCell.createMatchedCell(cellComparator, "Key", "Val")),
                        List.of(ResultCell.createMatchedCell(cellComparator, "A", "B")))));
        SummaryResultTable table3 = new SummaryResultTable(new ResultTable(
                new boolean[2],
                Arrays.asList(
                        List.of(ResultCell.createMatchedCell(cellComparator, "Key", "Val")),
                        List.of(ResultCell.createMatchedCell(cellComparator, "A", "B")))));
        table1.merge(table2);
        table1.merge(table3);
        assertEquals("{0={firstFew=1, totalRows=1}, 31={firstFew=2, totalRows=2}}", asString(table1));
        assertEquals("{31={firstFew=1, totalRows=1}}", asString(table2));
        assertEquals("{31={firstFew=1, totalRows=1}}", asString(table3));
    }

    private String asString(SummaryResultTable table) {
        return table.getResultsByKey().toString();
    }
}
