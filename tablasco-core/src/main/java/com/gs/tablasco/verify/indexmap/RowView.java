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

package com.gs.tablasco.verify.indexmap;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.CellComparator;
import com.gs.tablasco.verify.ColumnComparators;
import java.util.List;

public abstract class RowView {
    private final VerifiableTable table;
    private final List<IndexMap> columnIndices;
    private final int rowIndex;
    private final ColumnComparators columnComparators;

    RowView(VerifiableTable table, List<IndexMap> columnIndices, ColumnComparators columnComparators, int rowIndex) {
        this.table = table;
        this.columnIndices = columnIndices;
        this.rowIndex = rowIndex;
        this.columnComparators = columnComparators;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (IndexMap column : this.columnIndices) {
            if (column.isMatched()) {
                CellComparator comparator = getCellComparator(column);
                hashCode += comparator.computeHashCode(this.getValue(column));
            }
        }
        return hashCode;
    }

    private CellComparator getCellComparator(IndexMap column) {
        return this.columnComparators.getComparator(this.table.getColumnName(this.getColumnIndex(column)));
    }

    private Object getValue(IndexMap column) {
        return this.table.getValueAt(this.rowIndex, getColumnIndex(column));
    }

    protected abstract int getColumnIndex(IndexMap column);

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowView) {
            RowView that = (RowView) obj;
            for (IndexMap column : this.columnIndices) {
                if (column.isMatched()) {
                    Object thisVal = this.getValue(column);
                    Object thatVal = that.getValue(column);
                    if (!this.getCellComparator(column).equals(thisVal, thatVal)) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }
}
