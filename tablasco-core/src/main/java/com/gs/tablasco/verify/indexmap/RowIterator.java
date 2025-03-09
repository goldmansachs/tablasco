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
import com.gs.tablasco.verify.ColumnComparators;
import java.util.Iterator;
import java.util.List;

public abstract class RowIterator implements Iterator<RowView> {
    private final VerifiableTable table;
    private final List<IndexMap> columns;
    private final ColumnComparators columnComparators;
    private int rowIndex;
    private final int lastUnMatchedOffset;

    RowIterator(
            VerifiableTable table,
            List<IndexMap> columns,
            ColumnComparators columnComparators,
            int initialIndex,
            int lastUnMatchedOffset) {
        this.table = table;
        this.columns = columns;
        this.columnComparators = columnComparators;
        this.rowIndex = initialIndex;
        this.lastUnMatchedOffset = lastUnMatchedOffset;
    }

    @Override
    public boolean hasNext() {
        return this.rowIndex < this.table.getRowCount() - this.lastUnMatchedOffset;
    }

    @Override
    public RowView next() {
        RowView rowView = this.createRowView(this.rowIndex);
        this.rowIndex++;
        return rowView;
    }

    protected abstract RowView createRowView(int rowIndex);

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected VerifiableTable getTable() {
        return this.table;
    }

    protected List<IndexMap> getColumns() {
        return this.columns;
    }

    protected ColumnComparators getColumnComparators() {
        return this.columnComparators;
    }
}
