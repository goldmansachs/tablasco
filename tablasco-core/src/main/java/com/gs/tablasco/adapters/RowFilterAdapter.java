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

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.DefaultVerifiableTableAdapter;
import java.util.function.IntPredicate;

class RowFilterAdapter extends DefaultVerifiableTableAdapter {
    private final int[] indexMap;
    private int rowCount = 0;

    RowFilterAdapter(VerifiableTable delegate, IntPredicate rowFilter) {
        super(delegate);
        this.indexMap = new int[delegate.getRowCount()];
        for (int i = 0; i < delegate.getRowCount(); i++) {
            if (rowFilter.test(i)) {
                indexMap[this.rowCount++] = i;
            }
        }
    }

    @Override
    public int getRowCount() {
        return this.rowCount;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return super.getValueAt(this.indexMap[rowIndex], columnIndex);
    }
}
