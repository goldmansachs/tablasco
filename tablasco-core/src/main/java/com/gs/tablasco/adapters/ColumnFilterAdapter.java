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
import java.util.function.Predicate;

class ColumnFilterAdapter extends DefaultVerifiableTableAdapter
{
    private final int[] indexMap;
    private int columnCount = 0;

    ColumnFilterAdapter(VerifiableTable delegate, Predicate<String> columnFilter)
    {
        super(delegate);
        this.indexMap = new int[delegate.getColumnCount()];
        for (int i = 0; i < delegate.getColumnCount(); i++)
        {
            if (columnFilter.test(delegate.getColumnName(i)))
            {
                indexMap[this.columnCount++] = i;
            }
        }
    }

    @Override
    public int getColumnCount()
    {
        return this.columnCount;
    }

    @Override
    public String getColumnName(int columnIndex)
    {
        return super.getColumnName(this.indexMap[columnIndex]);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        return super.getValueAt(rowIndex, this.indexMap[columnIndex]);
    }
}
