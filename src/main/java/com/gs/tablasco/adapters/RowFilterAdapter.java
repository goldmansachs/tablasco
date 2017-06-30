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
import org.eclipse.collections.api.block.predicate.primitive.IntObjectPredicate;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

class RowFilterAdapter extends DefaultVerifiableTableAdapter
{
    private final IntArrayList indexMap;

    RowFilterAdapter(VerifiableTable delegate, IntObjectPredicate<VerifiableTable> rowFilter)
    {
        super(delegate);
        this.indexMap = new IntArrayList(delegate.getRowCount());
        for (int i = 0; i < delegate.getRowCount(); i++)
        {
            if (rowFilter.accept(i, delegate))
            {
                indexMap.add(i);
            }
        }
    }

    @Override
    public int getRowCount()
    {
        return this.indexMap.size();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        return super.getValueAt(this.indexMap.get(rowIndex), columnIndex);
    }
}
