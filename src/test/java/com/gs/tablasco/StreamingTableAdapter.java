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

import com.gs.tablasco.verify.StreamingVerifiableTable;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.Interval;

import java.util.List;

public class StreamingTableAdapter implements StreamingVerifiableTable
{
    private final VerifiableTable table;
    private final List<String> headers;
    private int rowIndex = 0;

    public StreamingTableAdapter(final VerifiableTable table)
    {
        this.table = table;
        this.headers = Interval.fromTo(0, table.getColumnCount() - 1).collect(new Function<Integer, String>()
        {
            @Override
            public String valueOf(Integer integer)
            {
                return table.getColumnName(integer);
            }
        }).toList();
    }

    @Override
    public List nextRow()
    {
        if (this.rowIndex >= this.table.getRowCount())
        {
            return null;
        }
        MutableList<Object> nextRow = Interval.fromTo(0, this.table.getColumnCount() - 1).collect(new Function<Integer, Object>()
        {
            @Override
            public Object valueOf(Integer integer)
            {
                return table.getValueAt(rowIndex, integer);
            }
        }).toList();
        this.rowIndex++;
        return nextRow;
    }

    @Override
    public List<String> getHeaders()
    {
        return this.headers;
    }
}
