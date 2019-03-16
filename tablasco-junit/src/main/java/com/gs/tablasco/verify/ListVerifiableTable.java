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

import com.gs.tablasco.VerifiableTable;

import java.util.List;

public class ListVerifiableTable implements VerifiableTable
{
    private final List<?> headers;
    private final List<List<Object>> data;

    public ListVerifiableTable(List<List<Object>> headersAndData)
    {
        this(headersAndData.get(0), headersAndData.subList(1, headersAndData.size()));
    }

    public ListVerifiableTable(List<?> headers, List<List<Object>> data)
    {
        this.headers = headers;
        this.data = data;
    }

    @Override
    public int getRowCount()
    {
        return this.data.size();
    }

    @Override
    public int getColumnCount()
    {
        return this.headers.size();
    }

    @Override
    public String getColumnName(int columnIndex)
    {
        return String.valueOf(this.headers.get(columnIndex));
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        return this.data.get(rowIndex).get(columnIndex);
    }
}
