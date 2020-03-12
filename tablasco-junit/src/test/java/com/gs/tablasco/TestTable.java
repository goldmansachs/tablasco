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

import java.util.ArrayList;
import java.util.List;

public class TestTable implements VerifiableTable
{
    private final String[] headers;
    private final List<Object[]> rows;

    public TestTable(String... headers)
    {
        this.headers = headers;
        this.rows = new ArrayList<>();
    }

    public TestTable withRow(Object... row)
    {
        if (row.length != this.headers.length)
        {
            throw new IllegalArgumentException("Row size " + row.length + " does not match header count " + this.headers.length);
        }
        this.rows.add(row);
        return this;
    }

    @Override
    public int getRowCount()
    {
        return this.rows.size();
    }

    @Override
    public int getColumnCount()
    {
        return this.headers.length;
    }

    @Override
    public String getColumnName(int columnIndex)
    {
        return this.headers[columnIndex];
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        return this.rows.get(rowIndex)[columnIndex];
    }
}
