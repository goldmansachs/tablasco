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

package com.gs.tablasco.results.parser;

import com.gs.tablasco.VerifiableTable;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.io.StreamTokenizer;
import java.text.ParseException;
import java.util.List;

public class ExpectedTable implements VerifiableTable
{
    private final List<String> headers = FastList.newList();
    private final List<List<?>> rowValues = FastList.newList();

    public void addColumnHeader(String header)
    {
        this.headers.add(header);
    }

    public void parseData(StreamTokenizer st, int currentNumber, List<Object> rowValue) throws ParseException
    {
        if (currentNumber >= this.headers.size())
        {
            throw new ParseException("extra data on line " + st.lineno(), st.lineno());
        }
        if (st.ttype == StreamTokenizer.TT_NUMBER)
        {
            rowValue.add(st.nval);
        }
        else
        {
            rowValue.add(st.sval);
        }
    }

    public void addRowToList(List<Object> rowValue)
    {
        this.rowValues.add(rowValue);
    }

    @Override
    public int getRowCount()
    {
        return this.rowValues.size();
    }

    @Override
    public int getColumnCount()
    {
        return this.headers.size();
    }

    @Override
    public String getColumnName(int columnIndex)
    {
        return columnIndex < this.getColumnCount() ? this.headers.get(columnIndex) : null;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        if (rowIndex < this.getRowCount())
        {
            List<?> rowData = this.rowValues.get(rowIndex);
            if (columnIndex < rowData.size())
            {
                return rowData.get(columnIndex);
            }
        }
        return null;
    }
}