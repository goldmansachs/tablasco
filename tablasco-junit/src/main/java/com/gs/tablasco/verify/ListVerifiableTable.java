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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

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

    /**
     * Creates a {@link com.gs.tablasco.VerifiableTable} from an iterable containing headers and rows as lists. The
     * first must be the list of headers as strings, the remaining items are rows as lists of objects. The size of each
     * row must match the number of headers.
     *
     * @param headersAndRows iterable of headers and rows
     * @return verifiable table
     */
    public static VerifiableTable create(Iterable<List> headersAndRows)
    {
        Iterator<List> iterator = headersAndRows.iterator();
        List headers = iterator.next();
        headers.forEach(ListVerifiableTable::verifyHeader);
        List rowList = new ArrayList();
        iterator.forEachRemaining(verifyRowSize(headers).andThen(rowList::add));
        return new ListVerifiableTable(headers, rowList);
    }

    private static void verifyHeader(Object obj) {
        if ((!(obj instanceof String)))
        {
            throw new IllegalArgumentException("Invalid header " + obj);
        }
    }

    /**
     * Creates a {@link com.gs.tablasco.VerifiableTable} from a list of headers and an iterable containing rows as
     * lists. The size of each row must match the number of headers.
     *
     * @param headers list of headers
     * @param rows iterable rows
     * @return the verifiable table
     */
    public static VerifiableTable create(List<String> headers, Iterable<List> rows)
    {
        if (rows instanceof List)
        {
            rows.forEach(verifyRowSize(headers));
            return new ListVerifiableTable(headers, (List) rows);
        }
        List rowList = new ArrayList();
        rows.forEach(verifyRowSize(headers).andThen(rowList::add));
        return new ListVerifiableTable(headers, rowList);
    }

    private static Consumer<List> verifyRowSize(final List headers) {
        return row -> {
            if (row.size() != headers.size())
            {
                throw new IllegalArgumentException(String.format("Row size %d does not match header size %s", row.size(), headers.size()));
            }
        };
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
