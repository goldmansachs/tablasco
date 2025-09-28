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

package com.gs.tablasco.core;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ListVerifiableTable;
import com.gs.tablasco.verify.ResultSetTable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@SuppressWarnings({"unchecked", "rawtypes"})
public class Tables {
    /**
     * Creates a {@link VerifiableTable} from a list containing headers and rows as lists. The first must be the list
     * of headers, the remaining items are rows as lists of objects. The size of each row must match the number of
     * headers.
     *
     * @param headersAndRows list of headers and rows
     * @return verifiable table
     */
    public static VerifiableTable fromList(List<List<?>> headersAndRows) {
        List<String> headers = (List<String>) headersAndRows.get(0);
        List rows = headersAndRows.subList(1, headersAndRows.size());
        return new ListVerifiableTable(headers, (List<List<Object>>) rows);
    }

    /**
     * Creates a {@link VerifiableTable} from a list of headers and list containing rows as lists. The size of each row
     * must match the number of headers.
     *
     * @param headers list of headers
     * @param rows list rows
     * @return the verifiable table
     */
    public static VerifiableTable fromList(List<String> headers, List<List<?>> rows) {
        List rowsList = rows;
        return new ListVerifiableTable(headers, (List<List<Object>>) rowsList);
    }

    /**
     * Creates a {@link VerifiableTable} from a JDBC {@link ResultSet}
     *
     * @param resultSet the result set
     * @return the verifiable table
     */
    public static VerifiableTable fromResultSet(ResultSet resultSet) throws SQLException {
        return ResultSetTable.create(resultSet);
    }
}
