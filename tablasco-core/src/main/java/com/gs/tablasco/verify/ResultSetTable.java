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
import com.gs.tablasco.core.Tables;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetTable {

    private ResultSetTable() {}

    /**
     * Creates a {@link VerifiableTable} from a {@link ResultSet}
     *
     * @param resultSet the result set
     * @return a verifiable table
     * @throws SQLException from reading ResultSet
     */
    public static VerifiableTable create(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<String> headers = new ArrayList<>(columnCount);
        for (int n = 1; n <= columnCount; n++) {
            headers.add(metaData.getColumnName(n));
        }
        List<List<?>> rows = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>(columnCount);
            for (int n = 1; n <= columnCount; n++) {
                row.add(resultSet.getObject(n));
            }
            rows.add(row);
        }
        return Tables.fromList(headers, rows);
    }
}
