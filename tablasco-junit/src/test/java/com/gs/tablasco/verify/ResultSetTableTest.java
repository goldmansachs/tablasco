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

import com.gs.tablasco.NamedTable;
import com.gs.tablasco.TableVerifier;
import com.gs.tablasco.TestTable;
import com.gs.tablasco.VerifiableTable;
import org.h2.tools.SimpleResultSet;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;

public class ResultSetTableTest {

    @Rule
    public final TableVerifier tableVerifier = new TableVerifier();

    @Test
    public void create() throws SQLException {
        SimpleResultSet resultSet = new SimpleResultSet();
        resultSet.addColumn("Name", Types.VARCHAR, 0, 0);
        resultSet.addColumn("Age", Types.INTEGER, 0, 0);
        resultSet.addColumn("Height", Types.DOUBLE, 0, 0);
        resultSet.addColumn("DoB", Types.DATE, 0, 0);
        resultSet.addRow("Joe", 70, 6.0, Date.valueOf("1940-02-16"));
        resultSet.addRow("Sue", 45, 5.8, Date.valueOf("1975-02-16"));
        VerifiableTable expected = new TestTable("Name", "Age", "Height", "DoB")
                .withRow("Joe", 70, 6.0, Date.valueOf("1940-02-16"))
                .withRow("Sue", 45, 5.8, Date.valueOf("1975-02-16"));
        VerifiableTable actual = ResultSetTable.create(resultSet);
        this.tableVerifier.verify(
                Collections.singletonList(new NamedTable("table", expected)),
                Collections.singletonList(new NamedTable("table", actual)));

    }
}