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

/**
 * A table with a name
 */
public class NamedTable {
    private final String name;
    private final VerifiableTable table;

    /**
     * @param name the name
     * @param table the table
     */
    public NamedTable(String name, VerifiableTable table) {
        this.name = name;
        this.table = table;
    }

    /**
     * @return the table name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the verifiable table
     */
    public VerifiableTable getTable() {
        return table;
    }
}
