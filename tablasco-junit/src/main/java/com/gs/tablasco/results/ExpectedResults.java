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

package com.gs.tablasco.results;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.Metadata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExpectedResults
{
    private final Map<String, Map<String, VerifiableTable>> tablesByTestName = new LinkedHashMap<>();
    private final Metadata metadata = Metadata.newEmpty();

    public VerifiableTable getTable(String testName)
    {
        return this.getTable(testName, null);
    }

    public Map<String, VerifiableTable> getTables(String testName)
    {
        Map<String, VerifiableTable> tables = this.tablesByTestName.get(testName);
        return tables == null ? Collections.emptyMap() : tables;
    }

    public VerifiableTable getTable(String testName, String tableName)
    {
        return this.getTables(testName).get(translateTableName(tableName));
    }

    public void addTable(String testName, String tableName, VerifiableTable table)
    {
        Map<String, VerifiableTable> tables = this.tablesByTestName.computeIfAbsent(testName, k -> new LinkedHashMap<>());
        String key = translateTableName(tableName);
        if (tables.containsKey(key))
        {
            throw new IllegalStateException("Duplicate expected table detected: " + testName + '/' + tableName);
        }
        tables.put(key, table);
    }

    private static String translateTableName(String tableName)
    {
        return tableName == null ? "" : tableName;
    }

    public void addMetadata(String key, String value)
    {
        this.metadata.add(key, value);
    }

    public Metadata getMetadata()
    {
        return this.metadata;
    }
}
