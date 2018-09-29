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

import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ExampleTableVerifierTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier().withMavenDirectoryStrategy();

    @Test
    public void example()
    {
        List<BalanceSheetRow> rows = Arrays.asList(
                new BalanceSheetRow("GSCO", 160.0, -40.0),
                new BalanceSheetRow("GSIL", 70.0, -30.0));
        this.tableVerifier.verify("balanceSheet", new BalanceSheetTable(rows));
    }

    private static class BalanceSheetRow
    {
        private final String entity;
        private final double assets;
        private final double liabilities;

        private BalanceSheetRow(String entity, double assets, double liabilities)
        {
            this.entity = entity;
            this.assets = assets;
            this.liabilities = liabilities;
        }
    }

    private static class BalanceSheetTable implements VerifiableTable
    {
        private final List<BalanceSheetRow> rows;

        private BalanceSheetTable(List<BalanceSheetRow> rows)
        {
            this.rows = rows;
        }

        @Override
        public int getRowCount()
        {
            return this.rows.size();
        }

        @Override
        public int getColumnCount()
        {
            return 3;
        }

        @Override
        public String getColumnName(int columnIndex)
        {
            switch (columnIndex)
            {
                case 0: return "Entity";
                case 1: return "Assets";
                default: return "Liabilities";
            }
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            BalanceSheetRow row = this.rows.get(rowIndex);
            switch (columnIndex)
            {
                case 0: return row.entity;
                case 1: return row.assets;
                default: return row.liabilities;
            }
        }
    }
}
