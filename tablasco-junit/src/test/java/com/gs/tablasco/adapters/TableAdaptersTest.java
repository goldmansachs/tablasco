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

package com.gs.tablasco.adapters;

import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.TableVerifier;
import com.gs.tablasco.VerifiableTable;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.block.predicate.primitive.IntObjectPredicate;
import org.eclipse.collections.impl.factory.Maps;
import org.junit.Rule;
import org.junit.Test;

public class TableAdaptersTest
{
    @Rule
    public final TableVerifier verifier = new TableVerifier()
            .withExpectedDir(TableTestUtils.getExpectedDirectory())
            .withOutputDir(TableTestUtils.getOutputDirectory())
            .withFilePerClass()
            .withHideMatchedRows(true)
            .withHideMatchedTables(true);

    @Test
    public void acceptAllRows()
    {
        this.verify(
                TableTestUtils.createTable(1, "C", 1, 2, 3, 4, 5),
                TableAdapters.withRows(TableTestUtils.createTable(1, "C", 1, 2, 3, 4, 5), new IntObjectPredicate<VerifiableTable>()
                {
                    @Override
                    public boolean accept(int i, VerifiableTable verifiableTable)
                    {
                        return true;
                    }
                }));
    }


    @Test
    public void acceptNoRows()
    {
        this.verify(
                TableTestUtils.createTable(1, "C"),
                TableAdapters.withRows(TableTestUtils.createTable(1, "C", 1, 2, 3, 4, 5), new IntObjectPredicate<VerifiableTable>()
                {
                    @Override
                    public boolean accept(int i, VerifiableTable verifiableTable)
                    {
                        return false;
                    }
                }));
    }

    @Test
    public void acceptSomeRows()
    {
        this.verify(
                TableTestUtils.createTable(1, "C", 2, 4),
                TableAdapters.withRows(TableTestUtils.createTable(1, "C", 1, 2, 3, 4, 5), new IntObjectPredicate<VerifiableTable>()
                {
                    @Override
                    public boolean accept(int i, VerifiableTable verifiableTable)
                    {
                        return (Integer) verifiableTable.getValueAt(i, 0) % 2 == 0;
                    }
                }));
    }

    @Test
    public void acceptAllColumns()
    {
        this.verify(
                TableTestUtils.createTable(5, "C1", "C2", "C3", "C4", "C5"),
                TableAdapters.withColumns(TableTestUtils.createTable(5, "C1", "C2", "C3", "C4", "C5"), new Predicate<String>()
                {
                    @Override
                    public boolean accept(String name)
                    {
                        return true;
                    }
                }));
    }

    @Test
    public void acceptSomeColumns()
    {
        this.verify(
                TableTestUtils.createTable(3, "C1", "C3", "C5"),
                TableAdapters.withColumns(TableTestUtils.createTable(5, "C1", "C2", "C3", "C4", "C5"), new Predicate<String>()
                {
                    @Override
                    public boolean accept(String name)
                    {
                        return name.matches("C[135]");
                    }
                }));
    }

    @Test
    public void composition1()
    {
        VerifiableTable table = TableTestUtils.createTable(2, "C1", "C2", 1, 2, 3, 4);
        VerifiableTable rowFilter = TableAdapters.withRows(TableAdapters.withColumns(table, new Predicate<String>()
        {
            @Override
            public boolean accept(String name)
            {
                return name.equals("C2");
            }
        }), new IntObjectPredicate<VerifiableTable>()
        {
            @Override
            public boolean accept(int i, VerifiableTable verifiableTable)
            {
                return i > 0;
            }
        });
        this.verify(TableTestUtils.createTable(1, "C2", 4), rowFilter);
    }

    @Test
    public void composition2()
    {
        VerifiableTable table = TableTestUtils.createTable(2, "C1", "C2", 1, 2, 3, 4);
        VerifiableTable columnFilter = TableAdapters.withColumns(
                TableAdapters.withRows(table, new IntObjectPredicate<VerifiableTable>()
                {
                    @Override
                    public boolean accept(int i, VerifiableTable verifiableTable)
                    {
                        return i > 0;
                    }
                }),
                new Predicate<String>()
                {
                    @Override
                    public boolean accept(String name)
                    {
                        return name.equals("C2");
                    }
                });
        this.verify(TableTestUtils.createTable(1, "C2", 4), columnFilter);
    }

    private void verify(VerifiableTable expected, VerifiableTable adaptedActual)
    {
        this.verifier.verify(Maps.fixedSize.of("table", adaptedActual), Maps.fixedSize.of("table", expected));
    }
}
