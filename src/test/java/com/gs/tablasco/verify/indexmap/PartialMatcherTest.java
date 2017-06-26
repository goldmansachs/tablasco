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

package com.gs.tablasco.verify.indexmap;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.ListVerifiableTable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class PartialMatcherTest
{
    private static final VerifiableTable MISSING = new ListVerifiableTable(
            FastList.<Object>newListWith("Entity", "Null-1", "Null-2", "Account", "Net", "MV", "Quantity"),
            FastList.<List<Object>>newListWith(
                    FastList.<Object>newListWith("GSIL", null, "", "71000", 100.0, 1000.0, 10.0),
                    FastList.<Object>newListWith("GSCO", null, "", "91001", 500.0, 5000.0, 50.0),
                    FastList.<Object>newListWith("GSCO", null, "", "91001", 500.0, 5000.0, 58.0)));
    private static final VerifiableTable SURPLUS = new ListVerifiableTable(
            FastList.<Object>newListWith("Entity", "String", "Account", "Net", "MV", "Quantity"),
            FastList.<List<Object>>newListWith(
                    FastList.<Object>newListWith("GSIL", "", null, "71000", 100.0, 9000.0, 90.0),
                    FastList.<Object>newListWith("GSIL", "", null, "71000", 100.0, 9000.0, 10.0),
                    FastList.<Object>newListWith("GSCO", "", "", "91001", 505.0, 5064.0, 58.0),
                    FastList.<Object>newListWith("GSCO", "", null, "91001", 500.0, 5064.0, 58.0)));
    private static final MutableList<IndexMap> COLUMNS = FastList.newListWith(
            new IndexMap(0, 0), new IndexMap(1, 1), new IndexMap(2, 2), new IndexMap(3, 3), new IndexMap(4, 4), new IndexMap(5, 5), new IndexMap(6, 6));

    private MutableList<UnmatchedIndexMap> missing;
    private MutableList<UnmatchedIndexMap> surplus;

    @Before
    public void setUp()
    {
        this.missing = FastList.newList();
        for (int i = 0; i < MISSING.getRowCount(); i++)
        {
            this.missing.add(new UnmatchedIndexMap(i, -1));
        }
        this.surplus = FastList.newList();
        for (int i = 0; i < SURPLUS.getRowCount(); i++)
        {
            this.surplus.add(new UnmatchedIndexMap(-1, i));
        }
    }

    @Test
    public void bestMatchPartialMatcher()
    {
        new BestMatchPartialMatcher(SURPLUS, MISSING, new ColumnComparators.Builder().build()).match(this.missing, this.surplus, COLUMNS);
        Assert.assertNull(this.surplus.get(0).getBestMutualMatch());
        Assert.assertSame(this.missing.get(0), this.surplus.get(1).getBestMutualMatch());
        Assert.assertSame(this.missing.get(1), this.surplus.get(2).getBestMutualMatch());
        Assert.assertSame(this.missing.get(2), this.surplus.get(3).getBestMutualMatch());
    }
}
