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

import com.gs.tablasco.TestTable;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.ColumnComparators;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartialMatcherTest {
    private static final VerifiableTable MISSING = new TestTable(
                    "Entity", "Null-1", "Null-2", "Account", "Net", "MV", "Quantity")
            .withRow("GSIL", null, "", "71000", 100.0, 1000.0, 10.0)
            .withRow("GSCO", null, "", "91001", 500.0, 5000.0, 50.0)
            .withRow("GSCO", null, "", "91001", 500.0, 5000.0, 58.0);
    private static final VerifiableTable SURPLUS = new TestTable(
                    "Entity", "Null-1", "Null-2", "Account", "Net", "MV", "Quantity")
            .withRow("GSIL", "", null, "71000", 100.0, 9000.0, 90.0)
            .withRow("GSIL", "", null, "71000", 100.0, 9000.0, 10.0)
            .withRow("GSCO", "", "", "91001", 505.0, 5064.0, 58.0)
            .withRow("GSCO", "", null, "91001", 500.0, 5064.0, 58.0);
    private static final List<IndexMap> COLUMNS = Arrays.asList(
            new IndexMap(0, 0),
            new IndexMap(1, 1),
            new IndexMap(2, 2),
            new IndexMap(3, 3),
            new IndexMap(4, 4),
            new IndexMap(5, 5),
            new IndexMap(6, 6));

    private List<UnmatchedIndexMap> missing;
    private List<UnmatchedIndexMap> surplus;

    @Before
    public void setUp() {
        this.missing = new ArrayList<>();
        for (int i = 0; i < MISSING.getRowCount(); i++) {
            this.missing.add(new UnmatchedIndexMap(i, -1));
        }
        this.surplus = new ArrayList<>();
        for (int i = 0; i < SURPLUS.getRowCount(); i++) {
            this.surplus.add(new UnmatchedIndexMap(-1, i));
        }
    }

    @Test
    public void bestMatchPartialMatcher() {
        new BestMatchPartialMatcher(SURPLUS, MISSING, new ColumnComparators.Builder().build())
                .match(this.missing, this.surplus, COLUMNS);
        Assert.assertNull(this.surplus.get(0).getBestMutualMatch());
        Assert.assertSame(this.missing.get(0), this.surplus.get(1).getBestMutualMatch());
        Assert.assertSame(this.missing.get(1), this.surplus.get(2).getBestMutualMatch());
        Assert.assertSame(this.missing.get(2), this.surplus.get(3).getBestMutualMatch());
    }
}
