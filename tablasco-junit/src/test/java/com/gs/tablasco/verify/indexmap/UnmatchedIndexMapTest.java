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

import org.junit.Assert;
import org.junit.Test;

public class UnmatchedIndexMapTest {
    private final UnmatchedIndexMap unmatched = new UnmatchedIndexMap(0, 0);
    private final UnmatchedIndexMap unmatched1 = new UnmatchedIndexMap(1, 1);
    private final UnmatchedIndexMap unmatched2 = new UnmatchedIndexMap(2, 2);

    @Test(expected = IllegalArgumentException.class)
    public void testAddPartialMatchFailsIfSelf() {
        this.unmatched.addMatch(2, this.unmatched);
    }

    @Test
    public void testInitialState() {
        Assert.assertNull(this.unmatched.getBestMutualMatch());
    }

    @Test
    public void addSinglePartialMatch() {
        this.unmatched.addMatch(1, this.unmatched1);
        Assert.assertTrue(this.unmatched.match());
        Assert.assertEquals(this.unmatched1, this.unmatched.getBestMutualMatch());
        Assert.assertEquals(this.unmatched, this.unmatched1.getBestMutualMatch());
    }

    @Test
    public void bestMatchAddedFirst() {
        this.unmatched.addMatch(2, this.unmatched1);
        this.unmatched.addMatch(1, this.unmatched2);
        Assert.assertTrue(this.unmatched.match());
        Assert.assertEquals(this.unmatched1, this.unmatched.getBestMutualMatch());
        Assert.assertEquals(this.unmatched, this.unmatched1.getBestMutualMatch());
    }

    @Test
    public void bestMatchAddedLast() {
        this.unmatched.addMatch(1, this.unmatched2);
        this.unmatched.addMatch(2, this.unmatched1);
        Assert.assertTrue(this.unmatched.match());
        Assert.assertEquals(this.unmatched1, this.unmatched.getBestMutualMatch());
        Assert.assertEquals(this.unmatched, this.unmatched1.getBestMutualMatch());
    }
}
