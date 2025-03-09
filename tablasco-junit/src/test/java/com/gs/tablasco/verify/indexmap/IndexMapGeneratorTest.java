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

import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

public class IndexMapGeneratorTest {
    @Test
    public void indexOrderIsCorrect() {
        IndexMapGenerator<String> generator = new IndexMapGenerator<>(
                Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J").iterator(),
                Arrays.asList("F", "G", "H", "I", "J", "K", "L", "M", "N", "O").iterator(),
                0);
        generator.generate();
        Assert.assertEquals(Arrays.asList(im(5, 0), im(6, 1), im(7, 2), im(8, 3), im(9, 4)), generator.getMatched());
        Assert.assertEquals(
                Arrays.asList(uim(0, -1), uim(1, -1), uim(2, -1), uim(3, -1), uim(4, -1)), generator.getMissing());
        Assert.assertEquals(
                Arrays.asList(uim(-1, 5), uim(-1, 6), uim(-1, 7), uim(-1, 8), uim(-1, 9)), generator.getSurplus());
    }

    private static IndexMap im(int expectedIndex, int actualIndex) {
        return new IndexMap(expectedIndex, actualIndex);
    }

    private static UnmatchedIndexMap uim(int expectedIndex, int actualIndex) {
        return new UnmatchedIndexMap(expectedIndex, actualIndex);
    }
}
