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
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class IndexMapTest {
    private List<String> actual;
    private List<String> expected;

    @Test
    public void testStates() {
        Assert.assertTrue(im(0, 0).isMatched());
        Assert.assertTrue(im(2, 1).isMatched());
        Assert.assertFalse(im(1, -1).isMatched());
        Assert.assertFalse(im(-1, 0).isMatched());
        Assert.assertTrue(im(-1, 0).isSurplus());
        Assert.assertTrue(im(1, -1).isMissing());
    }

    @Test
    public void testMatchingColumns() {
        this.expectHeaderRow("column1", "column2", "column3");
        this.actualHeaderRow("column1", "column2", "column3");
        this.assertIndices(im(0, 0), im(1, 1), im(2, 2));
    }

    @Test
    public void testSurplusColumnAtEnd() {
        this.expectHeaderRow("column1", "column2", "column3", "column4");
        this.actualHeaderRow("column1", "column2", "column3", "column4", "column5");
        this.assertIndices(im(0, 0), im(1, 1), im(2, 2), im(3, 3), im(-1, 4));
    }

    @Test
    public void testSurplusColumnAtFront() {
        this.expectHeaderRow(/*      */ "column2", "column3", "column4", "column5");
        this.actualHeaderRow("column1", "column2", "column3", "column4", "column5");
        this.assertIndices(im(-1, 0), im(0, 1), im(1, 2), im(2, 3), im(3, 4));
    }

    @Test
    public void testSurplusColumnInMiddle() {
        this.expectHeaderRow("column1", "column2", /*      */ "column4", "column5");
        this.actualHeaderRow("column1", "column2", "column3", "column4", "column5");
        this.assertIndices(im(0, 0), im(1, 1), im(-1, 2), im(2, 3), im(3, 4));
    }

    @Test
    public void testTwoSurplusColumnsInMiddle() {
        this.expectHeaderRow("column1", /*                 */ "column4", "column5");
        this.actualHeaderRow("column1", "column2", "column3", "column4", "column5");
        this.assertIndices(im(0, 0), im(-1, 1), im(-1, 2), im(1, 3), im(2, 4));
    }

    @Test
    public void testMissingColumnAtFront() {
        this.expectHeaderRow("column1", "column2", "column3", "column4");
        this.actualHeaderRow(/*      */ "column2", "column3", "column4");
        this.assertIndices(im(0, -1), im(1, 0), im(2, 1), im(3, 2));
    }

    @Test
    public void testMissingColumnAtEnd() {
        this.expectHeaderRow("column1", "column2", "column3", "column4");
        this.actualHeaderRow("column1", "column2", "column3" /*     */);
        this.assertIndices(im(0, 0), im(1, 1), im(2, 2), im(3, -1));
    }

    @Test
    public void testMissingColumnInMiddle() {
        this.expectHeaderRow("column1", "column2", "column3", "column4");
        this.actualHeaderRow("column1", /*      */ "column3", "column4");
        this.assertIndices(im(0, 0), im(1, -1), im(2, 1), im(3, 2));
    }

    @Test
    public void testTwoMissingColumnsInMiddle() {
        this.expectHeaderRow("column1", "column2", "column3", "column4");
        this.actualHeaderRow("column1", /*                 */ "column4");
        this.assertIndices(im(0, 0), im(1, -1), im(2, -1), im(3, 1));
    }

    @Test
    public void testMultipleSurplusAndMissingColumns() {
        this.expectHeaderRow("column1", "column2", "column3", "column4", "column6", "column7");
        this.actualHeaderRow("column1", "column4", "column5", "column7", "column8");
        this.assertIndices(im(0, 0), im(1, -1), im(2, -1), im(3, 1), im(-1, 2), im(4, -1), im(5, 3), im(-1, 4));
    }

    @Test
    public void testOutOfOrderColumns() {
        this.expectHeaderRow("column1", "column2", "column3", "column4");
        this.actualHeaderRow("column1", "column2", "column4", "column3");
        this.assertIndices(im(0, 0), im(1, 1), im(3, 2), im(2, 3));
    }

    /* todo: fix transitive bug in compareTo()
    @Test
    public void transitive1()
    {
        IndexMap im1 = new IndexMap(154, 154);
        IndexMap im2 = new IndexMap(165, -1);
        IndexMap im3 = new IndexMap(181, 152);
        Assert.assertTrue(im1.compareTo(im2) < 0);
        Assert.assertTrue(im1.compareTo(im3) > 0);
        Assert.assertTrue(im2.compareTo(im3) > 0);
    }
    */

    @Test
    public void compareTo1() {
        IndexMap im1 = new IndexMap(1, 2);
        IndexMap im2 = new IndexMap(2, 1);
        Assert.assertTrue(im1.compareTo(im2) > 0);
        Assert.assertTrue(im2.compareTo(im1) < 0);
    }

    @Test
    public void compareTo2() {
        IndexMap im1 = new IndexMap(1, 2);
        IndexMap im2 = new IndexMap(2, -1);
        Assert.assertTrue(im1.compareTo(im2) < 0);
        Assert.assertTrue(im2.compareTo(im1) > 0);
    }

    private void assertIndices(IndexMap... cols) {
        IndexMapGenerator<String> generator =
                new IndexMapGenerator<>(this.expected.iterator(), this.actual.iterator(), 0);
        generator.generate();
        Assert.assertEquals(Arrays.asList(cols), generator.getAll());
    }

    private static IndexMap im(int ei, int ai) {
        return new IndexMap(ei, ai);
    }

    private void actualHeaderRow(String... header) {
        this.actual = Arrays.asList(header);
    }

    private void expectHeaderRow(String... header) {
        this.expected = Arrays.asList(header);
    }
}
