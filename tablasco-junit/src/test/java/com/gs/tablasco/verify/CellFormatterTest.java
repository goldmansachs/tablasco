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

import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

public class CellFormatterTest
{
    @Test
    public void formatNumbers()
    {
        CellFormatter formatter = new CellFormatter(0.01, true);
        Assert.assertEquals("1", formatter.format(1.0d));
        Assert.assertEquals("1.1", formatter.format(1.10d));
        Assert.assertEquals("1.11", formatter.format(1.11d));
        Assert.assertEquals("1.11", formatter.format(1.111d));
        Assert.assertEquals("1.12", formatter.format(1.116d));
        Assert.assertEquals("-1.12", formatter.format(-1.116d));
        Assert.assertEquals("-1,000.12", formatter.format(-1000.116d));

        Assert.assertEquals("-1,000.12", formatter.format(-1000.116f));
        Assert.assertEquals("1,000", formatter.format(1000));
        Assert.assertEquals("-1,000", formatter.format((long) -1000));

        Assert.assertEquals("NaN", formatter.format(Double.NaN));
        Assert.assertEquals("NaN", formatter.format(Float.NaN));

        Assert.assertEquals("0", formatter.format(-0.0d));
        Assert.assertEquals("0", formatter.format(-0));
        Assert.assertEquals("0", formatter.format(-0.0001));
    }

    @Test
    public void formatNegativeZero()
    {
        CellFormatter formatter = new CellFormatter(0.0001, true);
        Assert.assertEquals("0", formatter.format(-0.0d));
        Assert.assertEquals("0", formatter.format(-0));
        Assert.assertEquals("0", formatter.format(-0.00001));
        Assert.assertEquals("-0.0001", formatter.format(-0.0001));

        Assert.assertFalse(CellFormatter.isNegativeZero("-01"));
        Assert.assertTrue(CellFormatter.isNegativeZero("-0.000000"));
    }

    @Test
    public void formatDate()
    {
        CellFormatter formatter = new CellFormatter(0, false);
        Assert.assertEquals("2009-02-13 23:31:30", formatter.format(Timestamp.valueOf("2009-02-13 23:31:30.0001")));
    }

    @Test
    public void formatString()
    {
        CellFormatter formatter = new CellFormatter(0, false);
        Assert.assertEquals("", formatter.format(""));
        Assert.assertEquals("foo", formatter.format("foo"));
        Assert.assertEquals("foo", formatter.format(" foo "));
        Assert.assertEquals("foo bar", formatter.format("foo bar"));
        Assert.assertEquals("foo  bar", formatter.format("foo  bar"));
        Assert.assertEquals("foo bar", formatter.format("foo\nbar "));
        Assert.assertEquals("foo bar", formatter.format(" foo\rbar"));
        Assert.assertEquals("foo   bar", formatter.format(" foo\r \nbar "));
        Assert.assertEquals("foo", formatter.format("foo\r"));
        Assert.assertEquals("foo", formatter.format("\n foo"));
        Assert.assertEquals("", formatter.format("\n \r "));
        Assert.assertEquals("", formatter.format(" \n \r"));
    }

}
