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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

class CellFormatterTest {
    @Test
    void formatNumbers() {
        CellFormatter formatter = new CellFormatter(0.01, true);
        assertEquals("1", formatter.format(1.0d));
        assertEquals("1.1", formatter.format(1.10d));
        assertEquals("1.11", formatter.format(1.11d));
        assertEquals("1.11", formatter.format(1.111d));
        assertEquals("1.12", formatter.format(1.116d));
        assertEquals("-1.12", formatter.format(-1.116d));
        assertEquals("-1,000.12", formatter.format(-1000.116d));

        assertEquals("-1,000.12", formatter.format(-1000.116f));
        assertEquals("1,000", formatter.format(1000));
        assertEquals("-1,000", formatter.format((long) -1000));

        assertEquals("NaN", formatter.format(Double.NaN));
        assertEquals("NaN", formatter.format(Float.NaN));

        assertEquals("0", formatter.format(-0.0d));
        assertEquals("0", formatter.format(-0));
        assertEquals("0", formatter.format(-0.0001));
    }

    @Test
    void formatNegativeZero() {
        CellFormatter formatter = new CellFormatter(0.0001, true);
        assertEquals("0", formatter.format(-0.0d));
        assertEquals("0", formatter.format(-0));
        assertEquals("0", formatter.format(-0.00001));
        assertEquals("-0.0001", formatter.format(-0.0001));

        assertFalse(CellFormatter.isNegativeZero("-01"));
        assertTrue(CellFormatter.isNegativeZero("-0.000000"));
    }

    @Test
    void formatDate() {
        CellFormatter formatter = new CellFormatter(0, false);
        assertEquals("2009-02-13 23:31:30", formatter.format(Timestamp.valueOf("2009-02-13 23:31:30.0001")));
    }

    @Test
    void formatString() {
        CellFormatter formatter = new CellFormatter(0, false);
        assertEquals("", formatter.format(""));
        assertEquals("foo", formatter.format("foo"));
        assertEquals("foo", formatter.format(" foo "));
        assertEquals("foo bar", formatter.format("foo bar"));
        assertEquals("foo  bar", formatter.format("foo  bar"));
        assertEquals("foo bar", formatter.format("foo\nbar "));
        assertEquals("foo bar", formatter.format(" foo\rbar"));
        assertEquals("foo   bar", formatter.format(" foo\r \nbar "));
        assertEquals("foo", formatter.format("foo\r"));
        assertEquals("foo", formatter.format("\n foo"));
        assertEquals("", formatter.format("\n \r "));
        assertEquals("", formatter.format(" \n \r"));
    }
}
