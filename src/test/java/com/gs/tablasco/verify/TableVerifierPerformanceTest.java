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

import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.TableVerifier;
import com.gs.tablasco.VerifiableTable;
import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.databene.contiperf.junit.ContiPerfRule;
import org.eclipse.collections.impl.factory.Maps;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TableVerifierPerformanceTest
{
    @Rule
    public final ContiPerfRule contiPerf = new ContiPerfRule();

    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withOutputDir(TableTestUtils.getOutputDirectory());

    @Before
    public void assumePerformanceEnabled()
    {
        Assume.assumeTrue(Boolean.getBoolean("perftests.enabled"));
    }

    @Test
    @PerfTest(invocations = 10, rampUp = 1)
    @Required(max = 20000, average = 10000)
    public void allPass()
    {
        verify(new ExpectedTable());
    }

    @Test(expected = AssertionError.class)
    @PerfTest(invocations = 10, rampUp = 1)
    @Required(max = 20000, average = 10000)
    public void brokenBalances()
    {
        verify(new BrokenBalances());
    }

    @Test(expected = AssertionError.class)
    @PerfTest(invocations = 10, rampUp = 1)
    @Required(max = 20000, average = 10000)
    public void partialMatch()
    {
        verify(new PartiallyMatchable());
    }

    private void verify(VerifiableTable actualData)
    {
        this.tableVerifier.verify(Maps.fixedSize.of("table", new ExpectedTable()), Maps.fixedSize.of("table", actualData));;
    }

    private static class ExpectedTable implements VerifiableTable
    {
        @Override
        public int getRowCount()
        {
            return 10000;
        }

        @Override
        public int getColumnCount()
        {
            return 10;
        }

        @Override
        public String getColumnName(int columnIndex)
        {
            return "Heading " + columnIndex;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            switch (columnIndex)
            {
                case 0:
                    return "Value " + columnIndex + ',' + rowIndex;
                case 1:
                    return "Value " + columnIndex + ',' + rowIndex;
                case 2:
                    return "Value " + columnIndex + ',' + rowIndex;
                case 3:
                    return "Value " + columnIndex + ',' + rowIndex;
                case 4:
                    return "Value " + columnIndex + ',' + rowIndex;
                case 5:
                    return (double) (columnIndex * rowIndex);
                case 6:
                    return (double) (columnIndex * rowIndex);
                case 7:
                    return (double) (columnIndex * rowIndex);
                case 8:
                    return (double) (columnIndex * rowIndex);
                case 9:
                    return (double) (columnIndex * rowIndex);
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private static class BrokenBalances extends ExpectedTable
    {
        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            switch (columnIndex)
            {
                case 5:
                case 6:
                case 7:
                case 8:
                case 9:
                    if (rowIndex % columnIndex == 0)
                    {
                        return (double) (columnIndex * rowIndex * 2);
                    }
            }
            return super.getValueAt(rowIndex, columnIndex);
        }
    }

    private static class PartiallyMatchable extends ExpectedTable
    {
        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            if (columnIndex == 9)
            {
                if (rowIndex % 100 == 0)
                {
                    return (double) (columnIndex * rowIndex * 2);
                }
            }
            return super.getValueAt(rowIndex, columnIndex);
        }
    }
}
