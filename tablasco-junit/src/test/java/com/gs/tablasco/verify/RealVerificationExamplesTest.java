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
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.rebase.RebaseFileWriter;
import com.gs.tablasco.results.ExpectedResults;
import com.gs.tablasco.results.FileSystemExpectedResultsLoader;
import com.gs.tablasco.results.parser.ExpectedResultsParser;
import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.*;

public class RealVerificationExamplesTest
{
    // can be set to true to obfuscate data before checkin (needs to be reverted back to false or tests will fail)
    private static final boolean OBFUSCATE = false;

    @Test
    public void BsGlReconciliationRegressionTest_usGaapTestsGscmInventoryRecTrue()
    {
        verify("BsGlReconciliationRegressionTest", "usGaapTestsGscmInventoryRecTrue", "GL Reconciliation Results", 2329); // was 3708
    }

    @Test
    public void BswCpsAnalysisRegressionTest_regrBalanceBusinessType()
    {
        verify("BswCpsAnalysisRegressionTest", "regrBalanceBusinessType", "results", 483); // was 491
    }

    @Test
    public void JournalCreationRegressionTest_usGaapPnl()
    {
        verify("JournalCreationRegressionTest", "usGaapPnl", "journals", 89789); // was 113840 and 90090
    }

    @Test
    public void RepoNetdownAllocationBswLiteTest_groupByDeskheadAndIncomeFunction()
    {
        verify("RepoNetdownAllocationBswLiteTest", "groupByDeskheadAndIncomeFunction", "Side-by-side", 36); // was 38
    }

    @Test
    public void UserQueryTestGsBankRegressionTest_gsbLoansCr()
    {
        verify("UserQueryTestGsBankRegressionTest", "gsbLoansCr", "Side-by-side", 8);
    }

    private static void verify(String className, String methodName, String tableName, int expectedBrokenCells)
    {
        File examplesDir = new File(TableTestUtils.getExpectedDirectory(), "examples");
        File actual = new File(examplesDir, className + '_' + methodName + "_ACTUAL.txt");
        File expected = new File(examplesDir, actual.getName().replace("_ACTUAL.txt", "_EXPECTED.txt"));
        ExpectedResults actualResults = new ExpectedResultsParser(new FileSystemExpectedResultsLoader(), actual).parse();
        ExpectedResults expectedResults = new ExpectedResultsParser(new FileSystemExpectedResultsLoader(), expected).parse();
        Map<String, VerifiableTable> actualTables = actualResults.getTables(methodName);
        Map<String, VerifiableTable> expectedTables = expectedResults.getTables(methodName);
        ColumnComparators columnComparators = new ColumnComparators.Builder().withTolerance(1.0d).build();
        if (OBFUSCATE)
        {
            Obfuscator obfuscator = new Obfuscator();
            adaptForObfuscation(actualTables, obfuscator);
            adaptForObfuscation(expectedTables, obfuscator);
            new RebaseFileWriter(actualResults.getMetadata(), new String[0], columnComparators, actual).writeRebasedResults(methodName, actualTables);
            new RebaseFileWriter(expectedResults.getMetadata(), new String[0], columnComparators, expected).writeRebasedResults(methodName, expectedTables);
        }
        Assert.assertFalse("Obfuscation is only to help prepare results for checkin", OBFUSCATE);
        Assert.assertTrue(!actualTables.isEmpty());
        Assert.assertEquals(actualTables.keySet(), expectedTables.keySet());
        ResultTable verify = new IndexMapTableVerifier(columnComparators, false, IndexMapTableVerifier.DEFAULT_BEST_MATCH_THRESHOLD, false, false).verify(actualTables.get(tableName), expectedTables.get(tableName));
        HtmlFormatter htmlFormatter = new HtmlFormatter(new File(TableTestUtils.getOutputDirectory(), RealVerificationExamplesTest.class.getSimpleName() + '_' + className + '_' + methodName + ".html"), new HtmlOptions(false, HtmlFormatter.DEFAULT_ROW_LIMIT, false, false, false, Collections.emptySet()));
        htmlFormatter.appendResults(methodName, Collections.singletonMap(tableName, new SummaryResultTable(verify)), Metadata.newEmpty());
        int failedCells = verify.getTotalCellCount() - verify.getPassedCellCount();
        Assert.assertEquals(expectedBrokenCells, failedCells);
    }

    private static void adaptForObfuscation(Map<String, VerifiableTable> tables, final Obfuscator obfuscator)
    {
        for (String key : tables.keySet().toArray(new String[0]))
        {
            tables.put(key, new ObfuscatingTableAdapter(tables.get(key), obfuscator));
        }
    }

    private static class ObfuscatingTableAdapter extends DefaultVerifiableTableAdapter
    {
        private final Obfuscator obfuscator;

        ObfuscatingTableAdapter(VerifiableTable delegate, Obfuscator obfuscator)
        {
            super(delegate);
            this.obfuscator = obfuscator;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            Object valueAt = super.getValueAt(rowIndex, columnIndex);
            return valueAt instanceof String ? this.obfuscator.obfuscate((String) valueAt) : valueAt;
        }
    }

    private static class Obfuscator
    {
        private static final char[] consonants = { 'q', 'w', 'r', 't', 'y', 'p', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm' };
        private static final char[] vowels = { 'a', 'e', 'i', 'o', 'u' };

        private static final Random rand = new Random(12345987345909L);

        private HashSet<String> immutables = new HashSet<>();
        private HashMap<String, String> replacements = new HashMap<>();

        private String makeWord(int length)
        {
            StringBuilder builder = new StringBuilder(length);
            for(int i=0;i<length;i++)
            {
                builder.append( ( i & 1) == 0 ? consonants[rand.nextInt(consonants.length)] : vowels[rand.nextInt(vowels.length)]);
            }
            return builder.toString();
        }

        String obfuscate(String toReplace)
        {
            if (immutables.contains(toReplace))
            {
                return toReplace;
            }
            String lower = toReplace.toLowerCase();
            String existing = replacements.get(lower);
            if (existing == null)
            {
                existing = makeWord(lower.length());
                replacements.put(lower, existing);
            }
            return makeSameCapCase(toReplace, existing);
        }

        private String makeSameCapCase(String toReplace, String existing)
        {
            StringBuilder builder = new StringBuilder(toReplace.length());
            for(int i=0;i<toReplace.length();i++)
            {
                builder.append(Character.isUpperCase(toReplace.charAt(i)) ? Character.toUpperCase(existing.charAt(i)) : existing.charAt(i));
            }
            return builder.toString();
        }

    }
}