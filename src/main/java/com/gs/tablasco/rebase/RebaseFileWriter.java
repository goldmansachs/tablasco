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

package com.gs.tablasco.rebase;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.CellComparator;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.Metadata;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.utility.ArrayIterate;
import org.eclipse.collections.impl.utility.StringIterate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RebaseFileWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RebaseFileWriter.class);

    // we create files first time for each test run, then append...
    private static final Set<String> SEEN_FILES = UnifiedSet.newSet();
    private static final Pattern DOUBLE_QUOTE_PATTERN = Pattern.compile("\"");
    private static final Pattern BACKSLASH_PATTERN = Pattern.compile("\\\\");
    private static final String DOUBLE_QUOTE_REPLACEMENT = Matcher.quoteReplacement("\\\"");
    private static final String BACKSLASH_REPLACEMENT = Matcher.quoteReplacement("\\\\");
    private static final Set<String> SPECIAL_NUMBERS = Sets.fixedSize.of("" + '\u221E', "-" + '\u221E', "NaN");
    private final Metadata metadata;
    private final String[] baselineHeaders;
    private final ColumnComparators columnComparators;
    private final File outputFile;

    public RebaseFileWriter(Metadata metadata, String[] baselineHeaders, ColumnComparators columnComparators, File outputFile)
    {
        this.metadata = metadata;
        this.baselineHeaders = baselineHeaders;
        this.columnComparators = columnComparators;
        this.outputFile = outputFile;
    }

    public void writeRebasedResults(String methodName, Map<String, VerifiableTable> actualResults)
    {
        deleteExpectedResults();
        boolean needsHeaderAndMetadata = !this.outputFile.exists();
        File parentDir = this.outputFile.getParentFile();
        if (!parentDir.exists() && !parentDir.mkdirs())
        {
            throw new IllegalStateException("Unable to create results directory:" + parentDir);
        }
        try (PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.outputFile, true), "UTF-8"))))
        {
            if (needsHeaderAndMetadata)
            {
                printHeaderAndMetadata(printWriter);
            }
            for (Map.Entry<String, VerifiableTable> namedTabled : actualResults.entrySet())
            {
                printTable(printWriter, methodName, namedTabled.getKey(), namedTabled.getValue());
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void printSeparatorIfNecessary(VerifiableTable table, PrintWriter printWriter, int column)
    {
        if (column < table.getColumnCount() - 1)
        {
            printWriter.print(",");
        }
    }

    private static void printString(PrintWriter printWriter, String str)
    {
        printWriter.print("\"");
        String backSlashesEscaped = str.indexOf('\\') < 0 ? str : BACKSLASH_PATTERN.matcher(str).replaceAll(BACKSLASH_REPLACEMENT);
        String backSlashesAndQuotesEscaped = backSlashesEscaped.indexOf('"') < 0 ? backSlashesEscaped : DOUBLE_QUOTE_PATTERN.matcher(backSlashesEscaped).replaceAll(DOUBLE_QUOTE_REPLACEMENT);
        printWriter.print(backSlashesAndQuotesEscaped);
        printWriter.print("\"");
    }

    private static void deleteFile(File file)
    {
        if (file.exists() && !file.delete())
        {
            throw new RuntimeException("Cannot delete output file " + file.getName());
        }
    }

    private void deleteExpectedResults()
    {
        if (SEEN_FILES.add(this.outputFile.getAbsolutePath()))
        {
            deleteFile(this.outputFile);
        }
    }

    private void printTable(PrintWriter printWriter, String methodName, String tableName, VerifiableTable verifiableTable)
    {
        LOGGER.info("Writing results for '" + methodName + ' ' + tableName + "' to '" + this.outputFile + '\'');
        printWriter.print("Section ");
        printWriter.print('"');
        printWriter.print(methodName);
        printWriter.print('"');
        if (StringIterate.notEmpty(tableName))
        {
            printWriter.print(' ');
            printString(printWriter, tableName);
        }
        printWriter.println();
        printTableContents(printWriter, verifiableTable);
        printWriter.println();
    }

    private void printTableContents(PrintWriter printWriter, VerifiableTable table)
    {
        for (int column = 0; column < table.getColumnCount(); column++)
        {
            String name = table.getColumnName(column);
            printString(printWriter, name);
            printSeparatorIfNecessary(table, printWriter, column);
        }
        printWriter.println();

        for (int row = 0; row < table.getRowCount(); row++)
        {
            for (int column = 0; column < table.getColumnCount(); column++)
            {
                CellComparator comparator = this.columnComparators.getComparatorForRebase(table.getColumnName(column));
                Object cell = table.getValueAt(row, column);
                String formattedCell = comparator.getFormatter().format(cell);
                if (cell instanceof Number && !SPECIAL_NUMBERS.contains(formattedCell))
                {
                    printWriter.print(formattedCell);
                }
                else
                {
                    printString(printWriter, formattedCell);
                }
                printSeparatorIfNecessary(table, printWriter, column);
            }
            printWriter.println();
        }
    }

    private void printHeaderAndMetadata(PrintWriter printWriter)
    {
        if (ArrayIterate.notEmpty(this.baselineHeaders))
        {
            printWriter.println("/*");
            for (String baselineHeader : this.baselineHeaders)
            {
                printWriter.print(" * ");
                printWriter.println(baselineHeader);
            }
            printWriter.println(" */");
            printWriter.println();
        }
        printWriter.print("Metadata");
        printWriter.print(' ');
        printWriter.println(this.metadata.toString("\""));
        printWriter.println();
    }
}
