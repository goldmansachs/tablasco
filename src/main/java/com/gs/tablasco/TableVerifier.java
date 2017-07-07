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

package com.gs.tablasco;

import com.gs.tablasco.adapters.TableAdapters;
import com.gs.tablasco.files.DirectoryStrategy;
import com.gs.tablasco.files.FilePerClassStrategy;
import com.gs.tablasco.files.FilePerMethodStrategy;
import com.gs.tablasco.files.FilenameStrategy;
import com.gs.tablasco.files.FixedDirectoryStrategy;
import com.gs.tablasco.files.MavenStyleDirectoryStrategy;
import com.gs.tablasco.investigation.Investigation;
import com.gs.tablasco.investigation.Sherlock;
import com.gs.tablasco.rebase.Rebaser;
import com.gs.tablasco.results.ExpectedResults;
import com.gs.tablasco.results.ExpectedResultsLoader;
import com.gs.tablasco.results.FileSystemExpectedResultsLoader;
import com.gs.tablasco.results.parser.ExpectedResultsCache;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.ExceptionHtml;
import com.gs.tablasco.verify.FormattableTable;
import com.gs.tablasco.verify.HtmlFormatter;
import com.gs.tablasco.verify.Metadata;
import com.gs.tablasco.verify.MultiTableVerifier;
import com.gs.tablasco.verify.ResultTable;
import com.gs.tablasco.verify.SummaryResultTable;
import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.block.predicate.Predicate2;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.collections.impl.list.fixed.ArrayAdapter;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.utility.MapIterate;
import org.junit.Assert;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * A JUnit <tt>Rule</tt> that can be included in JUnit 4 tests and used for verifying tabular data represented as
 * instances of <tt>VerifiableTable</tt>. <tt>TableVerifier</tt> can compare actual and expected tables provided by the
 * test or, more commonly, compares actual tables with expected results stored in the filesystem (the baseline). When
 * expected results are stored in the filesystem there are two modes of operation: rebase mode and the default verify
 * mode.<p>
 *
 * In rebase mode actual results provided by the test are saved in the configured expected results directory. No
 * verification is performed and the test always fails to ensure that rebase is not enabled accidentally. Rebase mode
 * can be enabled by setting the system property <ii>rebase</ii> to <ii>true</ii> or by calling
 * {@link .withRebase()}.<p>
 *
 * In the default verify mode expected results are read from the filesystem and compared with the actual results
 * provided by the test. If the actual and expected tables match the test passes, otherwise the test fails. The
 * verification results are published as an HTML file in the configured output directory.<p>
 *
 * A number of configuration options are available to influence the behaviour of <tt>TableVerifier</tt>. A fluent
 * interface allows configuration options to be combined in a flexible manner.<p>
 *
 * <tt>TableVerifier</tt> can be subclasses but the only methods that can be overridden are hooks into the JUnit
 * lifecycle to aloow for custom setup and teardown code. Subclasses should provide the subclass type as generic
 * type <tt>T</tt> to ensure that the fluent interface returns instances of the subclass rather than
 * <tt>TableVerfier</tt>.<p>
 *
 * @param <T> Subclass type provided by subclasses to ensure fluent interface returns instances of subclass rather than
 *           instances of <tt>TableVerifier</tt>
 */
public class TableVerifier<T extends TableVerifier> extends TestWatcher
{
    private static final ExecutorService EXPECTED_RESULTS_LOADER_EXECUTOR = Executors.newSingleThreadExecutor(new ThreadFactory()
    {
        @Override
        public Thread newThread(Runnable runnable)
        {
            Thread thread = new Thread(runnable);
            thread.setName("Expected Results Loader");
            thread.setDaemon(true);
            return thread;
        }
    });

    private File fixedExpectedDir;
    private File fixedOutputDir;
    private boolean isRebasing = Rebaser.inRebaseMode();
    private Description description;
    private FilenameStrategy fileStrategy = new FilePerMethodStrategy();
    private DirectoryStrategy directoryStrategy = new FixedDirectoryStrategy();
    private boolean verifyRowOrder = true;
    private boolean hideMatchedRows = false;
    private boolean hideMatchedTables = false;
    private boolean hideMatchedColumns = false;
    private boolean createActualResults = true;
    private boolean createActualResultsOnFailure = false;
    private boolean assertionSummary = false;
    private final ColumnComparators.Builder columnComparatorsBuilder = new ColumnComparators.Builder();
    private boolean ignoreSurplusRows = false;
    private boolean ignoreMissingRows = false;
    private boolean ignoreSurplusColumns = false;
    private boolean ignoreMissingColumns = false;
    private Function<VerifiableTable, VerifiableTable> actualAdapter = Functions.getPassThru();
    private Function<VerifiableTable, VerifiableTable> expectedAdapter = Functions.getPassThru();
    private String[] baselineHeaders = null;
    private long partialMatchTimeoutMillis = IndexMapTableVerifier.DEFAULT_PARTIAL_MATCH_TIMEOUT_MILLIS;

    private final Metadata metadata = Metadata.newWithRecordedAt();
    private Map<String, VerifiableTable> expectedTables;
    private Metadata expectedMetadata;
    private Set<String> tablesToAlwaysShowMatchedRowsFor = UnifiedSet.newSet();
    private Set<String> tablesNotToAdapt = UnifiedSet.newSet();
    private Predicate<String> tableFilter = new Predicate<String>()
    {
        @Override
        public boolean accept(String s)
        {
            return true;
        }
    };
    private ExpectedResultsLoader expectedResultsLoader = new FileSystemExpectedResultsLoader();
    private Future<ExpectedResults> expectedResultsFuture;
    private int verifyCount = 0;
    private int htmlRowLimit = HtmlFormatter.DEFAULT_ROW_LIMIT;
    private boolean summarisedResults = false;

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a fixed expected results directory.
     *
     * @param expectedDirPath path to the expected results directory
     * @return this
     */
    public final T withExpectedDir(String expectedDirPath)
    {
        return this.withExpectedDir(new File(expectedDirPath));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a fixed expected results directory.
     *
     * @param expectedDir expected results directory
     * @return this
     */
    public final T withExpectedDir(File expectedDir)
    {
        this.fixedExpectedDir = expectedDir;
        return this.withDirectoryStrategy(new FixedDirectoryStrategy(expectedDir, this.fixedOutputDir));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a fixed verification output directory.
     *
     * @param outputDirPath path to the verification output directory
     * @return this
     */
    public final T withOutputDir(String outputDirPath)
    {
        return this.withOutputDir(new File(outputDirPath));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a fixed verification output directory.
     *
     * @param outputDir verification output directory
     * @return this
     */
    public final T withOutputDir(File outputDir)
    {
        this.fixedOutputDir = outputDir;
        return this.withDirectoryStrategy(new FixedDirectoryStrategy(this.fixedExpectedDir, outputDir));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a Maven style directory strategy.
     *
     * @return this
     */
    public final T withMavenDirectoryStrategy()
    {
        return this.withDirectoryStrategy(new MavenStyleDirectoryStrategy());
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a Maven style directory strategy.
     *
     * @param expectedSubDir - the folder in src/main/resources where expected files are found
     * @param outputSubDir   - the folder in target where actual files are written
     * @return this
     */
    public final T withMavenDirectoryStrategy(String expectedSubDir, String outputSubDir)
    {
        final MavenStyleDirectoryStrategy directoryStrategy =
                new MavenStyleDirectoryStrategy()
                        .withExpectedSubDir(expectedSubDir)
                        .withOutputSubDir(outputSubDir);
        return this.withDirectoryStrategy(directoryStrategy);
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with rebase mode enabled.
     *
     * @return this
     */
    public final T withRebase()
    {
        this.isRebasing = true;
        return (T) this;
    }

    /**
     * @return whether rebasing is enabled or not
     */
    public final boolean isRebasing()
    {
        return this.isRebasing;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to use the same expected results and verification
     * output file for each test method.
     *
     * @return this
     */
    public final T withFilePerMethod()
    {
        return this.withFileStrategy(new FilePerMethodStrategy());
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to use a different expected results and
     * verification output file for each test method.
     *
     * @return this
     *
     * @deprecated Rebase does not work correctly with this strategy which will be removed eventually. Please use the
     * default FilePerMethod instead.
     */
    @Deprecated
    public final T withFilePerClass()
    {
        return this.withFileStrategy(new FilePerClassStrategy());
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a custom expected results and verification
     * output filename strategy
     *
     * @param filenameStrategy the filename stragety
     * @return this
     */
    public final T withFileStrategy(FilenameStrategy filenameStrategy)
    {
        this.fileStrategy = filenameStrategy;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a custom expected results and verification
     * output directory strategy
     *
     * @param directoryStrategy the directory strategy
     * @return this
     */
    public final T withDirectoryStrategy(DirectoryStrategy directoryStrategy)
    {
        this.directoryStrategy = directoryStrategy;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with row order verification disabled. If this is
     * disabled a test will pass if the cells match but row order is different between actual and expected results.
     *
     * @param verifyRowOrder whether to verify row order or not
     * @return this
     */
    public final T withVerifyRowOrder(boolean verifyRowOrder)
    {
        this.verifyRowOrder = verifyRowOrder;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a baseline metadata name and value. This
     * metadata will be included in the baseline expected results file.
     *
     * @param name  metadata name
     * @param value metadata value
     * @return this
     */
    public final T withMetadata(String name, String value)
    {
        this.metadata.add(name, value);
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a numeric tolerance to apply when matching
     * floating point numbers.
     *
     * @param tolerance the tolerance to apply
     * @return this
     */
    public final T withTolerance(double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(tolerance);
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a numeric tolerance to apply when matching
     * floating point numbers for the given column.
     *
     * @param columnName the column name for which the tolerance will be applied
     * @param tolerance the tolerance to apply
     * @return this
     */
    public final T withTolerance(String columnName, double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(columnName, tolerance);
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a variance threshold to apply when matching
     * numbers.
     *
     * @param varianceThreshold the variance threshold to apply
     * @return this
     */
    public final T withVarianceThreshold(double varianceThreshold)
    {
        this.columnComparatorsBuilder.withVarianceThreshold(varianceThreshold);
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a variance threshold to apply when matching
     * numbers for the given column.
     *
     * @param columnName the column name for which the variance will be applied
     * @param varianceThreshold the variance threshold to apply
     * @return this
     */
    public final T withVarianceThreshold(String columnName, double varianceThreshold)
    {
        this.columnComparatorsBuilder.withVarianceThreshold(columnName, varianceThreshold);
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to exclude matched rows from the verification
     * output.
     *
     * @param hideMatchedRows whether to hide matched rows or not
     * @return this
     */
    public final T withHideMatchedRows(boolean hideMatchedRows)
    {
        this.hideMatchedRows = hideMatchedRows;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to always show matched rows for the specified
     * tables. This only makes sense when withHideMatchedRows is true.
     *
     * @param tableNames varargs of table names to always show matched rows for
     * @return this
     */
    public final T withAlwaysShowMatchedRowsFor(String... tableNames)
    {
        this.tablesToAlwaysShowMatchedRowsFor.addAll(ArrayAdapter.adapt(tableNames));
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to exclude matched columns from the verification
     * output.
     *
     * @param hideMatchedColumns whether to hide matched columns or not
     * @return this
     */
    public T withHideMatchedColumns(boolean hideMatchedColumns)
    {
        this.hideMatchedColumns = hideMatchedColumns;
        return (T) this;
    }

    /**

     /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to use the original unmodified results for the specified
     * tables. This only makes sense when withActualAdapter or withExpectedAdapter is enabled and means that the adapter
     * is not applied to the specified tables.
     *
     * @param tableNames varargs of table names for which original unmodified results should be displayed
     * @return this
     */
    public final T withTablesNotToAdapt(String... tableNames)
    {
        this.tablesNotToAdapt.addAll(ArrayAdapter.adapt(tableNames));
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to exclude matched tables from the verification
     * output. If this is enabled and all tables are matched not output file will be created.
     *
     * @param hideMatchedTables whether to hide matched tables or not
     * @return this
     */
    public final T withHideMatchedTables(boolean hideMatchedTables)
    {
        this.hideMatchedTables = hideMatchedTables;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to limit the number of HTML rows to the specified
     * number.
     *
     * @param htmlRowLimit the number of rows to limit output to
     * @return this
     */
    public final T withHtmlRowLimit(int htmlRowLimit)
    {
        this.htmlRowLimit = htmlRowLimit;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to create a text file of the actual results in the
     * verification output directory. This can be useful for analysis and manual rebasing.
     *
     * @param createActualResults
     * @return this
     */
    public final T withCreateActualResults(boolean createActualResults)
    {
        this.createActualResults = createActualResults;
        return (T) this;
    }

    /**
     * Limits the creation of the actual results for only tests that have failed verification.
     *
     * @param createActualResultsOnFailure
     * @return this
     */
    public final T withCreateActualResultsOnFailure(boolean createActualResultsOnFailure)
    {
        this.createActualResultsOnFailure = createActualResultsOnFailure;
        return (T) this;
    }

    /**
     * Adds an assertion summary to html output
     *
     * @param assertionSummary
     * @return this
     */
    public final T withAssertionSummary(boolean assertionSummary)
    {
        this.assertionSummary = assertionSummary;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a function for adapting actual results. Each
     * table in the actual results will be adapted using the specified function before being verified or rebased.
     *
     * @param actualAdapter function for adapting tables
     * @return this
     */
    public final T withActualAdapter(Function<VerifiableTable, VerifiableTable> actualAdapter)
    {
        this.actualAdapter = actualAdapter;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a function for adapting expected results.
     * Each table in the expected results will be adapted using the specified function before being verified.
     *
     * @param expectedAdapter function for adapting tables
     * @return this
     */
    public final T withExpectedAdapter(Function<VerifiableTable, VerifiableTable> expectedAdapter)
    {
        this.expectedAdapter = expectedAdapter;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to ignore surplus rows from the verification.
     *
     * @return this
     */
    public final T withIgnoreSurplusRows()
    {
        this.ignoreSurplusRows = true;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to ignore missing rows from the verification.
     *
     * @return this
     */
    public final T withIgnoreMissingRows()
    {
        this.ignoreMissingRows = true;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to ignore surplus columns from the verification.
     *
     * @return this
     */
    public T withIgnoreSurplusColumns()
    {
        this.ignoreSurplusColumns = true;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to ignore missing columns from the verification.
     *
     * @return this
     */
    public T withIgnoreMissingColumns()
    {
        this.ignoreMissingColumns = true;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to ignore columns from both the actual and
     * expected tables.
     *
     * @param columnsToIgnore the columns to ignore
     * @return this
     */
    public T withIgnoreColumns(String... columnsToIgnore)
    {
        final Set<String> columnSet = Sets.immutable.of(columnsToIgnore).castToSet();
        return this.withColumnFilter(new Predicate<String>()
        {
            @Override
            public boolean accept(String s)
            {
                return !columnSet.contains(s);
            }
        });
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to filter columns from both the actual and
     * expected tables.
     *
     * @param columnFilter the column filter to apply
     * @return this
     */
    public T withColumnFilter(final Predicate<String> columnFilter)
    {
        Function<VerifiableTable, VerifiableTable> adapter = new Function<VerifiableTable, VerifiableTable>()
        {
            @Override
            public VerifiableTable valueOf(VerifiableTable table)
            {
                return TableAdapters.withColumns(table, columnFilter);
            }
        };
        return (T) this.withActualAdapter(adapter).withExpectedAdapter(adapter);
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to ignore tables from both the actual and
     * expected results.
     *
     * @param tableNames the names of tables to ignore
     * @return this
     */
    public T withIgnoreTables(String... tableNames)
    {
        final Set<String> tableNameSet = UnifiedSet.newSetWith(tableNames);
        return (T) this.withTableFilter(new Predicate<String>()
        {
            @Override
            public boolean accept(String s)
            {
                return !tableNameSet.contains(s);
            }
        });
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to filter tables from both the actual and
     * expected results.
     *
     * @param tableFilter the table filter to apply
     * @return this
     */
    public T withTableFilter(Predicate<String> tableFilter)
    {
        this.tableFilter = tableFilter;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to exclude SVN headers in expected results files
     *
     * @return this
     */
    public final T withBaselineHeaders(String... headers)
    {
        this.baselineHeaders = headers;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a custom <tt>ExpectedResultsLoader</tt>
     * instance
     *
     * @param expectedResultsLoader the <tt>ExpectedResultsLoader</tt> instance
     * @return this
     */
    public final T withExpectedResultsLoader(ExpectedResultsLoader expectedResultsLoader)
    {
        this.expectedResultsLoader = expectedResultsLoader;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to format HTML output grouped and summarised by
     * break type.
     *
     * @param summarisedResults whether to summarise results or not
     * @return this
     */
    public final T withSummarisedResults(boolean summarisedResults)
    {
        this.summarisedResults = summarisedResults;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with the specified partial match timeout. A value
     * of zero or less results in no timeout.
     *
     * @param partialMatchTimeoutMillis verification timeout in milliseconds
     * @return this
     */
    public final T withPartialMatchTimeoutMillis(long partialMatchTimeoutMillis)
    {
        this.partialMatchTimeoutMillis = partialMatchTimeoutMillis;
        return (T) this;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with no partial match timeout.
     *
     * @return this
     */
    public final T withoutPartialMatchTimeout()
    {
        return this.withPartialMatchTimeoutMillis(0);
    }

    @Override
    public final void starting(Description description)
    {
        this.description = description;
        if (!this.isRebasing)
        {
            this.expectedResultsFuture = EXPECTED_RESULTS_LOADER_EXECUTOR.submit(new Callable<ExpectedResults>()
            {
                @Override
                public ExpectedResults call() throws Exception
                {
                    return ExpectedResultsCache.getExpectedResults(expectedResultsLoader, getExpectedFile());
                }
            });
        }
        this.onStarted(description);
    }

    /**
     * Lifecycle hook called after JUnit has called <tt>starting</tt>
     *
     * @param description test description
     */
    protected void onStarted(Description description)
    {

    }

    @Override
    public final void succeeded(Description description)
    {
        try
        {
            if (MapIterate.notEmpty(this.expectedTables))
            {
                this.verifyTables(this.expectedTables, Maps.fixedSize.<String, VerifiableTable>of(), this.expectedMetadata, this.columnComparatorsBuilder.build());
            }
        }
        catch (AssertionError assertionError)
        {
            this.failed(assertionError, description);
            throw assertionError;
        }
        this.onSucceeded(description);
        if (this.isRebasing)
        {
            Assert.fail("REBASE SUCCESSFUL - failing test in case rebase flag is set by mistake");
        }
    }

    /**
     * Lifecycle hook called if and after JUnit has called <tt>succeeded</tt>
     *
     * @param description test description
     */
    protected void onSucceeded(Description description)
    {

    }

    @Override
    public final void failed(Throwable e, Description description)
    {
        this.onFailed(e, description);
    }

    /**
     * Lifecycle hook called if and after JUnit has called <tt>failed</tt>
     *
     * @param description test description
     */
    protected void onFailed(Throwable e, Description description)
    {
        // moved to onFailed so that it can be overridden in GLEW and resolve bad dependency
        // this exception handling will be removed in TVR2
        if (!AssertionError.class.isInstance(e))
        {
            ExceptionHtml.create(this.getOutputFile(), e);
        }
    }

    @Override
    public final void skipped(AssumptionViolatedException e, Description description)
    {
        this.onSkipped(description);
    }

    /**
     * Lifecycle hook called if and after JUnit has called <tt>skipped</tt>
     *
     * @param description test description
     */
    protected void onSkipped(Description description)
    {

    }

    @Override
    public final void finished(Description description)
    {
        this.onFinished(description);
    }

    /**
     * Lifecycle hook called after JUnit has called <tt>finished</tt>
     *
     * @param description test description
     */
    protected void onFinished(Description description)
    {

    }

    public final File getExpectedFile()
    {
        File dir = this.directoryStrategy.getExpectedDirectory(this.description.getTestClass());
        String filename = this.fileStrategy.getExpectedFilename(this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    public final File getOutputFile()
    {
        File dir = this.directoryStrategy.getOutputDirectory(this.description.getTestClass());
        String filename = this.fileStrategy.getOutputFilename(this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    final File getActualFile()
    {
        File dir = this.directoryStrategy.getActualDirectory(this.description.getTestClass());
        String filename = this.fileStrategy.getActualFilename(this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    /**
     * Verifies a named actual table.
     *
     * @param tableName   the table name
     * @param actualTable the actual table
     */
    public final void verify(String tableName, VerifiableTable actualTable)
    {
        this.verify(Maps.fixedSize.of(tableName, actualTable));
    }

    /**
     * Verifies a map of table names to actual tables.
     *
     * @param actualTables
     */
    public final void verify(Map<String, VerifiableTable> actualTables)
    {
        this.runPreVerifyChecks();
        this.makeSureDirectoriesAreNotSame();

        ColumnComparators columnComparators = this.columnComparatorsBuilder.build();
        if (this.isRebasing)
        {
            this.rebaser(columnComparators).rebase(this.description.getMethodName(), adaptAndFilterTables(actualTables, this.actualAdapter), this.getExpectedFile());
        }
        else
        {
            if (this.expectedTables == null)
            {
                ExpectedResults expectedResults = getExpectedResults();
                this.expectedTables = UnifiedMap.newMap(expectedResults.getTables(this.description.getMethodName()));
                this.expectedMetadata = expectedResults.getMetadata();
            }
            Map<String, VerifiableTable> expectedTablesToVerify = new LinkedHashMap<>(actualTables.size());
            for (String actualTableName : actualTables.keySet())
            {
                expectedTablesToVerify.put(actualTableName, this.expectedTables.remove(actualTableName));
            }
            this.verifyTables(expectedTablesToVerify, actualTables, this.expectedMetadata, columnComparators);
        }
    }

    public ExpectedResults getExpectedResults()
    {
        try
        {
            return this.isRebasing ? null : this.expectedResultsFuture.get();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private Rebaser rebaser(ColumnComparators columnComparators)
    {
        return new Rebaser(columnComparators, this.metadata, this.baselineHeaders);
    }

    /**
     * Verifies a map of table names to expected tables with a map of table names to actual tables.
     * @param expectedTables
     * @param actualTables
     */
    public final void verify(Map<String, VerifiableTable> expectedTables, Map<String, VerifiableTable> actualTables)
    {
        this.runPreVerifyChecks();
        if (!this.isRebasing)
        {
            this.verifyTables(expectedTables, actualTables, null, this.columnComparatorsBuilder.build());
        }
    }

    private void verifyTables(Map<String, VerifiableTable> expectedTables, Map<String, VerifiableTable> actualTables, Metadata metadata, ColumnComparators columnComparators)
    {
        Map<String, VerifiableTable> adaptedExpectedTables = adaptAndFilterTables(expectedTables, this.expectedAdapter);
        Map<String, VerifiableTable> adaptedActualTables = adaptAndFilterTables(actualTables, this.actualAdapter);
        Map<String, FormattableTable> allResults = getVerifiedResults(columnComparators, adaptedExpectedTables, adaptedActualTables);
        MutableMap<String, FormattableTable> failedTables = MapIterate.rejectMapOnEntry(allResults, new Predicate2<String, FormattableTable>()
        {
            @Override
            public boolean accept(String key, FormattableTable verifiedTable)
            {
                return verifiedTable.isSuccess();
            }
        });
        boolean createActual = this.createActualResults;
        if (this.createActualResultsOnFailure)
        {
            createActual = !failedTables.isEmpty();
        }
        if (createActual)
        {
            this.rebaser(columnComparators).rebase(this.description.getMethodName(), adaptedActualTables, this.getActualFile());
        }
        Map<String, FormattableTable> tablesToFormat = this.hideMatchedTables ? failedTables : allResults;
        if (!tablesToFormat.isEmpty())
        {
            Set<String> tablesToHideMatchedRowsFor = this.hideMatchedRows ? UnifiedSet.newSet(tablesToFormat.keySet()) : UnifiedSet.<String>newSet();
            tablesToHideMatchedRowsFor.removeAll(this.tablesToAlwaysShowMatchedRowsFor);
            HtmlFormatter htmlFormatter = new HtmlFormatter(this.getOutputFile(), tablesToHideMatchedRowsFor, this.assertionSummary, this.hideMatchedColumns, this.htmlRowLimit);
            htmlFormatter.appendResults(this.description.getMethodName(), tablesToFormat, metadata, ++this.verifyCount);
        }
        failUnless(failedTables.isEmpty());
    }

    private Map<String, FormattableTable> getVerifiedResults(ColumnComparators columnComparators, Map<String, VerifiableTable> adaptedExpectedTables, Map<String, VerifiableTable> adaptedActualTables)
    {
        MultiTableVerifier multiTableVerifier = new MultiTableVerifier(columnComparators, newSingleSingleTableVerifier());
        Map<String, ResultTable> resultTables = multiTableVerifier.verifyTables(adaptedExpectedTables, adaptedActualTables);
        Map<String, FormattableTable> resultTableInterfaces = new LinkedHashMap<>(resultTables.size());
        for (Map.Entry<String, ResultTable> resultTableEntry : resultTables.entrySet())
        {
            ResultTable resultTable = resultTableEntry.getValue();
            resultTableInterfaces.put(resultTableEntry.getKey(), this.summarisedResults ? new SummaryResultTable(resultTable) : resultTable);
        }
        return resultTableInterfaces;
    }

    private void failUnless(boolean condition)
    {
        Assert.assertTrue("Some tests failed. Check test results file " + this.getOutputFile().getAbsolutePath() + " for more details.", condition);
    }

    private IndexMapTableVerifier newSingleSingleTableVerifier()
    {
        return new IndexMapTableVerifier(this.verifyRowOrder, IndexMapTableVerifier.DEFAULT_BEST_MATCH_THRESHOLD, this.ignoreSurplusRows, this.ignoreMissingRows, this.ignoreSurplusColumns, this.ignoreMissingColumns, this.partialMatchTimeoutMillis);
    }

    private Map<String, VerifiableTable> adaptAndFilterTables(final Map<String, VerifiableTable> tables, final Function<VerifiableTable, VerifiableTable> adapter)
    {
        final Map<String, VerifiableTable> target = new LinkedHashMap<>(tables.size());
        MapIterate.forEachKeyValue(tables, new Procedure2<String, VerifiableTable>()
        {
            @Override
            public void value(String name, VerifiableTable table)
            {
                if (tableFilter.accept(name))
                {
                    if (TableVerifier.this.tablesNotToAdapt.contains(name))
                    {
                        target.put(name, table);
                    }
                    else
                    {
                        target.put(name, adapter.valueOf(table));
                    }
                }
            }
        });
        return target;
    }

    private void runPreVerifyChecks()
    {
        if (this.description == null)
        {
            throw new IllegalStateException("The starting() has not been called. Ensure watcher has @Rule annotation.");
        }
    }

    private void makeSureDirectoriesAreNotSame()
    {
        File expectedDirectory = this.directoryStrategy.getExpectedDirectory(this.description.getTestClass());
        File outputDirectory = this.directoryStrategy.getOutputDirectory(this.description.getTestClass());
        if (expectedDirectory != null && expectedDirectory.equals(outputDirectory))
        {
            throw new IllegalArgumentException("Expected results directory and verification output directory must NOT be the same.");
        }
    }

    public void investigate(Investigation investigation)
    {
        investigate(investigation, this.getOutputFile());
    }

    public void investigate(Investigation investigation, File outputFile)
    {
        new Sherlock().handle(investigation, outputFile);
    }
}