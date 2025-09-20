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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.gs.tablasco.adapters.TableAdapters;
import com.gs.tablasco.core.HtmlConfig;
import com.gs.tablasco.core.VerifierConfig;
import com.gs.tablasco.files.*;
import com.gs.tablasco.rebase.Rebaser;
import com.gs.tablasco.results.ExpectedResults;
import com.gs.tablasco.results.ExpectedResultsLoader;
import com.gs.tablasco.results.FileSystemExpectedResultsLoader;
import com.gs.tablasco.results.parser.ExpectedResultsCache;
import com.gs.tablasco.verify.*;
import java.io.File;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A JUnit plugin that can be included in JUnit 4 tests and used for verifying tabular data represented as
 * instances of {@link VerifiableTable}. {@link TableVerifier} can compare actual and expected tables provided by the
 * test or, more commonly, compares actual tables with expected results stored in the filesystem (the baseline). When
 * expected results are stored in the filesystem there are two modes of operation: rebase mode and the default verify
 * mode.
 * <p>
 * In rebase mode actual results provided by the test are saved in the configured expected results directory. No
 * verification is performed and the test always fails to ensure that rebase is not enabled accidentally. Rebase mode
 * can be enabled by setting the system property <i>rebase</i> to <i>true</i> or by calling <i>.withRebase()</i>.
 * <p>
 * In the default verify mode expected results are read from the filesystem and compared with the actual results
 * provided by the test. If the actual and expected tables match the test passes, otherwise the test fails. The
 * verification results are published as an HTML file in the configured output directory.
 * <p>
 * A number of configuration options are available to influence the behaviour of {@link TableVerifier}. A fluent
 * interface allows configuration options to be combined in a flexible manner.
 * <p>
 * {@link TableVerifier} can be subclasses but the only methods that can be overridden are hooks into the JUnit
 * lifecycle to aloow for custom setup and teardown code. Subclasses should provide the subclass type as generic
 * type {@link TableVerifier} to ensure that the fluent interface returns instances of the subclass rather than
 * {@link TableVerifier}.
 * <p>
 */
@SuppressWarnings("WeakerAccess")
public final class TableVerifier {
    private static final ExecutorService EXPECTED_RESULTS_LOADER_EXECUTOR =
            Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable);
                thread.setName("Expected Results Loader");
                thread.setDaemon(true);
                return thread;
            });

    private final VerifierConfig verifierConfig = new VerifierConfig();
    private final HtmlConfig htmlConfig = new HtmlConfig();

    private File fixedExpectedDir;
    private File fixedOutputDir;
    private boolean isRebasing = Rebaser.inRebaseMode();
    private Description description;
    private FilenameStrategy fileStrategy = new FilePerMethodStrategy();
    private DirectoryStrategy directoryStrategy = new FixedDirectoryStrategy();
    private boolean createActualResults = true;
    private boolean createActualResultsOnFailure = false;
    private String[] baselineHeaders = null;

    private final Metadata metadata = Metadata.newWithRecordedAt();
    private Map<String, VerifiableTable> expectedTables;
    private Metadata expectedMetadata;
    private final Set<String> tablesNotToAdapt = new HashSet<>();
    private Predicate<String> tableFilter = s -> true;
    private ExpectedResultsLoader expectedResultsLoader = new FileSystemExpectedResultsLoader();
    private Future<ExpectedResults> expectedResultsFuture;
    private int verifyCount = 0;
    private boolean summarisedResults = false;

    /**
     * Returns the same instance of {@link TableVerifier} configured with a fixed expected results directory.
     *
     * @param expectedDirPath path to the expected results directory
     * @return this
     */
    public TableVerifier withExpectedDir(String expectedDirPath) {
        return this.withExpectedDir(new File(expectedDirPath));
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a fixed expected results directory.
     *
     * @param expectedDir expected results directory
     * @return this
     */
    public TableVerifier withExpectedDir(File expectedDir) {
        this.fixedExpectedDir = expectedDir;
        return this.withDirectoryStrategy(new FixedDirectoryStrategy(expectedDir, this.fixedOutputDir));
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a fixed verification output directory.
     *
     * @param outputDirPath path to the verification output directory
     * @return this
     */
    public TableVerifier withOutputDir(String outputDirPath) {
        return this.withOutputDir(new File(outputDirPath));
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a fixed verification output directory.
     *
     * @param outputDir verification output directory
     * @return this
     */
    public TableVerifier withOutputDir(File outputDir) {
        this.fixedOutputDir = outputDir;
        return this.withDirectoryStrategy(new FixedDirectoryStrategy(this.fixedExpectedDir, outputDir));
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a Maven style directory strategy.
     *
     * @return this
     */
    public TableVerifier withMavenDirectoryStrategy() {
        return this.withDirectoryStrategy(new MavenStyleDirectoryStrategy());
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a Maven style directory strategy.
     *
     * @param expectedSubDir - the folder in src/main/resources where expected files are found
     * @param outputSubDir   - the folder in target where actual files are written
     * @return this
     */
    public TableVerifier withMavenDirectoryStrategy(String expectedSubDir, String outputSubDir) {
        final MavenStyleDirectoryStrategy directoryStrategy = new MavenStyleDirectoryStrategy()
                .withExpectedSubDir(expectedSubDir)
                .withOutputSubDir(outputSubDir);
        return this.withDirectoryStrategy(directoryStrategy);
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with rebase mode enabled.
     *
     * @return this
     */
    public TableVerifier withRebase() {
        this.isRebasing = true;
        return this;
    }

    /**
     * @return whether rebasing is enabled or not
     */
    public boolean isRebasing() {
        return this.isRebasing;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to use the same expected results and verification
     * output file for each test method.
     *
     * @return this
     */
    public TableVerifier withFilePerMethod() {
        return this.withFileStrategy(new FilePerMethodStrategy());
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to use a different expected results and
     * verification output file for each test method.
     *
     * @return this
     *
     * @deprecated Rebase does not work correctly with this strategy which will be removed eventually. Please use the
     * default FilePerMethod instead.
     */
    @Deprecated
    public TableVerifier withFilePerClass() {
        return this.withFileStrategy(new FilePerClassStrategy());
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a custom expected results and verification
     * output filename strategy
     *
     * @param filenameStrategy the filename stragety
     * @return this
     */
    public TableVerifier withFileStrategy(FilenameStrategy filenameStrategy) {
        this.fileStrategy = filenameStrategy;
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a custom expected results and verification
     * output directory strategy
     *
     * @param directoryStrategy the directory strategy
     * @return this
     */
    public TableVerifier withDirectoryStrategy(DirectoryStrategy directoryStrategy) {
        this.directoryStrategy = directoryStrategy;
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with row order verification disabled. If this is
     * disabled a test will pass if the cells match but row order is different between actual and expected results.
     *
     * @param verifyRowOrder whether to verify row order or not
     * @return this
     */
    public TableVerifier withVerifyRowOrder(boolean verifyRowOrder) {
        this.verifierConfig.withVerifyRowOrder(verifyRowOrder);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a baseline metadata name and value. This
     * metadata will be included in the baseline expected results file.
     *
     * @param name  metadata name
     * @param value metadata value
     * @return this
     */
    public TableVerifier withMetadata(String name, String value) {
        this.metadata.add(name, value);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a numeric tolerance to apply when matching
     * floating point numbers.
     * <p>
     * Note: this tolerance applies to all floating-point column types which could be dangerous. It is generally
     * advisable to set tolerance per column using {@link #withTolerance(String, double) withTolerance}
     *
     * @param tolerance the tolerance to apply
     * @return this
     */
    public TableVerifier withTolerance(double tolerance) {
        this.verifierConfig.withTolerance(tolerance);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a numeric tolerance to apply when matching
     * floating point numbers for the given column.
     *
     * @param columnName the column name for which the tolerance will be applied
     * @param tolerance the tolerance to apply
     * @return this
     */
    public TableVerifier withTolerance(String columnName, double tolerance) {
        this.verifierConfig.withTolerance(columnName, tolerance);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a variance threshold to apply when matching
     * numbers.
     * <p>
     * Note: this variance threshold applies to all floating-point column types which could be dangerous. It is
     * generally advisable to set variance threshold per column using {@link #withVarianceThreshold(String, double)
     * withVarianceThreshold}
     *
     * @param varianceThreshold the variance threshold to apply
     * @return this
     */
    public TableVerifier withVarianceThreshold(double varianceThreshold) {
        this.verifierConfig.withVarianceThreshold(varianceThreshold);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a variance threshold to apply when matching
     * numbers for the given column.
     *
     * @param columnName the column name for which the variance will be applied
     * @param varianceThreshold the variance threshold to apply
     * @return this
     */
    public TableVerifier withVarianceThreshold(String columnName, double varianceThreshold) {
        this.verifierConfig.withVarianceThreshold(columnName, varianceThreshold);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to exclude matched rows from the verification
     * output.
     *
     * @param hideMatchedRows whether to hide matched rows or not
     * @return this
     */
    public TableVerifier withHideMatchedRows(boolean hideMatchedRows) {
        this.htmlConfig.withHideMatchedRows(hideMatchedRows);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to always show matched rows for the specified
     * tables. This only makes sense when withHideMatchedRows is true.
     *
     * @param tableNames varargs of table names to always show matched rows for
     * @return this
     */
    public TableVerifier withAlwaysShowMatchedRowsFor(String... tableNames) {
        this.htmlConfig.withAlwaysShowMatchedRowsFor(tableNames);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to exclude matched columns from the verification
     * output.
     *
     * @param hideMatchedColumns whether to hide matched columns or not
     * @return this
     */
    public TableVerifier withHideMatchedColumns(boolean hideMatchedColumns) {
        this.htmlConfig.withHideMatchedColumns(hideMatchedColumns);
        return this;
    }

    /**
     *
     * /**
     * Returns the same instance of {@link TableVerifier} configured to use the original unmodified results for the specified
     * tables. This only makes sense when withActualAdapter or withExpectedAdapter is enabled and means that the adapter
     * is not applied to the specified tables.
     *
     * @param tableNames varargs of table names for which original unmodified results should be displayed
     * @return this
     */
    public TableVerifier withTablesNotToAdapt(String... tableNames) {
        this.tablesNotToAdapt.addAll(Arrays.asList(tableNames));
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to exclude matched tables from the verification
     * output. If this is enabled and all tables are matched not output file will be created.
     *
     * @param hideMatchedTables whether to hide matched tables or not
     * @return this
     */
    public TableVerifier withHideMatchedTables(boolean hideMatchedTables) {
        this.htmlConfig.withHideMatchedTables(hideMatchedTables);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to limit the number of HTML rows to the specified
     * number.
     *
     * @param htmlRowLimit the number of rows to limit output to
     * @return this
     */
    public TableVerifier withHtmlRowLimit(int htmlRowLimit) {
        this.htmlConfig.withHtmlRowLimit(htmlRowLimit);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to create a text file of the actual results in the
     * verification output directory. This can be useful for analysis and manual rebasing.
     *
     * @param createActualResults create actual results file
     * @return this
     */
    public TableVerifier withCreateActualResults(boolean createActualResults) {
        this.createActualResults = createActualResults;
        return this;
    }

    /**
     * Limits the creation of the actual results for only tests that have failed verification.
     *
     * @return this
     */
    public TableVerifier withCreateActualResultsOnFailure(boolean createActualResultsOnFailure) {
        this.createActualResultsOnFailure = createActualResultsOnFailure;
        return this;
    }

    /**
     * Adds an assertion summary to html output
     *
     * @return this
     */
    public TableVerifier withAssertionSummary(boolean assertionSummary) {
        this.htmlConfig.withAssertionSummary(assertionSummary);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a function for adapting actual results. Each
     * table in the actual results will be adapted using the specified function before being verified or rebased.
     *
     * @param actualAdapter function for adapting tables
     * @return this
     */
    public TableVerifier withActualAdapter(Function<VerifiableTable, VerifiableTable> actualAdapter) {
        this.verifierConfig.withActualAdapter(actualAdapter);
        return this;
    }

    /**
     * Returns the actual table adapter
     * @return - the actual table adapter
     */
    public Function<VerifiableTable, VerifiableTable> getActualAdapter() {
        return this.verifierConfig.getActualAdapter();
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a function for adapting expected results.
     * Each table in the expected results will be adapted using the specified function before being verified.
     *
     * @param expectedAdapter function for adapting tables
     * @return this
     */
    public TableVerifier withExpectedAdapter(Function<VerifiableTable, VerifiableTable> expectedAdapter) {
        this.verifierConfig.withExpectedAdapter(expectedAdapter);
        return this;
    }

    /**
     * Returns the expected table adapter
     * @return - the expected table adapter
     */
    public Function<VerifiableTable, VerifiableTable> getExpectedAdapter() {
        return this.verifierConfig.getExpectedAdapter();
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to ignore surplus rows from the verification.
     *
     * @return this
     */
    public TableVerifier withIgnoreSurplusRows() {
        this.verifierConfig.withIgnoreSurplusRows();
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to ignore missing rows from the verification.
     *
     * @return this
     */
    public TableVerifier withIgnoreMissingRows() {
        this.verifierConfig.withIgnoreMissingRows();
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to ignore surplus columns from the verification.
     *
     * @return this
     */
    public TableVerifier withIgnoreSurplusColumns() {
        this.verifierConfig.withIgnoreSurplusColumns();
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to ignore missing columns from the verification.
     *
     * @return this
     */
    public TableVerifier withIgnoreMissingColumns() {
        this.verifierConfig.withIgnoreMissingColumns();
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to ignore columns from both the actual and
     * expected tables.
     *
     * @param columnsToIgnore the columns to ignore
     * @return this
     */
    public TableVerifier withIgnoreColumns(String... columnsToIgnore) {
        final Set<String> columnSet = new HashSet<>(Arrays.asList(columnsToIgnore));
        return this.withColumnFilter(s -> !columnSet.contains(s));
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to filter columns from both the actual and
     * expected tables.
     *
     * @param columnFilter the column filter to apply
     * @return this
     */
    public TableVerifier withColumnFilter(final Predicate<String> columnFilter) {
        Function<VerifiableTable, VerifiableTable> adapter =
                verifiableTable -> TableAdapters.withColumns(verifiableTable, columnFilter);
        return this.withActualAdapter(adapter).withExpectedAdapter(adapter);
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to ignore tables from both the actual and
     * expected results.
     *
     * @param tableNames the names of tables to ignore
     * @return this
     */
    public TableVerifier withIgnoreTables(String... tableNames) {
        final Set<String> tableNameSet = new HashSet<>(Arrays.asList(tableNames));
        return this.withTableFilter(s -> !tableNameSet.contains(s));
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to filter tables from both the actual and
     * expected results.
     *
     * @param tableFilter the table filter to apply
     * @return this
     */
    public TableVerifier withTableFilter(Predicate<String> tableFilter) {
        this.tableFilter = tableFilter;
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to exclude SVN headers in expected results files
     *
     * @return this
     */
    public TableVerifier withBaselineHeaders(String... headers) {
        this.baselineHeaders = headers;
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with a custom {@link ExpectedResultsLoader}
     * instance
     *
     * @param expectedResultsLoader the {@link ExpectedResultsLoader} instance
     * @return this
     */
    public TableVerifier withExpectedResultsLoader(ExpectedResultsLoader expectedResultsLoader) {
        this.expectedResultsLoader = expectedResultsLoader;
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured to format HTML output grouped and summarised by
     * break type.
     *
     * @param summarisedResults whether to summarise results or not
     * @return this
     */
    public TableVerifier withSummarisedResults(boolean summarisedResults) {
        this.summarisedResults = summarisedResults;
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with the specified partial match timeout. A value
     * of zero or less results in no timeout.
     *
     * @param partialMatchTimeoutMillis verification timeout in milliseconds
     * @return this
     */
    public TableVerifier withPartialMatchTimeoutMillis(long partialMatchTimeoutMillis) {
        this.verifierConfig.withPartialMatchTimeoutMillis(partialMatchTimeoutMillis);
        return this;
    }

    /**
     * Returns the same instance of {@link TableVerifier} configured with no partial match timeout.
     *
     * @return this
     */
    public TableVerifier withoutPartialMatchTimeout() {
        this.verifierConfig.withoutPartialMatchTimeout();
        return this;
    }

    void starting(Description description) {
        this.description = description;
        if (!this.isRebasing) {
            this.expectedResultsFuture = EXPECTED_RESULTS_LOADER_EXECUTOR.submit(
                    () -> ExpectedResultsCache.getExpectedResults(expectedResultsLoader, getExpectedFile()));
        }
    }

    void succeeded(Description description) {
        if (this.expectedTables != null && !this.expectedTables.isEmpty()) {
            this.verifyTables(this.expectedTables, new HashMap<>(), this.expectedMetadata);
        }
        if (this.isRebasing) {
            fail("REBASE SUCCESSFUL - failing test in case rebase flag is set by mistake");
        }
    }

    public File getExpectedFile() {
        File dir = this.directoryStrategy.getExpectedDirectory(this.description.getTestClass());
        String filename = this.fileStrategy.getExpectedFilename(
                this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    public File getOutputFile() {
        File dir = this.directoryStrategy.getOutputDirectory(this.description.getTestClass());
        String filename =
                this.fileStrategy.getOutputFilename(this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    public File getActualFile() {
        File dir = this.directoryStrategy.getActualDirectory(this.description.getTestClass());
        String filename =
                this.fileStrategy.getActualFilename(this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    /**
     * Verifies a named actual table.
     *
     * @param tableName   the table name
     * @param actualTable the actual table
     */
    public void verify(String tableName, VerifiableTable actualTable) {
        this.verify(new NamedTable(tableName, actualTable));
    }

    /**
     * Verifies a list of actual named tables against expected baseline.
     *
     * @param actualTables - actual tables
     */
    public void verify(NamedTable... actualTables) {
        this.verify(Arrays.asList(actualTables));
    }

    /**
     * Verifies a list of actual named tables against expected baseline.
     *
     * @param actualTables - actual tables
     */
    public void verify(List<NamedTable> actualTables) {
        this.verify(toMap(actualTables));
    }

    /**
     * @deprecated replaced by {@link #verify(List)}}
     * Verifies a map of table names to actual tables.
     *
     * @param actualTables - actual tables by name
     */
    @Deprecated
    public void verify(Map<String, VerifiableTable> actualTables) {
        this.runPreVerifyChecks();
        this.makeSureDirectoriesAreNotSame();

        if (this.isRebasing) {
            this.newRebaser()
                    .rebase(
                            this.description.getMethodName(),
                            adaptAndFilterTables(actualTables, this.getActualAdapter()),
                            this.getExpectedFile());
        } else {
            if (this.expectedTables == null) {
                ExpectedResults expectedResults = getExpectedResults();
                this.expectedTables = new HashMap<>(
                        Objects.requireNonNull(expectedResults).getTables(this.description.getMethodName()));
                this.expectedMetadata = expectedResults.getMetadata();
            }
            Map<String, VerifiableTable> expectedTablesToVerify = new LinkedHashMap<>(actualTables.size());
            for (String actualTableName : actualTables.keySet()) {
                expectedTablesToVerify.put(actualTableName, this.expectedTables.remove(actualTableName));
            }
            this.verifyTables(expectedTablesToVerify, actualTables, this.expectedMetadata);
        }
    }

    public ExpectedResults getExpectedResults() {
        try {
            return this.isRebasing ? null : this.expectedResultsFuture.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Rebaser newRebaser() {
        return new Rebaser(this.verifierConfig, this.metadata, this.baselineHeaders);
    }

    /**
     * Verifies an expected table with an actual table.
     * @param name - table name
     * @param expectedTable - expected table
     * @param actualTable - actual table
     */
    public void verify(String name, VerifiableTable expectedTable, VerifiableTable actualTable) {
        this.verify(
                Collections.singletonList(new NamedTable(name, expectedTable)),
                Collections.singletonList(new NamedTable(name, actualTable)));
    }

    /**
     * Verifies a list of named expected tables with a list of named actual tables.
     * @param expectedTables - expected tables
     * @param actualTables - actual tables
     */
    public void verify(List<NamedTable> expectedTables, List<NamedTable> actualTables) {
        this.verify(toMap(expectedTables), toMap(actualTables));
    }

    private Map<String, VerifiableTable> toMap(List<NamedTable> namedTables) {
        Map<String, VerifiableTable> map = new LinkedHashMap<>();
        namedTables.forEach(namedTable -> map.put(namedTable.getName(), namedTable.getTable()));
        return map;
    }

    /**
     * @deprecated replaced by {@link #verify(List, List)}}
     * Verifies a map of table names to expected tables with a map of table names to actual tables.
     * @param expectedTables - expected tables by name
     * @param actualTables - actual tables by name
     */
    @Deprecated
    public void verify(Map<String, VerifiableTable> expectedTables, Map<String, VerifiableTable> actualTables) {
        this.runPreVerifyChecks();
        if (!this.isRebasing) {
            this.verifyTables(expectedTables, actualTables, null);
        }
    }

    private void verifyTables(
            Map<String, VerifiableTable> expectedTables, Map<String, VerifiableTable> actualTables, Metadata metadata) {
        Map<String, VerifiableTable> adaptedExpectedTables =
                adaptAndFilterTables(expectedTables, this.getExpectedAdapter());
        Map<String, VerifiableTable> adaptedActualTables = adaptAndFilterTables(actualTables, this.getActualAdapter());
        Map<String, FormattableTable> allResults = getVerifiedResults(adaptedExpectedTables, adaptedActualTables);
        boolean verificationSuccess = allResults.values().stream().allMatch(FormattableTable::isSuccess);
        boolean createActual = this.createActualResults;
        if (this.createActualResultsOnFailure) {
            createActual = !verificationSuccess;
        }
        if (createActual) {
            this.newRebaser().rebase(this.description.getMethodName(), adaptedActualTables, this.getActualFile());
        }
        HtmlFormatter htmlFormatter = newHtmlFormatter();
        htmlFormatter.appendResults(this.description.getMethodName(), allResults, metadata, ++this.verifyCount);
        assertTrue(verificationSuccess, "Some tests failed. See " + getOutputFileUrl() + " for more details.");
    }

    private URL getOutputFileUrl() {
        try {
            return this.getOutputFile().toURI().toURL();
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    public HtmlFormatter newHtmlFormatter() {
        return new HtmlFormatter(this.getOutputFile(), this.htmlConfig);
    }

    private Map<String, FormattableTable> getVerifiedResults(
            Map<String, VerifiableTable> adaptedExpectedTables, Map<String, VerifiableTable> adaptedActualTables) {
        MultiTableVerifier multiTableVerifier = new MultiTableVerifier(this.verifierConfig);
        Map<String, ResultTable> resultTables =
                multiTableVerifier.verifyTables(adaptedExpectedTables, adaptedActualTables);
        Map<String, FormattableTable> resultTableInterfaces = new LinkedHashMap<>(resultTables.size());
        for (Map.Entry<String, ResultTable> resultTableEntry : resultTables.entrySet()) {
            ResultTable resultTable = resultTableEntry.getValue();
            resultTableInterfaces.put(
                    resultTableEntry.getKey(),
                    this.summarisedResults ? new SummaryResultTable(resultTable) : resultTable);
        }
        return resultTableInterfaces;
    }

    private Map<String, VerifiableTable> adaptAndFilterTables(
            final Map<String, VerifiableTable> tables, final Function<VerifiableTable, VerifiableTable> adapter) {
        final Map<String, VerifiableTable> target = new LinkedHashMap<>(tables.size());
        tables.forEach((name, table) -> {
            if (tableFilter.test(name)) {
                if (TableVerifier.this.tablesNotToAdapt.contains(name)) {
                    target.put(name, table);
                } else {
                    target.put(name, adapter.apply(table));
                }
            }
        });
        return target;
    }

    private void runPreVerifyChecks() {
        if (this.description == null) {
            throw new IllegalStateException("The starting() has not been called. Ensure watcher has @Rule annotation.");
        }
    }

    private void makeSureDirectoriesAreNotSame() {
        File expectedDirectory = this.directoryStrategy.getExpectedDirectory(this.description.getTestClass());
        File outputDirectory = this.directoryStrategy.getOutputDirectory(this.description.getTestClass());
        if (expectedDirectory != null && expectedDirectory.equals(outputDirectory)) {
            throw new IllegalArgumentException(
                    "Expected results directory and verification output directory must NOT be the same.");
        }
    }
}
