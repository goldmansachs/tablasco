package com.gs.tablasco.spark;

import com.gs.tablasco.verify.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Spark calculation that compares two HDFS datasets and produces a detailed yet compact HTML break report. The
 * comparison is distributed by grouping rows based on a set of group keys provided by the caller and comparing the
 * groups independently. The group key(s) do not have to be unique primary keys (although a unique primary key is
 * ideal) but they should be sufficient to distribute the data fairly evenly across many buckets. For example given a
 * dataset containing all people in the USA 'town' would not be an appropriate group key (New York City vs a small
 * village in upstate NY) but a combination of 'surname' and 'zip code' would probably be fine.
 *
 * The result of the calculation contains a self-contained (CSS, etc) HTML result; It is up to the caller to publish
 * this somewhere to be reviewed.
 */
@SuppressWarnings("WeakerAccess")
public class SparkVerifier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkVerifier.class);
    private final List<String> groupKeyColumns;
    private final boolean groupKeyIsPrimaryKey;
    private final Metadata metadata = Metadata.newEmpty();
    private final ColumnComparators.Builder columnComparatorsBuilder = new ColumnComparators.Builder();
    private boolean ignoreSurplusColumns;
    private Set<String> columnsToIgnore;
    private int maxGroupSize = 10_000;

    public static SparkVerifier newWithGroupKeyColumns(List<String> groupKeyColumns, int maxGroupSize) {
        return new SparkVerifier(groupKeyColumns, false).withMaxGroupSize(maxGroupSize);
    }

    public static SparkVerifier newWithPrimaryKeyColumns(List<String> primaryKeyColumns) {
        return new SparkVerifier(primaryKeyColumns, true);
    }

    private SparkVerifier(List<String> groupKeyColumns, boolean groupKeyIsPrimaryKey)
    {
        this.groupKeyColumns = groupKeyColumns;
        this.groupKeyIsPrimaryKey = groupKeyIsPrimaryKey;
    }

    /**
     * Creates a new SparkVerifier
     * @param groupKeyColumns a list of group keys to distribute the data
     * @deprecated use {@link #newWithGroupKeyColumns(java.util.List, int)}
     */
    @Deprecated
    public SparkVerifier(List<String> groupKeyColumns)
    {
        this(groupKeyColumns, false);
    }

    /**
     * A hint as to the maximum size of groups to be compared together. The default size is 10,000.
     * @param maxGroupSize the maximum group size
     * @return this same SparkVerifier
     * @deprecated use {@link #newWithGroupKeyColumns(java.util.List, int)}
     */
    @Deprecated
    public SparkVerifier withMaxGroupSize(int maxGroupSize)
    {
        this.maxGroupSize = maxGroupSize;
        return this;
    }

    /**
     * Adds metadata to include in the HTML report
     * @param name metadata name
     * @param value metadata value
     * @return this same SparkVerifier
     */
    public final SparkVerifier withMetadata(String name, String value)
    {
        this.metadata.add(name, value);
        return this;
    }

    /**
     * Controls whether verification fails in the presence of surplus columns in the actual data
     * @param ignoreSurplusColumns whether to ignore surplus columns or not
     * @return this same SparkVerifier
     */
    public SparkVerifier withIgnoreSurplusColumns(boolean ignoreSurplusColumns)
    {
        this.ignoreSurplusColumns = ignoreSurplusColumns;
        return this;
    }

    /**
     * Tells this SparkVerifier to ignore certain columns
     * @param columnsToIgnore the set of columns to ignore
     * @return this same SparkVerifier
     */
    public SparkVerifier withColumnsToIgnore(Set<String> columnsToIgnore)
    {
        this.columnsToIgnore = columnsToIgnore;
        return this;
    }

    /**
     * Sets a global tolerance to apply when comparing doubles
     * @param tolerance the tolerance
     * @return this same SparkVerifier
     */
    public SparkVerifier withTolerance(double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(tolerance);
        return this;
    }

    /**
     * Sets a tolerance to apply to a single column only
     * @param columnName the column name
     * @param tolerance the tolerance
     * @return this same SparkVerifier
     */
    public SparkVerifier withTolerance(String columnName, double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(columnName, tolerance);
        return this;
    }

    /**
     * Compares two HDFS datasets and produces a detailed yet compact HTML break report
     * @param dataName the name to use in the output HTML
     * @param actualDataSupplier the actual data supplier
     * @param expectedDataSupplier the expected data supplier
     * @return a SparkResult containing pass/fail and the HTML report
     */
    public SparkResult verify(String dataName, Supplier<DistributedTable> actualDataSupplier, Supplier<DistributedTable> expectedDataSupplier)
    {
        DistributedTable actualDistributedTable = actualDataSupplier.get();
        if (!new HashSet<>(actualDistributedTable.getHeaders()).containsAll(this.groupKeyColumns)) {
            throw new IllegalArgumentException("Actual data does not contain all group key columns: " + this.groupKeyColumns);
        }
        DistributedTable expectedDistributedTable = expectedDataSupplier.get();
        if (!new HashSet<>(expectedDistributedTable.getHeaders()).containsAll(this.groupKeyColumns)) {
            throw new IllegalArgumentException("Expected data does not contain all group key columns: " + this.groupKeyColumns);
        }
        PartialResult<BoundedDouble> countApproxPartialResult = expectedDistributedTable.getRows().countApprox(TimeUnit.SECONDS.toMillis(5), 0.9);
        int maximumNumberOfGroups = getMaximumNumberOfGroups(countApproxPartialResult.getFinalValue(), maxGroupSize);
        LOGGER.info("Maximum number of groups : " + maximumNumberOfGroups);
        Set<String> groupKeyColumnSet = new LinkedHashSet<>(this.groupKeyColumns);

        SummaryResultTable summaryResultTable;
        if (this.groupKeyIsPrimaryKey)
        {
            JavaPairRDD<List<Object>, List<Object>> actualByPk = actualDistributedTable.getRows().mapToPair(new PrimaryKeyFunction(actualDistributedTable.getHeaders(), groupKeyColumnSet));
            JavaPairRDD<List<Object>, List<Object>> expectedByPk = expectedDistributedTable.getRows().mapToPair(new PrimaryKeyFunction(expectedDistributedTable.getHeaders(), groupKeyColumnSet));
            JavaPairRDD<List<Object>, Tuple2<Optional<List<Object>>, Optional<List<Object>>>> actualExpectedJoin = actualByPk.fullOuterJoin(expectedByPk);
            VerifyPrimaryKeyPairFunction verifyPrimaryKeyPairFunction = new VerifyPrimaryKeyPairFunction(
                    groupKeyColumnSet,
                    actualDistributedTable.getHeaders(),
                    expectedDistributedTable.getHeaders(),
                    this.ignoreSurplusColumns,
                    this.columnComparatorsBuilder.build(),
                    this.columnsToIgnore);
            summaryResultTable = actualExpectedJoin.map(verifyPrimaryKeyPairFunction).reduce(new SummaryResultTableReducer());
        }
        else
        {
            JavaPairRDD<Integer, Iterable<List<Object>>> actualGroups = actualDistributedTable.getRows()
                    .mapToPair(new GroupRowsFunction(actualDistributedTable.getHeaders(), groupKeyColumnSet, maximumNumberOfGroups))
                    .groupByKey();
            JavaPairRDD<Integer, Iterable<List<Object>>> expectedGroups = expectedDistributedTable.getRows()
                    .mapToPair(new GroupRowsFunction(expectedDistributedTable.getHeaders(), groupKeyColumnSet, maximumNumberOfGroups))
                    .groupByKey();
            JavaPairRDD<Integer, Tuple2<Optional<Iterable<List<Object>>>, Optional<Iterable<List<Object>>>>> joinedRdd = actualGroups.fullOuterJoin(expectedGroups);
            VerifyGroupFunction verifyGroupFunction = new VerifyGroupFunction(
                    groupKeyColumnSet,
                    actualDistributedTable.getHeaders(),
                    expectedDistributedTable.getHeaders(),
                    this.ignoreSurplusColumns,
                    this.columnComparatorsBuilder.build(),
                    this.columnsToIgnore);
            summaryResultTable = joinedRdd.map(verifyGroupFunction).reduce(new SummaryResultTableReducer());
        }
        HtmlOptions htmlOptions = new HtmlOptions(false, HtmlFormatter.DEFAULT_ROW_LIMIT, false, false, false, Collections.emptySet());
        HtmlFormatter htmlFormatter = new HtmlFormatter(null, htmlOptions);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try
        {
            htmlFormatter.appendResults(dataName, Collections.singletonMap("Summary", summaryResultTable), metadata, 1, null, bytes);
            return new SparkResult(summaryResultTable.isSuccess(), new String(bytes.toByteArray(), StandardCharsets.UTF_8));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    static int getMaximumNumberOfGroups(BoundedDouble approxCountBoundedDouble, int maxGroupSize)
    {
        long countApprox = Math.round(approxCountBoundedDouble.mean());
        LOGGER.info("Approximate count of expected results: " + countApprox);
        LOGGER.info("Maximum group size: " + maxGroupSize);
        long maximumNumberOfGroups = Math.max(1, countApprox / maxGroupSize);
        if (maximumNumberOfGroups > Integer.MAX_VALUE) {
            throw new IllegalStateException("Invalid max group size: " + maximumNumberOfGroups);
        }
        return (int) maximumNumberOfGroups;
    }
}
