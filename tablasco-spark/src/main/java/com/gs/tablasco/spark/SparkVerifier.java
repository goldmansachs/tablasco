package com.gs.tablasco.spark;

import com.gs.tablasco.verify.*;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("WeakerAccess")
public class SparkVerifier
{
    private final List<String> groupKeyColumns;
    private final int maximumNumberOfGroups;
    private final DataFormat dataFormat;
    private final Metadata metadata = Metadata.newEmpty();
    private final ColumnComparators.Builder columnComparatorsBuilder = new ColumnComparators.Builder();
    private boolean ignoreSurplusColumns;
    private Set<String> columnsToIgnore;

    public SparkVerifier(List<String> groupKeyColumns, int maximumNumberOfGroups, DataFormat dataFormat)
    {
        this.groupKeyColumns = groupKeyColumns;
        this.maximumNumberOfGroups = maximumNumberOfGroups;
        this.dataFormat = dataFormat;
    }

    public final SparkVerifier withMetadata(String name, String value)
    {
        this.metadata.add(name, value);
        return this;
    }

    public SparkVerifier withIgnoreSurplusColumns(boolean ignoreSurplusColumns)
    {
        this.ignoreSurplusColumns = ignoreSurplusColumns;
        return this;
    }

    public SparkVerifier withColumnsToIgnore(Set<String> columnsToIgnore)
    {
        this.columnsToIgnore = columnsToIgnore;
        return this;
    }

    public SparkVerifier withTolerance(double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(tolerance);
        return this;
    }

    public SparkVerifier withTolerance(String columnName, double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(columnName, tolerance);
        return this;
    }

    public SparkResult verify(String dataName, Path actualDataLocation, Path expectedDataLocation)
    {
        Set<String> groupKeyColumnSet = new LinkedHashSet<>(this.groupKeyColumns);
        DistributedTable actualDistributedTable = this.dataFormat.getDistributedTable(actualDataLocation, groupKeyColumnSet, this.maximumNumberOfGroups);
        DistributedTable expectedDistributedTable = this.dataFormat.getDistributedTable(expectedDataLocation, groupKeyColumnSet, this.maximumNumberOfGroups);
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
        SummaryResultTable summaryResultTable = joinedRdd.map(verifyGroupFunction).reduce(new SummaryResultTableReducer());
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

}
