package com.gs.tablasco.spark;

import com.google.common.base.Optional;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.FormattableTable;
import com.gs.tablasco.verify.HtmlFormatter;
import com.gs.tablasco.verify.HtmlOptions;
import com.gs.tablasco.verify.Metadata;
import com.gs.tablasco.verify.SummaryResultTable;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.Sets;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
        Pair<List<String>, JavaPairRDD<Integer, Iterable<List<Object>>>> actualHeadersAndGroups = this.dataFormat.readHeadersAndGroups(actualDataLocation, groupKeyColumnSet, this.maximumNumberOfGroups);
        Pair<List<String>, JavaPairRDD<Integer, Iterable<List<Object>>>> expectedHeadersAndGroups = this.dataFormat.readHeadersAndGroups(expectedDataLocation, groupKeyColumnSet, this.maximumNumberOfGroups);
        JavaPairRDD<Integer, Tuple2<Optional<Iterable<List<Object>>>, Optional<Iterable<List<Object>>>>> joinedRdd = actualHeadersAndGroups.getTwo()
                .fullOuterJoin(expectedHeadersAndGroups.getTwo());
        VerifyGroupFunction verifyGroupFunction = new VerifyGroupFunction(
                groupKeyColumnSet,
                actualHeadersAndGroups.getOne(),
                expectedHeadersAndGroups.getOne(),
                this.ignoreSurplusColumns,
                this.columnComparatorsBuilder.build(),
                this.columnsToIgnore);
        SummaryResultTable summaryResultTable = joinedRdd.map(verifyGroupFunction).reduce(new SummaryResultTableReducer());
        HtmlOptions htmlOptions = new HtmlOptions(false, HtmlFormatter.DEFAULT_ROW_LIMIT, false, false, false, Sets.fixedSize.<String>of());
        HtmlFormatter htmlFormatter = new HtmlFormatter(null, htmlOptions);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try
        {
            htmlFormatter.appendResults(dataName, Maps.fixedSize.<String, FormattableTable>of("Summary", summaryResultTable), metadata, 1, null, bytes);
            return new SparkResult(summaryResultTable.isSuccess(), new String(bytes.toByteArray(), "UTF-8"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

}
