package com.gs.tablasco.spark;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.adapters.TableAdapters;
import com.gs.tablasco.verify.*;
import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class VerifyPrimaryKeyPairFunction implements Function<Tuple2<List<Object>, Tuple2<Optional<List<Object>>, Optional<List<Object>>>>, SummaryResultTable>
{
    private final Set<String> groupKeyColumns;
    private final List<String> actualColumns;
    private final List<String> expectedColumns;
    private final boolean ignoreSurplusColumns;
    private final ColumnComparators columnComparators;
    private final Set<String> columnsToIgnore;

    VerifyPrimaryKeyPairFunction(Set<String> groupKeyColumns, List<String> actualColumns, List<String> expectedHeaders, boolean ignoreSurplusColumns, ColumnComparators columnComparators, Set<String> columnsToIgnore)
    {
        this.groupKeyColumns = groupKeyColumns;
        this.actualColumns = actualColumns;
        this.expectedColumns = expectedHeaders;
        this.ignoreSurplusColumns = ignoreSurplusColumns;
        this.columnComparators = columnComparators;
        this.columnsToIgnore = columnsToIgnore;
    }

    @Override
    public SummaryResultTable call(Tuple2<List<Object>, Tuple2<Optional<List<Object>>, Optional<List<Object>>>> actualExpectedRows) throws Exception
    {
        Optional<List<Object>> actualRow = actualExpectedRows._2()._1();
        Optional<List<Object>> expectedRow = actualExpectedRows._2()._2();
        Iterable<List<Object>> actualRows = actualRow.isPresent() ? Collections.singleton(actualRow.get()) : Collections.emptyList();
        Iterable<List<Object>> expectedRows = expectedRow.isPresent() ? Collections.singleton(expectedRow.get()) : Collections.emptyList();
        VerifiableTable actualTable = getVerifiableTable(actualRows, this.actualColumns);
        VerifiableTable expectedTable = getVerifiableTable(expectedRows, this.expectedColumns);
        IndexMapTableVerifier singleSingleTableVerifier = new IndexMapTableVerifier(
                this.columnComparators,
                false,
                IndexMapTableVerifier.DEFAULT_BEST_MATCH_THRESHOLD,
                false,
                false,
                this.ignoreSurplusColumns,
                false,
                0);
        ResultTable resultTable = singleSingleTableVerifier.verify(actualTable, expectedTable);
        return new SummaryResultTable(resultTable);
    }

    private VerifiableTable getVerifiableTable(Iterable<List<Object>> data, List<String> headers)
    {
        List<List<Object>> dataList = new ArrayList<>();
        data.forEach(dataList::add);
        VerifiableTable verifiableTable = new ListVerifiableTable(headers, dataList);
        if (this.columnsToIgnore != null)
        {
            verifiableTable = TableAdapters.withColumns(verifiableTable, (col) -> !VerifyPrimaryKeyPairFunction.this.columnsToIgnore.contains(col));
        }
        return this.groupKeyColumns.isEmpty() ? verifiableTable : new GroupKeyedVerifiableTable(verifiableTable, this.groupKeyColumns);
    }

    private static class GroupKeyedVerifiableTable extends DefaultVerifiableTableAdapter implements KeyedVerifiableTable
    {
        private final Set<String> groupKeyColumns;

        GroupKeyedVerifiableTable(VerifiableTable delegate, Set<String> groupKeyColumns)
        {
            super(delegate);
            this.groupKeyColumns = groupKeyColumns;
        }

        @Override
        public boolean isKeyColumn(int columnIndex)
        {
            return columnIndex >= 0 && this.groupKeyColumns.contains(this.getColumnName(columnIndex));
        }
    }

}
