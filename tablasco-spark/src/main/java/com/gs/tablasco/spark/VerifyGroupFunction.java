package com.gs.tablasco.spark;

import com.google.common.base.Optional;
import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.adapters.TableAdapters;
import com.gs.tablasco.verify.ColumnComparators;
import com.gs.tablasco.verify.DefaultVerifiableTableAdapter;
import com.gs.tablasco.verify.KeyedVerifiableTable;
import com.gs.tablasco.verify.ListVerifiableTable;
import com.gs.tablasco.verify.ResultTable;
import com.gs.tablasco.verify.SummaryResultTable;
import com.gs.tablasco.verify.indexmap.IndexMapTableVerifier;
import org.apache.spark.api.java.function.Function;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class VerifyGroupFunction implements Function<Tuple2<Integer, Tuple2<Optional<Iterable<List<Object>>>, Optional<Iterable<List<Object>>>>>, SummaryResultTable>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyGroupFunction.class);

    private final Set<String> groupKeyColumns;
    private final List<String> actualHeaders;
    private final List<String> expectedHeaders;
    private final boolean ignoreSurplusColumns;
    private final ColumnComparators columnComparators;
    private final Set<String> columnsToIgnore;

    VerifyGroupFunction(Set<String> groupKeyColumns, List<String> actualHeaders, List<String> expectedHeaders, boolean ignoreSurplusColumns, ColumnComparators columnComparators, Set<String> columnsToIgnore)
    {
        this.groupKeyColumns = groupKeyColumns;
        this.actualHeaders = actualHeaders;
        this.expectedHeaders = expectedHeaders;
        this.ignoreSurplusColumns = ignoreSurplusColumns;
        this.columnComparators = columnComparators;
        this.columnsToIgnore = columnsToIgnore;
    }

    @Override
    public SummaryResultTable call(Tuple2<Integer, Tuple2<Optional<Iterable<List<Object>>>, Optional<Iterable<List<Object>>>>> v1)
    {
        Integer shardNumber = v1._1();
        Optional<Iterable<List<Object>>> actualOptional = v1._2()._1();
        Optional<Iterable<List<Object>>> expectedOptional = v1._2()._2();
        Iterable<List<Object>> actualRows = actualOptional.isPresent() ? actualOptional.get() : Collections.<List<Object>>emptyList();
        Iterable<List<Object>> expectedRows = expectedOptional.isPresent() ? expectedOptional.get() : Collections.<List<Object>>emptyList();
        VerifiableTable actualTable = getVerifiableTable(actualRows, this.actualHeaders);
        VerifiableTable expectedTable = getVerifiableTable(expectedRows, this.expectedHeaders);
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
        LOGGER.info("Verification of shard {} {}", shardNumber, resultTable.isSuccess() ? "PASSED" : "FAILED");
        return new SummaryResultTable(resultTable);
    }

    private VerifiableTable getVerifiableTable(Iterable<List<Object>> data, List<String> headers)
    {
        VerifiableTable verifiableTable = new ListVerifiableTable(headers, FastList.newList(data));
        if (this.columnsToIgnore != null)
        {
            verifiableTable = TableAdapters.withColumns(verifiableTable, new Predicate<String>()
            {
                @Override
                public boolean accept(String s)
                {
                    return !VerifyGroupFunction.this.columnsToIgnore.contains(s);
                }
            });
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
