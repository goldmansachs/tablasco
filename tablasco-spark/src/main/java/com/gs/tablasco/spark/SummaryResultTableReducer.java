package com.gs.tablasco.spark;

import com.gs.tablasco.verify.SummaryResultTable;
import org.apache.spark.api.java.function.Function2;

public class SummaryResultTableReducer implements Function2<SummaryResultTable, SummaryResultTable, SummaryResultTable>
{
    @Override
    public SummaryResultTable call(SummaryResultTable t1, SummaryResultTable t2)
    {
        return t1.merge(t2);
    }
}
