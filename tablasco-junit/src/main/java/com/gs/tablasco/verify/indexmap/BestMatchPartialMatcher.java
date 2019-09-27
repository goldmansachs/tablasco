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

package com.gs.tablasco.verify.indexmap;

import com.gs.tablasco.VerifiableTable;
import com.gs.tablasco.verify.CellComparator;
import com.gs.tablasco.verify.ColumnComparators;

import java.util.List;

public class BestMatchPartialMatcher implements PartialMatcher
{
    private final VerifiableTable actualData;
    private final VerifiableTable expectedData;
    private final ColumnComparators columnComparators;

    BestMatchPartialMatcher(VerifiableTable actualData, VerifiableTable expectedData, ColumnComparators columnComparators)
    {
        this.actualData = actualData;
        this.expectedData = expectedData;
        this.columnComparators = columnComparators;
    }

    @Override
    public void match(List<UnmatchedIndexMap> allMissingRows, List<UnmatchedIndexMap> allSurplusRows, List<IndexMap> matchedColumns)
    {
        for (UnmatchedIndexMap expected : allMissingRows)
        {
            for (UnmatchedIndexMap actual : allSurplusRows)
            {
                int matchScore = 0;
                for (int colIndex = 0; colIndex < matchedColumns.size(); colIndex++)
                {
                    IndexMap column = matchedColumns.get(colIndex);
                    Object expectedValue = this.expectedData.getValueAt(expected.getExpectedIndex(), column.getExpectedIndex());
                    Object actualValue = this.actualData.getValueAt(actual.getActualIndex(), column.getActualIndex());
                    CellComparator comparator = this.columnComparators.getComparator(expectedData.getColumnName(column.getExpectedIndex()));
                    if (comparator.equals(actualValue, expectedValue))
                    {
                        int inverseColumnNumber = matchedColumns.size() - colIndex;
                        matchScore += inverseColumnNumber * inverseColumnNumber;
                    }
                }
                if (matchScore > 0)
                {
                    expected.addMatch(matchScore, actual);
                }
            }
        }
        UnmatchedIndexMap.linkBestMatches(allMissingRows);
    }
}
