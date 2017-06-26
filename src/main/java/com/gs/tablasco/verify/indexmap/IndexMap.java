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

public class IndexMap implements Comparable<IndexMap>
{
    private final int expectedIndex;
    private final int actualIndex;
    private boolean isOutOfOrder;

    public IndexMap(int expectedIndex, int actualIndex)
    {
        this.expectedIndex = expectedIndex;
        this.actualIndex = actualIndex;
        this.isOutOfOrder = false;
        if (expectedIndex < 0 && actualIndex < 0)
        {
            throw new IllegalStateException("Only one index can be negative: " + this);
        }
    }

    public void setOutOfOrder()
    {
        this.isOutOfOrder = true;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof IndexMap)
        {
            IndexMap that = (IndexMap) obj;
            return this.expectedIndex == that.expectedIndex && this.actualIndex == that.actualIndex;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.isMissing() ? this.expectedIndex : this.actualIndex;
    }

    @Override
    public int compareTo(IndexMap that)
    {
        if (this.equals(that))
        {
            return 0;
        }
        if (this.isMatched())
        {
            if (that.actualIndex >= 0)
            {
                return compareUnequals(this.actualIndex, that.actualIndex, this.isSurplus());
            }
            return compareUnequals(this.expectedIndex, that.expectedIndex, this.isSurplus());
        }
        if (this.isSurplus())
        {
            if (that.actualIndex >= 0)
            {
                return compareUnequals(this.actualIndex, that.actualIndex, this.isSurplus());
            }
            return compareUnequals(this.actualIndex, that.expectedIndex, this.isSurplus());
        }
        if (that.expectedIndex >= 0)
        {
            return compareUnequals(this.expectedIndex, that.expectedIndex, this.isSurplus());
        }
        return compareUnequals(this.expectedIndex, that.actualIndex, this.isSurplus());
    }

    public boolean isMissing()
    {
        return this.expectedIndex >= 0 && this.actualIndex < 0;
    }

    public boolean isSurplus()
    {
        return this.actualIndex >= 0 && this.expectedIndex < 0;
    }

    public boolean isMatched()
    {
        return this.actualIndex >= 0 && this.expectedIndex >= 0;
    }

    public boolean isOutOfOrder()
    {
        return this.isOutOfOrder;
    }

    private static int compareUnequals(int thisIndex, int thatIndex, boolean thisIsSurplus)
    {
        if (thisIndex < thatIndex)
        {
            return -1;
        }
        if (thisIndex > thatIndex)
        {
            return 1;
        }
        if (thisIsSurplus)
        {
            return -1;
        }
        return 1;
    }

    @Override
    public String toString()
    {
        return "IndexMap{" +
                "expectedIndex=" + expectedIndex +
                ", actualIndex=" + actualIndex +
                ", isOutOfOrder=" + isOutOfOrder +
                '}';
    }

    public int getExpectedIndex()
    {
        return expectedIndex;
    }

    public int getActualIndex()
    {
        return actualIndex;
    }
}
