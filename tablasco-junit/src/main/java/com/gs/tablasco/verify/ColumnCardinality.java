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

package com.gs.tablasco.verify;

import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.api.block.procedure.primitive.ObjectIntProcedure;
import org.eclipse.collections.api.tuple.primitive.ObjectIntPair;
import org.eclipse.collections.impl.factory.Bags;

import java.io.Serializable;

class ColumnCardinality implements Serializable
{
    private final int maximumCardinalityToCount;
    private MutableBag<Object> bag = Bags.mutable.of();

    ColumnCardinality(int maximumCardinalityToCount)
    {
        this.maximumCardinalityToCount = maximumCardinalityToCount;
    }

    int getDistinctCount()
    {
        if (this.isFull())
        {
            return this.maximumCardinalityToCount;
        }
        return this.bag.sizeDistinct();
    }

    void merge(ColumnCardinality that)
    {
        if (that.isFull())
        {
            this.setFull();
        }
        else
        {
            that.bag.forEachWithOccurrences(new ObjectIntProcedure<Object>()
            {
                @Override
                public void value(Object value, int cardinality)
                {
                    ColumnCardinality.this.addCardinality(value, cardinality);
                }
            });
        }
    }

    void add(Object value)
    {
        this.addCardinality(value, 1);
    }

    void remove(Object value)
    {
        if (!this.isFull())
        {
            this.bag.remove(value);
        }
    }

    void forEachWithOccurrences(final Procedure2<Object, Integer> procedure)
    {
        this.bag.topOccurrences(this.maximumCardinalityToCount).forEach(new Procedure<ObjectIntPair<Object>>()
        {
            @Override
            public void value(ObjectIntPair<Object> pair)
            {
                procedure.value(pair.getOne(), pair.getTwo());
            }
        });

    }

    boolean isFull()
    {
        return this.bag == null;
    }

    private void addCardinality(Object value, int cardinalityToAdd)
    {
        if (!this.isFull())
        {
            if (this.bag.contains(value) || this.bag.sizeDistinct() < this.maximumCardinalityToCount)
            {
                this.bag.addOccurrences(value, cardinalityToAdd);
            }
            else
            {
                this.setFull();
            }
        }
    }

    private void setFull()
    {
        this.bag = null;
    }

}
