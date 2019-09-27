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

import java.io.Serializable;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

public class CellFormatter implements Function<Object, String>, Serializable
{
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private final NumberFormat numberFormat;
    private final DateFormat dateFormat;
    private final double tolerance;
    private final StringBuilder builder;

    public CellFormatter(double tolerance, boolean isGroupingUsed)
    {
        this.tolerance = tolerance;
        this.numberFormat = NumberFormat.getInstance();
        this.numberFormat.setMinimumFractionDigits(0);
        int numberOfMeaningfulDigitsToDisplay = (int) Math.ceil(-1 * Math.log10(tolerance));
        this.numberFormat.setMaximumFractionDigits(numberOfMeaningfulDigitsToDisplay);
        this.numberFormat.setGroupingUsed(isGroupingUsed);
        this.dateFormat = new SimpleDateFormat(DATE_FORMAT);
        this.builder = new StringBuilder();
    }

    @Override
    public String apply(Object object)
    {
        return this.format(object);
    }

    public String format(Object value)
    {
        if (isNumber(value))
        {
            if (value.equals(Double.NaN) || value.equals(Float.NaN))
            {
                return "NaN";
            }
            String formatted = this.numberFormat.format(value);
            if (isNegativeZero(formatted))
            {
                return formatted.substring(1);
            }
            return formatted;
        }
        if (value instanceof Date)
        {
            return this.dateFormat.format(value);
        }
        if (value == null)
        {
            return "";
        }
        return this.formatString(String.valueOf(value));
    }

    private String formatString(String untrimmedValue)
    {
        String value = untrimmedValue.trim();
        this.builder.setLength(0);
        boolean changed = false;
        for (int i = 0; i < value.length(); i++)
        {
            char c = value.charAt(i);
            if (Character.isWhitespace(c))
            {
                this.builder.append(' ');
                if (c != ' ')
                {
                    changed = true;
                }
            }
            else
            {
                this.builder.append(c);
            }
        }
        return changed ? this.builder.toString() : value;
    }

    static boolean isNegativeZero(String formatted)
    {
        if (!isCharAt(formatted, 0, '-'))
        {
            return false;
        }
        if (!isCharAt(formatted, 1, '0'))
        {
            return false;
        }
        if (formatted.length() == 2)
        {
            return true;
        }
        if (!isCharAt(formatted, 2, '.'))
        {
            return false;
        }
        for (int i = 3; i < formatted.length(); i++)
        {
            if (formatted.charAt(i) != '0')
            {
                return false;
            }
        }
        return true;
    }

    private static boolean isCharAt(String str, int index, char ch)
    {
        return str.length() > index && str.charAt(index) == ch;
    }

    double getTolerance()
    {
        return this.tolerance;
    }

    static boolean isNumber(Object value)
    {
        return value instanceof Number;
    }
}
