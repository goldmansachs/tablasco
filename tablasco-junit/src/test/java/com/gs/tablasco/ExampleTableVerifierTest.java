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

package com.gs.tablasco;

import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ExampleTableVerifierTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier().withMavenDirectoryStrategy();

    @Test
    public void example()
    {
        List<Movie> movieRanks = Arrays.asList(
            new Movie("The Batman", 2022, 1,  8.3),
            new Movie("Deep Water", 2022, 2, 5.4),
            new Movie("X", 2022, 3, 7.4),
            new Movie("The Adam Project", 2022, 4, 6.7),
            new Movie("Turning Red", 2022, 5, 7.1),
            new Movie("Windfall", 2022, 6, 5.7));
        this.tableVerifier.verify("Most Popular Movies", new MovieTable(movieRanks));
    }

    private static class MovieTable implements VerifiableTable
    {
        private final List<Movie> rows;

        private MovieTable(List<Movie> rows)
        {
            this.rows = rows;
        }

        @Override
        public int getRowCount()
        {
            return this.rows.size();
        }

        @Override
        public int getColumnCount()
        {
            return 4;
        }

        @Override
        public String getColumnName(int columnIndex)
        {
            switch (columnIndex)
            {
                case 0: return "Title";
                case 1: return "Year";
                case 2: return "User Rank";
                default: return "IMDb Rating";
            }
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            Movie row = this.rows.get(rowIndex);
            switch (columnIndex)
            {
                case 0: return row.title;
                case 1: return row.year;
                case 2: return row.rank;
                default: return row.rating;
            }
        }
    }

    private static class Movie {
        private final String title;
        private final int year;
        private final int rank;
        private final double rating;

        public Movie(String title, int year, int rank, double rating) {
            this.title = title;
            this.year = year;
            this.rank = rank;
            this.rating = rating;
        }
    }
}
