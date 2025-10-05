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

package com.gs.tablasco.results.parser;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class DataReaderState extends ParserState {
    private String sectionName;

    DataReaderState(ExpectedResultsParser parserState) {
        super(parserState);
    }

    @Override
    public ParserState parse(StreamTokenizer st) throws IOException, ParseException {
        if (this.sectionName == null) {
            throw new ParseException("no section name found before line " + st.lineno(), st.lineno());
        }

        // parse the data
        int currentAttribute = 0;
        int token = st.ttype;

        boolean wantData = true;
        List<Object> rowValue = new ArrayList<>();
        while (token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF) {
            if (wantData) {
                this.getParser().getExpectedTable().parseData(st, currentAttribute, rowValue);
                currentAttribute++;
            } else {
                if (token != ',') {
                    throw new ParseException("Expected a comma on line " + st.lineno(), st.lineno());
                }
            }
            wantData = !wantData;
            token = st.nextToken();
        }
        if (!rowValue.isEmpty()) {
            this.getParser().getExpectedTable().addRowToList(rowValue);
        }
        return this.getParser().getBeginningOfLineState();
    }

    void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }
}
