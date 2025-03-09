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

public class BeginningOfLineState extends ParserState {
    BeginningOfLineState(ExpectedResultsParser parserState) {
        super(parserState);
    }

    @Override
    public ParserState parse(StreamTokenizer st) throws IOException {
        ParserState nextState = null;
        while (nextState == null && st.ttype != StreamTokenizer.TT_EOF) {
            int nextToken = st.nextToken();
            if (nextToken != StreamTokenizer.TT_EOL && nextToken != StreamTokenizer.TT_EOF) {
                if (nextValueIs(ExpectedResultsParser.SECTION_IDENTIFIER, st, nextToken)) {
                    nextState = this.getParser().getSectionReaderState();
                } else if (nextValueIs(ExpectedResultsParser.METADATA_IDENTIFIER, st, nextToken)) {
                    nextState = this.getParser().getMetadataReaderState();
                } else {
                    nextState = this.getParser().getDataReaderState();
                }
            }
        }
        return nextState;
    }

    private boolean nextValueIs(String sectionIdentifier, StreamTokenizer st, int nextToken) {
        return nextToken == StreamTokenizer.TT_WORD && st.sval.equals(sectionIdentifier);
    }
}
