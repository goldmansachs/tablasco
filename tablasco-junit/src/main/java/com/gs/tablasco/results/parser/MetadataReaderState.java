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

/**
 * Expected metadata format
 * Metadata "key1" "value1", "key2" "value2"
 */
public class MetadataReaderState extends ParserState
{
    protected MetadataReaderState(ExpectedResultsParser parser)
    {
        super(parser);
    }

    @Override
    public ParserState parse(StreamTokenizer st) throws IOException, ParseException
    {
        int token = st.ttype;
        while (!endOfLineOrFile(token))
        {
            //token 1: key
            st.nextToken();
            if (!endOfLineOrFile(token))
            {
                String key = st.sval;

                //token 2: value
                token = st.nextToken();
                if (endOfLineOrFile(token))
                {
                    throw new ParseException("Expected a value for metadata key: " + key, st.lineno());
                }
                String value = st.sval;
                this.getParser().getExpectedResults().addMetadata(key, value);

                //token 3: EOL or ,
                token = st.nextToken();
                if (!endOfLineOrFile(token) && token != (int) ',')
                {
                    throw new ParseException("Expected EOL or EOF or a comma on line " + st.lineno(), st.lineno());
                }
            }
        }

        return this.getParser().getBeginningOfLineState();
    }

    private static boolean endOfLineOrFile(int token)
    {
        return token == StreamTokenizer.TT_EOL || token == StreamTokenizer.TT_EOF;
    }
}
