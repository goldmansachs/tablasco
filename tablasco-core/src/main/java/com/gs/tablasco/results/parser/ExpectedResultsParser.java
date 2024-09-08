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

import com.gs.tablasco.results.ExpectedResults;
import com.gs.tablasco.results.ExpectedResultsLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;

public class ExpectedResultsParser
{
    static final String SECTION_IDENTIFIER = "Section";
    static final String METADATA_IDENTIFIER = "Metadata";

    private final ExpectedResultsLoader loader;
    private final File file;
    private ExpectedResults results;
    private ExpectedTable expectedTable;
    private HeaderParserState headerState;
    private SectionReaderState sectionReaderState;
    private BeginningOfLineState beginningOfLineState;
    private DataReaderState dataReaderState;
    private MetadataReaderState metadataReaderState;

    public ExpectedResultsParser(ExpectedResultsLoader loader, File file)
    {
        this.loader = loader;
        this.file = file;
        this.initializeStates();
    }

    void startNewSection(String testName, String tableName)
    {
        this.expectedTable = new ExpectedTable();
        this.results.addTable(testName, tableName, this.expectedTable);
        this.dataReaderState.setSectionName(testName);
    }

    private void initializeStates()
    {
        this.sectionReaderState = new SectionReaderState(this);
        this.headerState = new HeaderParserState(this);
        this.beginningOfLineState = new BeginningOfLineState(this);
        this.dataReaderState = new DataReaderState(this);
        this.metadataReaderState = new MetadataReaderState(this);
    }

    ExpectedTable getExpectedTable()
    {
        return this.expectedTable;
    }

    ExpectedResults getExpectedResults()
    {
        return this.results;
    }

    DataReaderState getDataReaderState()
    {
        return this.dataReaderState;
    }

    HeaderParserState getHeaderState()
    {
        return this.headerState;
    }

    SectionReaderState getSectionReaderState()
    {
        return this.sectionReaderState;
    }

    BeginningOfLineState getBeginningOfLineState()
    {
        return this.beginningOfLineState;
    }

    ParserState getMetadataReaderState()
    {
        return this.metadataReaderState;
    }

    public ExpectedResults parse()
    {
        this.results = new ExpectedResults();
        try (InputStream inputStream = this.loader.load(this.file))
        {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            parse(reader);
            return this.results;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (ParseException e)
        {
            throw new RuntimeException("Parsing error reading '" + this.file + '\'', e);
        }
    }

    private void parse(Reader reader) throws ParseException, IOException
    {
        StreamTokenizer st = new StreamTokenizer(reader);
        st.eolIsSignificant(true);
        st.wordChars('_', '_');
        st.parseNumbers();
        st.quoteChar('"');
        // These calls caused comments to be discarded
        st.slashSlashComments(true);
        st.slashStarComments(true);

        // Parse the file
        ParserState currentState = this.getBeginningOfLineState();
        while (currentState != null)
        {
            currentState = currentState.parse(st);
        }
    }
}
