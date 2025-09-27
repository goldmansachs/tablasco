package com.gs.tablasco.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class HtmlConfigTest {

    @Test
    void testDefaultConfiguration() {
        HtmlConfig config = new HtmlConfig();

        assertFalse(config.isHideMatchedRows());
        assertFalse(config.isHideMatchedTables());
        assertFalse(config.isHideMatchedColumns());
        assertFalse(config.isShowAssertionSummary());
        assertFalse(config.isSummarizedResults());
        assertTrue(config.getTablesToAlwaysShowMatchedRowsFor().isEmpty());
        assertEquals(10000, config.getHtmlRowLimit());
    }

    @Test
    void testHideMatchedRows() {
        HtmlConfig config = new HtmlConfig().withHideMatchedRows(true);

        assertTrue(config.isHideMatchedRows());
    }

    @Test
    void testHideMatchedColumns() {
        HtmlConfig config = new HtmlConfig().withHideMatchedColumns(true);

        assertTrue(config.isHideMatchedColumns());
    }

    @Test
    void testHideMatchedTables() {
        HtmlConfig config = new HtmlConfig().withHideMatchedTables(true);

        assertTrue(config.isHideMatchedTables());
    }

    @Test
    void testHtmlRowLimit() {
        int limit = 5000;
        HtmlConfig config = new HtmlConfig().withHtmlRowLimit(limit);

        assertEquals(limit, config.getHtmlRowLimit());
    }

    @Test
    void testAssertionSummary() {
        HtmlConfig config = new HtmlConfig().withAssertionSummary(true);

        assertTrue(config.isShowAssertionSummary());
    }

    @Test
    void testSummarizedResults() {
        HtmlConfig config = new HtmlConfig().withSummarizedResults(true);

        assertTrue(config.isSummarizedResults());
    }

    @Test
    void testAlwaysShowMatchedRowsFor() {
        String[] tables = {"table1", "table2"};
        HtmlConfig config = new HtmlConfig().withAlwaysShowMatchedRowsFor(tables);

        assertEquals(2, config.getTablesToAlwaysShowMatchedRowsFor().size());
        assertTrue(config.getTablesToAlwaysShowMatchedRowsFor().contains("table1"));
        assertTrue(config.getTablesToAlwaysShowMatchedRowsFor().contains("table2"));
    }

    @Test
    void testMethodChaining() {
        HtmlConfig config = new HtmlConfig()
                .withHideMatchedRows(true)
                .withHideMatchedColumns(true)
                .withHideMatchedTables(true)
                .withHtmlRowLimit(5000)
                .withAssertionSummary(true)
                .withSummarizedResults(true)
                .withAlwaysShowMatchedRowsFor("table1");

        assertTrue(config.isHideMatchedRows());
        assertTrue(config.isHideMatchedColumns());
        assertTrue(config.isHideMatchedTables());
        assertEquals(5000, config.getHtmlRowLimit());
        assertTrue(config.isShowAssertionSummary());
        assertTrue(config.isSummarizedResults());
        assertEquals(1, config.getTablesToAlwaysShowMatchedRowsFor().size());
        assertTrue(config.getTablesToAlwaysShowMatchedRowsFor().contains("table1"));
    }
}
