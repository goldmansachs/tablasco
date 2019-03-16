package com.gs.tablasco.spark;

@SuppressWarnings("WeakerAccess")
public class SparkResult
{
    private final boolean passed;
    private final String html;

    public SparkResult(boolean passed, String html)
    {
        this.passed = passed;
        this.html = html;
    }

    public boolean isPassed()
    {
        return passed;
    }

    public String getHtml()
    {
        return html;
    }
}
