package org.example;


import java.util.List;
import java.util.Map;

class BenchmarkInfo {
    private String benchmarkName;
    private String benchamrkSourcePaths;
    private Map<String, List<String>> schema;
    private Map<String,List<String>> columnNames;
    private Map<String, Integer> numRows;
    private String delimiter;
    private String outputPath;

    public BenchmarkInfo(String benchmarkName, String benchamrkSourcePaths,
                         Map<String, List<String>> schema, Map<String,List<String>> columnNames,
                         Map<String, Integer> numRows,String delimiter,String outputPath) {
        this.benchmarkName = benchmarkName;
        this.benchamrkSourcePaths = benchamrkSourcePaths;
        this.schema = schema;
        this.columnNames=columnNames;
        this.numRows = numRows;
        this.delimiter=delimiter;
        this.outputPath=outputPath;
    }

    public String getBenchmarkName() {
        return benchmarkName;
    }

    public String getBenchamrkSourcePaths() {
        return benchamrkSourcePaths;
    }

    public Map<String, List<String>> getSchema() {
        return schema;
    }

    public Map<String,List<String>> getColumnNames() {return columnNames;}

    public Map<String, Integer> getNumRows() {
        return numRows;
    }

    public String getDelimiter() {return delimiter;}

    public String getOutputPath() {return  outputPath;}
}