package com.amex.lumi;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface IngestionOptions extends PipelineOptions {
    @Description("A file, split directory, glob")
    String getInputPath();
    void setInputPath(String value);

    @Description("File extension used to select files from an input directory")
    String getFileExtension();
    void setFileExtension(String value);

    @Description("Execution UUID shared by all records in this run")
    String getExecutionId();
    void setExecutionId(String value);

    @Description("Path prefix for parse and validation errors")
    String getErrorLogPath();
    void setErrorLogPath(String value);

    @Description("Optional properties file containing record.count")
    String getControlFile();
    void setControlFile(String value);
}