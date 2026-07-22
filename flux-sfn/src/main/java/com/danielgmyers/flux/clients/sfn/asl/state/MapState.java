/*
 *   Copyright Flux Contributors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.danielgmyers.flux.clients.sfn.asl.state;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an ASL Map state, which processes all elements of an array,
 * potentially in parallel, with the processing of each element independent of the others.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MapState extends AslState {

    @JsonProperty("ItemProcessor")
    private ItemProcessor itemProcessor;

    @JsonProperty("ItemsPath")
    private String itemsPath;

    @JsonProperty("ItemSelector")
    private Map<String, Object> itemSelector;

    @JsonProperty("ItemReader")
    private ItemReader itemReader;

    @JsonProperty("ItemBatcher")
    private ItemBatcher itemBatcher;

    @JsonProperty("ResultWriter")
    private ResultWriter resultWriter;

    @JsonProperty("MaxConcurrency")
    private Integer maxConcurrency;

    @JsonProperty("MaxConcurrencyPath")
    private String maxConcurrencyPath;

    @JsonProperty("ToleratedFailurePercentage")
    private Number toleratedFailurePercentage;

    @JsonProperty("ToleratedFailurePercentagePath")
    private String toleratedFailurePercentagePath;

    @JsonProperty("ToleratedFailureCount")
    private Integer toleratedFailureCount;

    @JsonProperty("ToleratedFailureCountPath")
    private String toleratedFailureCountPath;

    @JsonProperty("Parameters")
    private Map<String, Object> parameters;

    @JsonProperty("ResultSelector")
    private Map<String, Object> resultSelector;

    @JsonProperty("ResultPath")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String resultPath;

    @JsonProperty("Retry")
    private List<Retrier> retry;

    @JsonProperty("Catch")
    private List<Catcher> catchers;

    public MapState() {
        super("Map");
    }

    public ItemProcessor getItemProcessor() {
        return itemProcessor;
    }

    public void setItemProcessor(ItemProcessor itemProcessor) {
        this.itemProcessor = itemProcessor;
    }

    public String getItemsPath() {
        return itemsPath;
    }

    public void setItemsPath(String itemsPath) {
        this.itemsPath = itemsPath;
    }

    public Map<String, Object> getItemSelector() {
        return itemSelector;
    }

    public void setItemSelector(Map<String, Object> itemSelector) {
        this.itemSelector = itemSelector;
    }

    public ItemReader getItemReader() {
        return itemReader;
    }

    public void setItemReader(ItemReader itemReader) {
        this.itemReader = itemReader;
    }

    public ItemBatcher getItemBatcher() {
        return itemBatcher;
    }

    public void setItemBatcher(ItemBatcher itemBatcher) {
        this.itemBatcher = itemBatcher;
    }

    public ResultWriter getResultWriter() {
        return resultWriter;
    }

    public void setResultWriter(ResultWriter resultWriter) {
        this.resultWriter = resultWriter;
    }

    public Integer getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(Integer maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public String getMaxConcurrencyPath() {
        return maxConcurrencyPath;
    }

    public void setMaxConcurrencyPath(String maxConcurrencyPath) {
        this.maxConcurrencyPath = maxConcurrencyPath;
    }

    public Number getToleratedFailurePercentage() {
        return toleratedFailurePercentage;
    }

    public void setToleratedFailurePercentage(Number toleratedFailurePercentage) {
        this.toleratedFailurePercentage = toleratedFailurePercentage;
    }

    public String getToleratedFailurePercentagePath() {
        return toleratedFailurePercentagePath;
    }

    public void setToleratedFailurePercentagePath(String toleratedFailurePercentagePath) {
        this.toleratedFailurePercentagePath = toleratedFailurePercentagePath;
    }

    public Integer getToleratedFailureCount() {
        return toleratedFailureCount;
    }

    public void setToleratedFailureCount(Integer toleratedFailureCount) {
        this.toleratedFailureCount = toleratedFailureCount;
    }

    public String getToleratedFailureCountPath() {
        return toleratedFailureCountPath;
    }

    public void setToleratedFailureCountPath(String toleratedFailureCountPath) {
        this.toleratedFailureCountPath = toleratedFailureCountPath;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public Map<String, Object> getResultSelector() {
        return resultSelector;
    }

    public void setResultSelector(Map<String, Object> resultSelector) {
        this.resultSelector = resultSelector;
    }

    public String getResultPath() {
        return resultPath;
    }

    public void setResultPath(String resultPath) {
        this.resultPath = resultPath;
    }

    public List<Retrier> getRetry() {
        return retry;
    }

    public void setRetry(List<Retrier> retry) {
        this.retry = retry;
    }

    public List<Catcher> getCatchers() {
        return catchers;
    }

    public void setCatchers(List<Catcher> catchers) {
        this.catchers = catchers;
    }

    /**
     * The ItemProcessor defines the sub-state-machine that processes each item or batch.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ItemProcessor {

        @JsonProperty("StartAt")
        private String startAt;

        @JsonProperty("States")
        private Map<String, AslState> states;

        @JsonProperty("ProcessorConfig")
        private Map<String, Object> processorConfig;

        public String getStartAt() {
            return startAt;
        }

        public void setStartAt(String startAt) {
            this.startAt = startAt;
        }

        public Map<String, AslState> getStates() {
            return states;
        }

        public void setStates(Map<String, AslState> states) {
            this.states = states;
        }

        public Map<String, Object> getProcessorConfig() {
            return processorConfig;
        }

        public void setProcessorConfig(Map<String, Object> processorConfig) {
            this.processorConfig = processorConfig;
        }
    }

    /**
     * The ItemReader specifies where to read items from instead of the effective input.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ItemReader {

        @JsonProperty("Resource")
        private String resource;

        @JsonProperty("Parameters")
        private Map<String, Object> parameters;

        @JsonProperty("ReaderConfig")
        private ReaderConfig readerConfig;

        public String getResource() {
            return resource;
        }

        public void setResource(String resource) {
            this.resource = resource;
        }

        public Map<String, Object> getParameters() {
            return parameters;
        }

        public void setParameters(Map<String, Object> parameters) {
            this.parameters = parameters;
        }

        public ReaderConfig getReaderConfig() {
            return readerConfig;
        }

        public void setReaderConfig(ReaderConfig readerConfig) {
            this.readerConfig = readerConfig;
        }
    }

    /**
     * Configuration for the ItemReader.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ReaderConfig {

        @JsonProperty("MaxItems")
        private Integer maxItems;

        @JsonProperty("MaxItemsPath")
        private String maxItemsPath;

        public Integer getMaxItems() {
            return maxItems;
        }

        public void setMaxItems(Integer maxItems) {
            this.maxItems = maxItems;
        }

        public String getMaxItemsPath() {
            return maxItemsPath;
        }

        public void setMaxItemsPath(String maxItemsPath) {
            this.maxItemsPath = maxItemsPath;
        }
    }

    /**
     * The ItemBatcher specifies how to batch items before passing them to each invocation.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ItemBatcher {

        @JsonProperty("MaxItemsPerBatch")
        private Integer maxItemsPerBatch;

        @JsonProperty("MaxItemsPerBatchPath")
        private String maxItemsPerBatchPath;

        @JsonProperty("MaxInputBytesPerBatch")
        private Integer maxInputBytesPerBatch;

        @JsonProperty("MaxInputBytesPerBatchPath")
        private String maxInputBytesPerBatchPath;

        @JsonProperty("BatchInput")
        private Map<String, Object> batchInput;

        public Integer getMaxItemsPerBatch() {
            return maxItemsPerBatch;
        }

        public void setMaxItemsPerBatch(Integer maxItemsPerBatch) {
            this.maxItemsPerBatch = maxItemsPerBatch;
        }

        public String getMaxItemsPerBatchPath() {
            return maxItemsPerBatchPath;
        }

        public void setMaxItemsPerBatchPath(String maxItemsPerBatchPath) {
            this.maxItemsPerBatchPath = maxItemsPerBatchPath;
        }

        public Integer getMaxInputBytesPerBatch() {
            return maxInputBytesPerBatch;
        }

        public void setMaxInputBytesPerBatch(Integer maxInputBytesPerBatch) {
            this.maxInputBytesPerBatch = maxInputBytesPerBatch;
        }

        public String getMaxInputBytesPerBatchPath() {
            return maxInputBytesPerBatchPath;
        }

        public void setMaxInputBytesPerBatchPath(String maxInputBytesPerBatchPath) {
            this.maxInputBytesPerBatchPath = maxInputBytesPerBatchPath;
        }

        public Map<String, Object> getBatchInput() {
            return batchInput;
        }

        public void setBatchInput(Map<String, Object> batchInput) {
            this.batchInput = batchInput;
        }
    }

    /**
     * The ResultWriter specifies where to write results instead of to the Map state's result.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ResultWriter {

        @JsonProperty("Resource")
        private String resource;

        @JsonProperty("Parameters")
        private Map<String, Object> parameters;

        public String getResource() {
            return resource;
        }

        public void setResource(String resource) {
            this.resource = resource;
        }

        public Map<String, Object> getParameters() {
            return parameters;
        }

        public void setParameters(Map<String, Object> parameters) {
            this.parameters = parameters;
        }
    }
}
