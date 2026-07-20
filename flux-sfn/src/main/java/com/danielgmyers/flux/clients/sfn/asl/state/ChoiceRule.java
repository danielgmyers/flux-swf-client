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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an ASL Choice Rule. A choice rule can be either a data-test expression
 * (with Variable and a comparison operator) or a boolean expression (And, Or, Not).
 *
 * Data-test expression usage:
 *   rule.setVariable("$.type");
 *   rule.setStringEquals("Private");
 *   rule.setNext("HandlePrivate");
 *
 * Boolean expression usage:
 *   rule.setAnd(List.of(subRule1, subRule2));
 *   rule.setNext("HandleBoth");
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChoiceRule {

    // Transition
    @JsonProperty("Next")
    private String next;

    // Boolean expressions
    @JsonProperty("And")
    private List<ChoiceRule> and;

    @JsonProperty("Or")
    private List<ChoiceRule> or;

    @JsonProperty("Not")
    private ChoiceRule not;

    // Data-test expression: Variable to compare
    @JsonProperty("Variable")
    private String variable;

    // String comparisons
    @JsonProperty("StringEquals")
    private String stringEquals;

    @JsonProperty("StringEqualsPath")
    private String stringEqualsPath;

    @JsonProperty("StringLessThan")
    private String stringLessThan;

    @JsonProperty("StringLessThanPath")
    private String stringLessThanPath;

    @JsonProperty("StringGreaterThan")
    private String stringGreaterThan;

    @JsonProperty("StringGreaterThanPath")
    private String stringGreaterThanPath;

    @JsonProperty("StringLessThanEquals")
    private String stringLessThanEquals;

    @JsonProperty("StringLessThanEqualsPath")
    private String stringLessThanEqualsPath;

    @JsonProperty("StringGreaterThanEquals")
    private String stringGreaterThanEquals;

    @JsonProperty("StringGreaterThanEqualsPath")
    private String stringGreaterThanEqualsPath;

    @JsonProperty("StringMatches")
    private String stringMatches;

    // Numeric comparisons
    @JsonProperty("NumericEquals")
    private Number numericEquals;

    @JsonProperty("NumericEqualsPath")
    private String numericEqualsPath;

    @JsonProperty("NumericLessThan")
    private Number numericLessThan;

    @JsonProperty("NumericLessThanPath")
    private String numericLessThanPath;

    @JsonProperty("NumericGreaterThan")
    private Number numericGreaterThan;

    @JsonProperty("NumericGreaterThanPath")
    private String numericGreaterThanPath;

    @JsonProperty("NumericLessThanEquals")
    private Number numericLessThanEquals;

    @JsonProperty("NumericLessThanEqualsPath")
    private String numericLessThanEqualsPath;

    @JsonProperty("NumericGreaterThanEquals")
    private Number numericGreaterThanEquals;

    @JsonProperty("NumericGreaterThanEqualsPath")
    private String numericGreaterThanEqualsPath;

    // Boolean comparison
    @JsonProperty("BooleanEquals")
    private Boolean booleanEquals;

    @JsonProperty("BooleanEqualsPath")
    private String booleanEqualsPath;

    // Timestamp comparisons
    @JsonProperty("TimestampEquals")
    private String timestampEquals;

    @JsonProperty("TimestampEqualsPath")
    private String timestampEqualsPath;

    @JsonProperty("TimestampLessThan")
    private String timestampLessThan;

    @JsonProperty("TimestampLessThanPath")
    private String timestampLessThanPath;

    @JsonProperty("TimestampGreaterThan")
    private String timestampGreaterThan;

    @JsonProperty("TimestampGreaterThanPath")
    private String timestampGreaterThanPath;

    @JsonProperty("TimestampLessThanEquals")
    private String timestampLessThanEquals;

    @JsonProperty("TimestampLessThanEqualsPath")
    private String timestampLessThanEqualsPath;

    @JsonProperty("TimestampGreaterThanEquals")
    private String timestampGreaterThanEquals;

    @JsonProperty("TimestampGreaterThanEqualsPath")
    private String timestampGreaterThanEqualsPath;

    // Type checks
    @JsonProperty("IsNull")
    private Boolean isNull;

    @JsonProperty("IsPresent")
    private Boolean isPresent;

    @JsonProperty("IsNumeric")
    private Boolean isNumeric;

    @JsonProperty("IsString")
    private Boolean isString;

    @JsonProperty("IsBoolean")
    private Boolean isBoolean;

    @JsonProperty("IsTimestamp")
    private Boolean isTimestamp;

    // --- Getters and Setters ---

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }

    public List<ChoiceRule> getAnd() {
        return and;
    }

    public void setAnd(List<ChoiceRule> and) {
        this.and = and;
    }

    public List<ChoiceRule> getOr() {
        return or;
    }

    public void setOr(List<ChoiceRule> or) {
        this.or = or;
    }

    public ChoiceRule getNot() {
        return not;
    }

    public void setNot(ChoiceRule not) {
        this.not = not;
    }

    public String getVariable() {
        return variable;
    }

    public void setVariable(String variable) {
        this.variable = variable;
    }

    public String getStringEquals() {
        return stringEquals;
    }

    public void setStringEquals(String stringEquals) {
        this.stringEquals = stringEquals;
    }

    public String getStringEqualsPath() {
        return stringEqualsPath;
    }

    public void setStringEqualsPath(String stringEqualsPath) {
        this.stringEqualsPath = stringEqualsPath;
    }

    public String getStringLessThan() {
        return stringLessThan;
    }

    public void setStringLessThan(String stringLessThan) {
        this.stringLessThan = stringLessThan;
    }

    public String getStringLessThanPath() {
        return stringLessThanPath;
    }

    public void setStringLessThanPath(String stringLessThanPath) {
        this.stringLessThanPath = stringLessThanPath;
    }

    public String getStringGreaterThan() {
        return stringGreaterThan;
    }

    public void setStringGreaterThan(String stringGreaterThan) {
        this.stringGreaterThan = stringGreaterThan;
    }

    public String getStringGreaterThanPath() {
        return stringGreaterThanPath;
    }

    public void setStringGreaterThanPath(String stringGreaterThanPath) {
        this.stringGreaterThanPath = stringGreaterThanPath;
    }

    public String getStringLessThanEquals() {
        return stringLessThanEquals;
    }

    public void setStringLessThanEquals(String stringLessThanEquals) {
        this.stringLessThanEquals = stringLessThanEquals;
    }

    public String getStringLessThanEqualsPath() {
        return stringLessThanEqualsPath;
    }

    public void setStringLessThanEqualsPath(String stringLessThanEqualsPath) {
        this.stringLessThanEqualsPath = stringLessThanEqualsPath;
    }

    public String getStringGreaterThanEquals() {
        return stringGreaterThanEquals;
    }

    public void setStringGreaterThanEquals(String stringGreaterThanEquals) {
        this.stringGreaterThanEquals = stringGreaterThanEquals;
    }

    public String getStringGreaterThanEqualsPath() {
        return stringGreaterThanEqualsPath;
    }

    public void setStringGreaterThanEqualsPath(String stringGreaterThanEqualsPath) {
        this.stringGreaterThanEqualsPath = stringGreaterThanEqualsPath;
    }

    public String getStringMatches() {
        return stringMatches;
    }

    public void setStringMatches(String stringMatches) {
        this.stringMatches = stringMatches;
    }

    public Number getNumericEquals() {
        return numericEquals;
    }

    public void setNumericEquals(Number numericEquals) {
        this.numericEquals = numericEquals;
    }

    public String getNumericEqualsPath() {
        return numericEqualsPath;
    }

    public void setNumericEqualsPath(String numericEqualsPath) {
        this.numericEqualsPath = numericEqualsPath;
    }

    public Number getNumericLessThan() {
        return numericLessThan;
    }

    public void setNumericLessThan(Number numericLessThan) {
        this.numericLessThan = numericLessThan;
    }

    public String getNumericLessThanPath() {
        return numericLessThanPath;
    }

    public void setNumericLessThanPath(String numericLessThanPath) {
        this.numericLessThanPath = numericLessThanPath;
    }

    public Number getNumericGreaterThan() {
        return numericGreaterThan;
    }

    public void setNumericGreaterThan(Number numericGreaterThan) {
        this.numericGreaterThan = numericGreaterThan;
    }

    public String getNumericGreaterThanPath() {
        return numericGreaterThanPath;
    }

    public void setNumericGreaterThanPath(String numericGreaterThanPath) {
        this.numericGreaterThanPath = numericGreaterThanPath;
    }

    public Number getNumericLessThanEquals() {
        return numericLessThanEquals;
    }

    public void setNumericLessThanEquals(Number numericLessThanEquals) {
        this.numericLessThanEquals = numericLessThanEquals;
    }

    public String getNumericLessThanEqualsPath() {
        return numericLessThanEqualsPath;
    }

    public void setNumericLessThanEqualsPath(String numericLessThanEqualsPath) {
        this.numericLessThanEqualsPath = numericLessThanEqualsPath;
    }

    public Number getNumericGreaterThanEquals() {
        return numericGreaterThanEquals;
    }

    public void setNumericGreaterThanEquals(Number numericGreaterThanEquals) {
        this.numericGreaterThanEquals = numericGreaterThanEquals;
    }

    public String getNumericGreaterThanEqualsPath() {
        return numericGreaterThanEqualsPath;
    }

    public void setNumericGreaterThanEqualsPath(String numericGreaterThanEqualsPath) {
        this.numericGreaterThanEqualsPath = numericGreaterThanEqualsPath;
    }

    public Boolean getBooleanEquals() {
        return booleanEquals;
    }

    public void setBooleanEquals(Boolean booleanEquals) {
        this.booleanEquals = booleanEquals;
    }

    public String getBooleanEqualsPath() {
        return booleanEqualsPath;
    }

    public void setBooleanEqualsPath(String booleanEqualsPath) {
        this.booleanEqualsPath = booleanEqualsPath;
    }

    public String getTimestampEquals() {
        return timestampEquals;
    }

    public void setTimestampEquals(String timestampEquals) {
        this.timestampEquals = timestampEquals;
    }

    public String getTimestampEqualsPath() {
        return timestampEqualsPath;
    }

    public void setTimestampEqualsPath(String timestampEqualsPath) {
        this.timestampEqualsPath = timestampEqualsPath;
    }

    public String getTimestampLessThan() {
        return timestampLessThan;
    }

    public void setTimestampLessThan(String timestampLessThan) {
        this.timestampLessThan = timestampLessThan;
    }

    public String getTimestampLessThanPath() {
        return timestampLessThanPath;
    }

    public void setTimestampLessThanPath(String timestampLessThanPath) {
        this.timestampLessThanPath = timestampLessThanPath;
    }

    public String getTimestampGreaterThan() {
        return timestampGreaterThan;
    }

    public void setTimestampGreaterThan(String timestampGreaterThan) {
        this.timestampGreaterThan = timestampGreaterThan;
    }

    public String getTimestampGreaterThanPath() {
        return timestampGreaterThanPath;
    }

    public void setTimestampGreaterThanPath(String timestampGreaterThanPath) {
        this.timestampGreaterThanPath = timestampGreaterThanPath;
    }

    public String getTimestampLessThanEquals() {
        return timestampLessThanEquals;
    }

    public void setTimestampLessThanEquals(String timestampLessThanEquals) {
        this.timestampLessThanEquals = timestampLessThanEquals;
    }

    public String getTimestampLessThanEqualsPath() {
        return timestampLessThanEqualsPath;
    }

    public void setTimestampLessThanEqualsPath(String timestampLessThanEqualsPath) {
        this.timestampLessThanEqualsPath = timestampLessThanEqualsPath;
    }

    public String getTimestampGreaterThanEquals() {
        return timestampGreaterThanEquals;
    }

    public void setTimestampGreaterThanEquals(String timestampGreaterThanEquals) {
        this.timestampGreaterThanEquals = timestampGreaterThanEquals;
    }

    public String getTimestampGreaterThanEqualsPath() {
        return timestampGreaterThanEqualsPath;
    }

    public void setTimestampGreaterThanEqualsPath(String timestampGreaterThanEqualsPath) {
        this.timestampGreaterThanEqualsPath = timestampGreaterThanEqualsPath;
    }

    public Boolean getIsNull() {
        return isNull;
    }

    public void setIsNull(Boolean isNull) {
        this.isNull = isNull;
    }

    public Boolean getIsPresent() {
        return isPresent;
    }

    public void setIsPresent(Boolean isPresent) {
        this.isPresent = isPresent;
    }

    public Boolean getIsNumeric() {
        return isNumeric;
    }

    public void setIsNumeric(Boolean isNumeric) {
        this.isNumeric = isNumeric;
    }

    public Boolean getIsString() {
        return isString;
    }

    public void setIsString(Boolean isString) {
        this.isString = isString;
    }

    public Boolean getIsBoolean() {
        return isBoolean;
    }

    public void setIsBoolean(Boolean isBoolean) {
        this.isBoolean = isBoolean;
    }

    public Boolean getIsTimestamp() {
        return isTimestamp;
    }

    public void setIsTimestamp(Boolean isTimestamp) {
        this.isTimestamp = isTimestamp;
    }
}
