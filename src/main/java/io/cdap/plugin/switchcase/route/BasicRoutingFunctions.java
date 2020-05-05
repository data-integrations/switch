/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.switchcase.route;

import io.cdap.cdap.api.data.schema.Schema;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Class that contains various implementations of {@link BasicRoutingFunction}
 */
final class BasicRoutingFunctions {

  /**
   * Routing function based on equality
   */
  static class EqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Equals");
      return compareValue.equals(actualValue);
    }
  }

  /**
   * Routing function based on non-equality
   */
  static class NotEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Not Equals");
      return !compareValue.equals(actualValue);
    }
  }

  /**
   * Routing function based on substrings
   */
  static class ContainsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Contains");
      return ((String) actualValue).contains(compareValue);
    }
  }

  /**
   * Routing function based on non-substrings
   */
  static class NotContainsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Not Contains");
      return !((String) actualValue).contains(compareValue);
    }
  }

  /**
   * Routing function based on presence in a list of possible values
   */
  static class InFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "In");
      List<String> possibleValues = Arrays.asList(compareValue.split("\\|"));
      //noinspection SuspiciousMethodCalls
      return possibleValues.contains(actualValue);
    }
  }

  /**
   * Routing function based on absence from a list of possible values
   */
  static class NotInFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Not In");
      List<String> possibleValues = Arrays.asList(compareValue.split("\\|"));
      //noinspection SuspiciousMethodCalls
      return !possibleValues.contains(actualValue);
    }
  }

  /**
   * Routing function based on matching against a regular expression
   */
  static class MatchesFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Matches");
      return ((String) actualValue).matches(compareValue);
    }
  }

  /**
   * Routing function based on not matching a regular expression
   */
  static class NotMatchesFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Not Matches");
      return !((String) actualValue).matches(compareValue);
    }
  }

  /**
   * Routing function based on matching a prefix
   */
  static class StartsWithFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Starts With");
      return ((String) actualValue).startsWith(compareValue);
    }
  }

  /**
   * Routing function based on not matching a prefix
   */
  static class NotStartsWithFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Not Starts With");
      return !((String) actualValue).startsWith(compareValue);
    }
  }

  /**
   * Routing function based on matching a suffix
   */
  static class EndsWithFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Ends With");
      return ((String) actualValue).endsWith(compareValue);
    }
  }

  /**
   * Routing function based on not matching a suffix
   */
  static class NotEndsWithFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      ensureString(actualValue, getNonNullableType(schema), "Not Ends With");
      return !((String) actualValue).endsWith(compareValue);
    }
  }

  /**
   * Routing function that checks if a number is equal to a value
   */
  static class NumberEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      return compare(actualValue, compareValue, schema, "Equals") == 0;
    }
  }

  /**
   * Routing function that checks if a number is not equal to a value
   */
  static class NumberNotEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      return compare(actualValue, compareValue, schema, "Not Equals") != 0;
    }
  }

  /**
   * Routing function that checks if a number is greater than a value
   */
  static class GreaterThanFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      return compare(actualValue, compareValue, schema, "Greater Than") > 0;
    }
  }

  /**
   * Routing function that checks if a number is greater than or equal to a value
   */
  static class GreaterThanOrEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      return compare(actualValue, compareValue, schema, "Greater Than or Equals") >= 0;
    }
  }

  /**
   * Routing function that checks if a number is lesser than a value
   */
  static class LesserThanFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      return compare(actualValue, compareValue, schema, "Less Than") < 0;
    }
  }

  /**
   * Routing function that checks if a number is lesser than or equal to a value
   */
  static class LesserThanOrEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      return compare(actualValue, compareValue, schema, "Less Than or Equals") <= 0;
    }
  }

  /**
   * Routing function that checks if a number is lesser than or equal to a value
   */
  static class NumberBetweenFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      return checkNumberBetween(actualValue, compareValue, schema, "Number Between");
    }
  }

  /**
   * Routing function that checks if a number is lesser than or equal to a value
   */
  static class NumberNotBetweenFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      return !checkNumberBetween(actualValue, compareValue, schema, "Number Not Between");
    }
  }

  /**
   * Routing function that checks for date equality
   */
  static class DateEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      Schema.LogicalType logicalType = Objects.requireNonNull(schema.getLogicalType());
      return compareDate(actualValue, compareValue, logicalType, "Date Equals") == 0;
    }
  }

  /**
   * Routing function that checks for date inequality
   */
  static class DateNotEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      Schema.LogicalType logicalType = Objects.requireNonNull(schema.getLogicalType());
      return compareDate(actualValue, compareValue, logicalType, "Date Not Equals") != 0;
    }
  }

  /**
   * Routing function that checks for a date value being before a specified date (exclusive)
   */
  static class DateBeforeFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      Schema.LogicalType logicalType = Objects.requireNonNull(schema.getLogicalType());
      return compareDate(actualValue, compareValue, logicalType, "Date Before") < 0;
    }
  }

  /**
   * Routing function that checks for a date value being before a specified date (inclusive)
   */
  static class DateBeforeOrOnFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      Schema.LogicalType logicalType = Objects.requireNonNull(schema.getLogicalType());
      return compareDate(actualValue, compareValue, logicalType, "Date Before or On") <= 0;
    }
  }

  /**
   * Routing function that checks for a date value being after a specified date (exclusive)
   */
  static class DateAfterFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      Schema.LogicalType logicalType = Objects.requireNonNull(schema.getLogicalType());
      return compareDate(actualValue, compareValue, logicalType, "Date After") > 0;
    }
  }

  /**
   * Routing function that checks for a date value being after a specified date (inclusive)
   */
  static class DateAfterOrOnFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      Schema.LogicalType logicalType = Objects.requireNonNull(schema.getLogicalType());
      return compareDate(actualValue, compareValue, logicalType, "Date After or On") >= 0;
    }
  }

  /**
   * Routing function that checks for a date value being between two specified dates (inclusive)
   */
  static class DateBetweenFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      Schema.LogicalType logicalType = Objects.requireNonNull(schema.getLogicalType());
      return checkDateBetween(actualValue, compareValue, logicalType, "Date Between");
    }
  }

  /**
   * Routing function that checks for a date value not being between two specified dates (inclusive)
   */
  static class DateNotBetweenFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(Object actualValue, String compareValue, Schema schema) {
      Schema.LogicalType logicalType = Objects.requireNonNull(schema.getLogicalType());
      return !checkDateBetween(actualValue, compareValue, logicalType, "Date Not Between");
    }
  }

  private static Schema.Type getNonNullableType(Schema schema) {
    return schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
  }

  private static void ensureString(Object actualValue, Schema.Type type, String function) {
    if (!(actualValue instanceof String) && !(type == Schema.Type.STRING)) {
      throw new IllegalArgumentException(
        String.format("Function %s can only be called on Strings. Found data type %s and schema type %s",
                      function, actualValue.getClass().getName(), type)
      );
    }
  }

  private static int compare(Object actualValue, String compareValue, Schema schema, String function) {
    Schema.Type type = getNonNullableType(schema);
    switch (type) {
      case INT:
        return ((Integer) actualValue).compareTo(parseInt(compareValue, function));
      case DOUBLE:
        return ((Double) actualValue).compareTo(parseDouble(compareValue, function));
      case FLOAT:
        return ((Float) actualValue).compareTo(parseFloat(compareValue, function));
      case LONG:
        return ((Long) actualValue).compareTo(parseLong(compareValue, function));
      case BYTES:
        // ensure decimal type
        if (schema.getLogicalType() == null) {
          throw new IllegalArgumentException(
            String.format("Routing function %s is not supported on bytes fields.", function)
          );
        }
        if (Schema.LogicalType.DECIMAL != schema.getLogicalType()) {
          throw new IllegalArgumentException(
            String.format("Routing function %s must be called on a field with logical type as decimal.", function)
          );
        }
        BigDecimal actualDecimal = (BigDecimal) actualValue;
        BigDecimal compareDecimal = parseDecimal(compareValue, function);
        return actualDecimal.compareTo(compareDecimal);
      default:
        throw new IllegalArgumentException(
          String.format("Numeric function %s called on non-numeric type %s", function, type)
        );
    }
  }

  private static boolean checkNumberBetween(Object actualValue, String compareValue, Schema schema, String function) {
    String[] bounds = parseRange(compareValue);
    return compare(actualValue, bounds[0], schema, function) >= 0 &&
      compare(actualValue, bounds[1], schema, function) <= 0;
  }

  private static Integer parseInt(String value, String function) {
    int returnValue;
    try {
      returnValue = Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Expected integer, but found argument '%s' for function %s", value, function)
      );
    }
    return returnValue;
  }

  private static Long parseLong(String value, String function) {
    long returnValue;
    try {
      returnValue = Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Expected long, but found argument '%s' for function %s", value, function)
      );
    }
    return returnValue;
  }

  private static Float parseFloat(String value, String function) {
    float returnValue;
    try {
      returnValue = Float.parseFloat(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Expected float, but found argument '%s' for function %s", value, function)
      );
    }
    return returnValue;
  }

  private static Double parseDouble(String value, String function) {
    double returnValue;
    try {
      returnValue = Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Expected double, but found argument '%s' for function %s", value, function)
      );
    }
    return returnValue;
  }

  private static BigDecimal parseDecimal(String value, String function) {
    BigDecimal returnValue;
    try {
      returnValue = new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Expected BigDecimal, but found argument '%s' for function %s", value, function)
      );
    }
    return returnValue;
  }

  private static int compareDate(Object actualValue, String compareValue, Schema.LogicalType logicalType,
                                 String function) {
    switch(logicalType) {
      case DATE:
        LocalDate actualDate = (LocalDate) actualValue;
        LocalDate compareDate = LocalDate.parse(compareValue, DateTimeFormatter.ISO_LOCAL_DATE);
        return actualDate.compareTo(compareDate);
      case TIME_MILLIS:
        LocalTime actualTime = (LocalTime) actualValue;
        LocalTime compareTime = LocalTime.parse(compareValue, DateTimeFormatter.ISO_LOCAL_TIME);
        return actualTime.compareTo(compareTime);
      case TIME_MICROS:
        actualTime = (LocalTime) actualValue;
        compareTime = LocalTime.parse(compareValue, DateTimeFormatter.ISO_LOCAL_TIME);
        return actualTime.compareTo(compareTime);
      // NOTE: For timestamps, can't use ISO_LOCAL_DATE_TIME, because in Java 8, ZonedDateTime.parse() fails if the
      // input does not contain a zone (https://bugs.openjdk.java.net/browse/JDK-8033662). So you have to explicitly
      // specify a zone, and use the ISO_ZONED_DATE_TIME formatter. E.g. the input
      // "2020-05-02T00:03:19-07:00[America/Los_Angeles]" will work, but the input
      // "2020-05-02T00:03:19" (without timezone) will not work.
      case TIMESTAMP_MILLIS:
        ZonedDateTime actualTimestamp = (ZonedDateTime) actualValue;
        ZonedDateTime compareTimestamp = ZonedDateTime.parse(compareValue, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        return actualTimestamp.compareTo(compareTimestamp);
      case TIMESTAMP_MICROS:
        actualTimestamp = (ZonedDateTime) actualValue;
        compareTimestamp = ZonedDateTime.parse(compareValue, DateTimeFormatter.ISO_ZONED_DATE_TIME);
        return actualTimestamp.compareTo(compareTimestamp);
      default:
        throw new IllegalArgumentException(
          String.format("Date function %s called on non-date type %s", function, logicalType)
        );
    }
  }

  private static boolean checkDateBetween(Object actualValue, String compareValue, Schema.LogicalType logicalType,
                                          String function) {
    String[] bounds = parseRange(compareValue);
    return compareDate(actualValue, bounds[0], logicalType, function) >= 0 &&
      compareDate(actualValue, bounds[1], logicalType, function) <= 0;
  }

  private static String[] parseRange(String input) {
    String[] bounds = input.split("\\|");
    if (bounds.length != 2) {
      throw new IllegalArgumentException(
        String.format("Should specify a lower bound and upper bound separated by a pipe. Found %s.", input)
      );
    }
    return bounds;
  }

  private BasicRoutingFunctions() {
    //no-op
  }
}
