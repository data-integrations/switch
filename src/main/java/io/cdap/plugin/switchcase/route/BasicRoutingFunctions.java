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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Class that contains various implementations of {@link BasicRoutingFunction}
 */
final class BasicRoutingFunctions {

  /**
   * Routing function based on equality
   */
  static class EqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return compareValue.equals(actualValue);
    }
  }

  /**
   * Routing function based on non-equality
   */
  static class NotEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return !compareValue.equals(actualValue);
    }
  }

  /**
   * Routing function based on substrings
   */
  static class ContainsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return actualValue.contains(compareValue);
    }
  }

  /**
   * Routing function based on non-substrings
   */
  static class NotContainsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return !actualValue.contains(compareValue);
    }
  }

  /**
   * Routing function based on presence in a list of possible values
   */
  static class InFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      List<String> possibleValues = Arrays.asList(compareValue.split("\\|"));
      return possibleValues.contains(actualValue);
    }
  }

  /**
   * Routing function based on absence from a list of possible values
   */
  static class NotInFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      List<String> possibleValues = Arrays.asList(compareValue.split("\\|"));
      return !possibleValues.contains(actualValue);
    }
  }

  /**
   * Routing function based on matching against a regular expression
   */
  static class MatchesFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return actualValue.matches(compareValue);
    }
  }

  /**
   * Routing function based on not matching a regular expression
   */
  static class NotMatchesFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return !actualValue.matches(compareValue);
    }
  }

  /**
   * Routing function based on matching a prefix
   */
  static class StartsWithFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return actualValue.startsWith(compareValue);
    }
  }

  /**
   * Routing function based on not matching a prefix
   */
  static class NotStartsWithFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return !actualValue.startsWith(compareValue);
    }
  }

  /**
   * Routing function based on matching a suffix
   */
  static class EndsWithFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return actualValue.endsWith(compareValue);
    }
  }

  /**
   * Routing function based on not matching a suffix
   */
  static class NotEndsWithFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return !actualValue.endsWith(compareValue);
    }
  }

  /**
   * Routing function that checks if a number is equal to a value
   */
  static class NumberEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      BigDecimal actualNumber = parseNumber(actualValue, "Number Equals");
      BigDecimal compareNumber = parseNumber(compareValue, "Number Equals");
      return actualNumber.equals(compareNumber);
    }
  }

  /**
   * Routing function that checks if a number is not equal to a value
   */
  static class NumberNotEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      BigDecimal actualNumber = parseNumber(actualValue, "Number Not Equals");
      BigDecimal compareNumber = parseNumber(compareValue, "Number Not Equals");
      return !actualNumber.equals(compareNumber);
    }
  }

  /**
   * Routing function that checks if a number is greater than a value
   */
  static class GreaterThanFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      BigDecimal actualNumber = parseNumber(actualValue, "Number Greater Than");
      BigDecimal compareNumber = parseNumber(compareValue, "Number Greater Than");
      return actualNumber.compareTo(compareNumber) > 0;
    }
  }

  /**
   * Routing function that checks if a number is greater than or equal to a value
   */
  static class GreaterThanOrEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      BigDecimal actualNumber = parseNumber(actualValue, "Number Greater Than or Equals");
      BigDecimal compareNumber = parseNumber(compareValue, "Number Greater Than or Equals");
      return actualNumber.compareTo(compareNumber) >= 0;
    }
  }

  /**
   * Routing function that checks if a number is lesser than a value
   */
  static class LesserThanFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      BigDecimal actualNumber = parseNumber(actualValue, "Number Greater Than");
      BigDecimal compareNumber = parseNumber(compareValue, "Number Greater Than");
      return actualNumber.compareTo(compareNumber) < 0;
    }
  }

  /**
   * Routing function that checks if a number is lesser than or equal to a value
   */
  static class LesserThanOrEqualsFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      BigDecimal actualNumber = parseNumber(actualValue, "Number Greater Than or Equals");
      BigDecimal compareNumber = parseNumber(compareValue, "Number Greater Than or Equals");
      return actualNumber.compareTo(compareNumber) <= 0;
    }
  }

  /**
   * Routing function that checks if a number is lesser than or equal to a value
   */
  static class NumberBetweenFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return checkNumberBetween(actualValue, compareValue);
    }
  }

  /**
   * Routing function that checks if a number is lesser than or equal to a value
   */
  static class NumberNotBetweenFunction implements BasicRoutingFunction {

    @Override
    public boolean evaluate(String actualValue, String compareValue) {
      return !checkNumberBetween(actualValue, compareValue);
    }
  }

  private static BigDecimal parseNumber(String value, String function) {
    BigDecimal returnValue;
    try {
      returnValue = new BigDecimal(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Expected number, but found non-numeric argument '%s' for function %s", value, function)
      );
    }
    return returnValue;
  }

  private static boolean checkNumberBetween(String actualValue, String compareValue) {
    String[] bounds = compareValue.split("\\|");
    if (bounds.length != 2) {
      throw new IllegalArgumentException(
        String.format("Should specify a lower bound and upper bound separated by a pipe. Found %s.", compareValue)
      );
    }
    BigDecimal lower = parseNumber(bounds[0], "Number Between");
    BigDecimal upper = parseNumber(bounds[1], "Number Between");
    BigDecimal actual = parseNumber(actualValue, "Number Between");
    return actual.compareTo(lower) >= 0 && actual.compareTo(upper) <= 0;
  }

  private BasicRoutingFunctions() {
    //no-op
  }
}
