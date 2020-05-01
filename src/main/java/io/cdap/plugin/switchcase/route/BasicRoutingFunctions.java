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

  private BasicRoutingFunctions() {
    //no-op
  }
}
