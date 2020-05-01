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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link BasicRoutingFunctions}
 */
public class BasicRoutingFunctionsTest {
  @Test
  public void testStringFunctions() {
    BasicRoutingFunction function = new BasicRoutingFunctions.EqualsFunction();
    Assert.assertTrue(function.evaluate("value", "value"));
    Assert.assertFalse(function.evaluate("val", "value"));
    function = new BasicRoutingFunctions.NotEqualsFunction();
    Assert.assertTrue(function.evaluate("val", "value"));
    Assert.assertFalse(function.evaluate("value", "value"));
    function = new BasicRoutingFunctions.ContainsFunction();
    Assert.assertTrue(function.evaluate("value", "val"));
    Assert.assertFalse(function.evaluate("val", "value"));
    function = new BasicRoutingFunctions.NotContainsFunction();
    Assert.assertTrue(function.evaluate("val", "value"));
    Assert.assertFalse(function.evaluate("value", "val"));
    function = new BasicRoutingFunctions.InFunction();
    Assert.assertTrue(function.evaluate("val", "val|value"));
    Assert.assertFalse(function.evaluate("value", "val|val1"));
    function = new BasicRoutingFunctions.NotInFunction();
    Assert.assertTrue(function.evaluate("val", "value|value1"));
    Assert.assertFalse(function.evaluate("val", "val|val1"));
    function = new BasicRoutingFunctions.StartsWithFunction();
    Assert.assertTrue(function.evaluate("value", "val"));
    Assert.assertFalse(function.evaluate("val", "value"));
    function = new BasicRoutingFunctions.NotStartsWithFunction();
    Assert.assertTrue(function.evaluate("val", "value"));
    Assert.assertFalse(function.evaluate("value", "val"));
    function = new BasicRoutingFunctions.EndsWithFunction();
    Assert.assertTrue(function.evaluate("value", "lue"));
    Assert.assertFalse(function.evaluate("value", "value1"));
    function = new BasicRoutingFunctions.NotEndsWithFunction();
    Assert.assertTrue(function.evaluate("value", "value1"));
    Assert.assertFalse(function.evaluate("value", "lue"));
    function = new BasicRoutingFunctions.MatchesFunction();
    Assert.assertTrue(function.evaluate("value", ".*alu.*"));
    Assert.assertFalse(function.evaluate("value", ".*al$"));
    function = new BasicRoutingFunctions.NotMatchesFunction();
    Assert.assertTrue(function.evaluate("value", ".*al$"));
    Assert.assertFalse(function.evaluate("value", ".*alu.*"));
  }

  @Test
  public void testNumericFunctions() {
    BasicRoutingFunction function = new BasicRoutingFunctions.NumberEqualsFunction();
    Assert.assertTrue(function.evaluate("999999999999", "999999999999"));
    Assert.assertFalse(function.evaluate("999999999999", "9999999999999"));
    try {
      Assert.assertFalse(function.evaluate("9999", "val"));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.NumberNotEqualsFunction();
    Assert.assertTrue(function.evaluate("999999999999", "99999999999999"));
    Assert.assertFalse(function.evaluate("999999999999", "999999999999"));
    try {
      Assert.assertFalse(function.evaluate("9999", "val"));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.GreaterThanFunction();
    Assert.assertTrue(function.evaluate("999999999999", "999999"));
    Assert.assertFalse(function.evaluate("999999", "999999999999"));
    Assert.assertFalse(function.evaluate("999999999999", "999999999999"));
    try {
      Assert.assertFalse(function.evaluate("9999", "val"));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.GreaterThanOrEqualsFunction();
    Assert.assertTrue(function.evaluate("999999999999", "999999"));
    Assert.assertTrue(function.evaluate("999999999999", "999999999999"));
    Assert.assertFalse(function.evaluate("999999", "999999999999"));
    try {
      Assert.assertFalse(function.evaluate("9999", "val"));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.LesserThanFunction();
    Assert.assertTrue(function.evaluate("999999", "999999999999"));
    Assert.assertFalse(function.evaluate("999999999999", "99999"));
    Assert.assertFalse(function.evaluate("999999999999", "999999999999"));
    try {
      Assert.assertFalse(function.evaluate("9999", "val"));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.LesserThanOrEqualsFunction();
    Assert.assertTrue(function.evaluate("999999", "999999999999"));
    Assert.assertTrue(function.evaluate("999999999999", "999999999999"));
    Assert.assertFalse(function.evaluate("999999999999", "999999"));
    try {
      Assert.assertFalse(function.evaluate("9999", "val"));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.NumberBetweenFunction();
    Assert.assertTrue(function.evaluate("999999", "99999|999999999999"));
    Assert.assertTrue(function.evaluate("999999", "999999|999999999999"));
    Assert.assertTrue(function.evaluate("999999", "9999|999999"));
    Assert.assertFalse(function.evaluate("999999", "9999999|99999999"));
    try {
      Assert.assertFalse(function.evaluate("9999", "9999"));
      Assert.fail("Expected function to fail when only one of lower and upper bound is specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate("9999", "9999|99999|99999"));
      Assert.fail("Expected function to fail when more than lower and upper bound is specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate("9999", "val"));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.NumberNotBetweenFunction();
    Assert.assertTrue(function.evaluate("999999", "999999999|999999999999"));
    Assert.assertFalse(function.evaluate("999999", "999999|999999999999"));
    Assert.assertFalse(function.evaluate("999999", "9999|999999"));
    Assert.assertFalse(function.evaluate("999999", "9999|99999999"));
    try {
      Assert.assertFalse(function.evaluate("9999", "9999"));
      Assert.fail("Expected function to fail when only one of lower and upper bound is specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate("9999", "9999|99999|99999"));
      Assert.fail("Expected function to fail when more than lower and upper bound is specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate("9999", "val"));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
