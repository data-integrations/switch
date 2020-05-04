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
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

/**
 * Tests for {@link BasicRoutingFunctions}
 */
public class BasicRoutingFunctionsTest {
  @Test
  public void testStringFunctions() {
    Schema stringSchema = Schema.of(Schema.Type.STRING);
    BasicRoutingFunction function = new BasicRoutingFunctions.EqualsFunction();
    Assert.assertTrue(function.evaluate("value", "value", stringSchema));
    Assert.assertFalse(function.evaluate("val", "value", stringSchema));
    function = new BasicRoutingFunctions.NotEqualsFunction();
    Assert.assertTrue(function.evaluate("val", "value", stringSchema));
    Assert.assertFalse(function.evaluate("value", "value", stringSchema));
    function = new BasicRoutingFunctions.ContainsFunction();
    Assert.assertTrue(function.evaluate("value", "val", stringSchema));
    Assert.assertFalse(function.evaluate("val", "value", stringSchema));
    function = new BasicRoutingFunctions.NotContainsFunction();
    Assert.assertTrue(function.evaluate("val", "value", stringSchema));
    Assert.assertFalse(function.evaluate("value", "val", stringSchema));
    function = new BasicRoutingFunctions.InFunction();
    Assert.assertTrue(function.evaluate("val", "val|value", stringSchema));
    Assert.assertFalse(function.evaluate("value", "val|val1", stringSchema));
    function = new BasicRoutingFunctions.NotInFunction();
    Assert.assertTrue(function.evaluate("val", "value|value1", stringSchema));
    Assert.assertFalse(function.evaluate("val", "val|val1", stringSchema));
    function = new BasicRoutingFunctions.StartsWithFunction();
    Assert.assertTrue(function.evaluate("value", "val", stringSchema));
    Assert.assertFalse(function.evaluate("val", "value", stringSchema));
    function = new BasicRoutingFunctions.NotStartsWithFunction();
    Assert.assertTrue(function.evaluate("val", "value", stringSchema));
    Assert.assertFalse(function.evaluate("value", "val", stringSchema));
    function = new BasicRoutingFunctions.EndsWithFunction();
    Assert.assertTrue(function.evaluate("value", "lue", stringSchema));
    Assert.assertFalse(function.evaluate("value", "value1", stringSchema));
    function = new BasicRoutingFunctions.NotEndsWithFunction();
    Assert.assertTrue(function.evaluate("value", "value1", stringSchema));
    Assert.assertFalse(function.evaluate("value", "lue", stringSchema));
    function = new BasicRoutingFunctions.MatchesFunction();
    Assert.assertTrue(function.evaluate("value", ".*alu.*", stringSchema));
    Assert.assertFalse(function.evaluate("value", ".*al$", stringSchema));
    function = new BasicRoutingFunctions.NotMatchesFunction();
    Assert.assertTrue(function.evaluate("value", ".*al$", stringSchema));
    Assert.assertFalse(function.evaluate("value", ".*alu.*", stringSchema));
  }

  @Test
  public void testNumericFunctions() {
    Schema intSchema = Schema.of(Schema.Type.INT);
    Schema longSchema = Schema.of(Schema.Type.LONG);
    Schema floatSchema = Schema.of(Schema.Type.FLOAT);
    Schema doubleSchema = Schema.of(Schema.Type.DOUBLE);
    BasicRoutingFunction function = new BasicRoutingFunctions.NumberEqualsFunction();
    Assert.assertTrue(function.evaluate(999999999999L, "999999999999", longSchema));
    Assert.assertFalse(function.evaluate(999999999999L, "9999999999999", longSchema));
    try {
      Assert.assertFalse(function.evaluate(9999, "val", intSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.NumberNotEqualsFunction();
    Assert.assertTrue(function.evaluate(999999999, "99999", intSchema));
    Assert.assertFalse(function.evaluate(999999999999L, "999999999999", longSchema));
    try {
      Assert.assertFalse(function.evaluate(9999, "val", intSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.GreaterThanFunction();
    Assert.assertTrue(function.evaluate(3.14f, "3", floatSchema));
    Assert.assertFalse(function.evaluate(3.14, "9.99999999999", doubleSchema));
    Assert.assertFalse(function.evaluate(3.14, "3.14", doubleSchema));
    try {
      Assert.assertFalse(function.evaluate(9999, "val", intSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.GreaterThanOrEqualsFunction();
    Assert.assertTrue(function.evaluate(999999999999L, "999999", longSchema));
    Assert.assertTrue(function.evaluate(999999999999L, "999999999999", longSchema));
    Assert.assertFalse(function.evaluate(999999, "99999999", intSchema));
    try {
      Assert.assertFalse(function.evaluate(9999, "val", intSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.LesserThanFunction();
    Assert.assertTrue(function.evaluate(999999, "9999999", intSchema));
    Assert.assertFalse(function.evaluate(99999, "999", intSchema));
    Assert.assertFalse(function.evaluate(9.99999999999, "9.99999999999", doubleSchema));
    try {
      Assert.assertFalse(function.evaluate(9.999, "val", doubleSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.LesserThanOrEqualsFunction();
    Assert.assertTrue(function.evaluate(999999, "99999999", intSchema));
    Assert.assertTrue(function.evaluate(999999, "999999", intSchema));
    Assert.assertFalse(function.evaluate(999999999999L, "999999", longSchema));
    try {
      Assert.assertFalse(function.evaluate(9999, "val", intSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.NumberBetweenFunction();
    Assert.assertTrue(function.evaluate(999999, "99999|9999999", intSchema));
    Assert.assertTrue(function.evaluate(999999, "999999|9999999", intSchema));
    Assert.assertTrue(function.evaluate(999999, "9999|999999", intSchema));
    Assert.assertFalse(function.evaluate(999999, "9999999|99999999", intSchema));
    try {
      Assert.assertFalse(function.evaluate(9999, "9999", intSchema));
      Assert.fail("Expected function to fail when only one of lower and upper bound is specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate(9999, "9999|99999|99999", intSchema));
      Assert.fail("Expected function to fail when more than lower and upper bound is specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate(9999, "val1|val2", intSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.NumberNotBetweenFunction();
    Assert.assertTrue(function.evaluate(3.14f, "3.11|3.13", floatSchema));
    Assert.assertFalse(function.evaluate(3.14f, "3.12|3.14", floatSchema));
    Assert.assertFalse(function.evaluate(999999, "9999|999999", intSchema));
    Assert.assertFalse(function.evaluate(999999, "9999|99999999", intSchema));
    try {
      Assert.assertFalse(function.evaluate(3.14, "3.14", doubleSchema));
      Assert.fail("Expected function to fail when only one of lower and upper bound is specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate(3.14, "9999|99999|99999", doubleSchema));
      Assert.fail("Expected function to fail when more than lower and upper bound is specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate(9999, "val1|val2", intSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testNumericFunctionsWithDecimals() {
    String decimalText = "9999999999.99998";
    String lower = "9999999999.99997";
    String upper = "9999999999.99999";
    BigDecimal decimal = new BigDecimal(decimalText);
    BasicRoutingFunction function = new BasicRoutingFunctions.NumberEqualsFunction();
    Schema decimalSchema = Schema.decimalOf(20, 5);
    Assert.assertTrue(function.evaluate(decimal, decimalText, decimalSchema));
    Assert.assertFalse(function.evaluate(decimal, lower, decimalSchema));
    try {
      Assert.assertFalse(function.evaluate(decimal, "val", decimalSchema));
      Assert.fail("Expected function to fail for non-numeric value");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.NumberBetweenFunction();
    Assert.assertTrue(function.evaluate(decimal, String.format("%s|%s", lower, upper), decimalSchema));
    Assert.assertTrue(function.evaluate(decimal, String.format("%s|%s", lower, decimalText), decimalSchema));
    Assert.assertFalse(function.evaluate(decimal, String.format("%s|%s", upper, upper), decimalSchema));
  }

  @Test
  public void testDateFunctions() {
    // Epoch 1588575800 = LocalDateTime 2020-05-04T00:03:20
    LocalDate date = LocalDate.of(2020, Month.MAY, 4);
    BasicRoutingFunction function = new BasicRoutingFunctions.DateEqualsFunction();
    Schema dateSchema = Schema.of(Schema.LogicalType.DATE);
    Assert.assertTrue(function.evaluate(date, "2020-05-04", dateSchema));
    Assert.assertFalse(function.evaluate(date, "2020-05-02", dateSchema));
    try {
      Assert.assertFalse(function.evaluate(date, "20200502", dateSchema));
      Assert.fail("Should have failed for a non-ISO-8601 date format");
    } catch (DateTimeParseException e) {
      // expected
    }
    function = new BasicRoutingFunctions.DateNotEqualsFunction();
    LocalTime time = LocalTime.of(0, 3, 20);
    Schema timeMillisSchema = Schema.of(Schema.LogicalType.TIME_MILLIS);
    Assert.assertTrue(function.evaluate(time, "10:15:30", timeMillisSchema));
    Assert.assertFalse(function.evaluate(time, "00:03:20", timeMillisSchema));
    try {
      Assert.assertFalse(function.evaluate(time, "00:03:20432", timeMillisSchema));
      Assert.fail("Should have failed for a non-ISO-8601 date format");
    } catch (DateTimeParseException e) {
      // expected
    }
    function = new BasicRoutingFunctions.DateAfterFunction();
    Schema timeMicrosSchema = Schema.of(Schema.LogicalType.TIME_MICROS);
    Assert.assertTrue(function.evaluate(time, "00:00:00", timeMicrosSchema));
    Assert.assertFalse(function.evaluate(time, "00:03:20", timeMicrosSchema));
    Assert.assertFalse(function.evaluate(time, "00:04:20", timeMicrosSchema));
    try {
      Assert.assertFalse(function.evaluate(time, "00:0432:20", timeMicrosSchema));
      Assert.fail("Should have failed for a non-ISO-8601 date format");
    } catch (DateTimeParseException e) {
      // expected
    }
    function = new BasicRoutingFunctions.DateAfterOrOnFunction();
    ZonedDateTime zonedDateTime = ZonedDateTime.of(date, time, ZoneId.systemDefault());
    Schema timestampMillisSchema = Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
    Assert.assertTrue(function.evaluate(zonedDateTime, "2020-05-02T00:03:20-07:00[America/Los_Angeles]",
                                        timestampMillisSchema));
    Assert.assertTrue(function.evaluate(zonedDateTime, "2020-05-04T00:02:20-07:00[America/Los_Angeles]",
                                        timestampMillisSchema));
    Assert.assertTrue(function.evaluate(zonedDateTime, "2020-05-04T00:03:20-07:00[America/Los_Angeles]",
                                        timestampMillisSchema));
    Assert.assertFalse(function.evaluate(zonedDateTime, "2020-05-04T00:03:21-07:00[America/Los_Angeles]",
                                         timestampMillisSchema));
    Assert.assertFalse(function.evaluate(zonedDateTime, "2020-05-05T00:03:21-07:00[America/Los_Angeles]",
                                         timestampMillisSchema));
    try {
      Assert.assertFalse(function.evaluate(zonedDateTime, "20200502000320-0700", timestampMillisSchema));
      Assert.fail("Should have failed for a non-ISO-8601 date format");
    } catch (DateTimeParseException e) {
      // expected
    }
    function = new BasicRoutingFunctions.DateBeforeFunction();
    Schema timestampMicrosSchema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
    Assert.assertTrue(function.evaluate(zonedDateTime, "2020-05-04T00:03:21-07:00[America/Los_Angeles]",
                                        timestampMicrosSchema));
    Assert.assertFalse(function.evaluate(zonedDateTime, "2020-05-04T00:03:20-07:00[America/Los_Angeles]",
                                         timestampMicrosSchema));
    Assert.assertFalse(function.evaluate(zonedDateTime, "2020-05-02T00:03:19-07:00[America/Los_Angeles]",
                                         timestampMicrosSchema));
    try {
      Assert.assertFalse(function.evaluate(zonedDateTime, "20200502000320-0700", timestampMicrosSchema));
      Assert.fail("Should have failed for a non-ISO-8601 date format");
    } catch (DateTimeParseException e) {
      // expected
    }
    function = new BasicRoutingFunctions.DateBeforeOrOnFunction();
    Assert.assertTrue(function.evaluate(date, "2020-05-05", dateSchema));
    Assert.assertTrue(function.evaluate(date, "2020-05-04", dateSchema));
    Assert.assertFalse(function.evaluate(date, "2020-05-02", dateSchema));
    try {
      Assert.assertFalse(function.evaluate(date, "20200502", dateSchema));
      Assert.fail("Should have failed for a non-ISO-8601 date format");
    } catch (DateTimeParseException e) {
      // expected
    }
    function = new BasicRoutingFunctions.DateBetweenFunction();
    Assert.assertTrue(function.evaluate(date, "2020-05-03|2020-05-05", dateSchema));
    Assert.assertTrue(function.evaluate(date, "2020-05-04|2020-05-04", dateSchema));
    Assert.assertFalse(function.evaluate(date, "2020-05-02|2020-05-03", dateSchema));
    try {
      Assert.assertFalse(function.evaluate(date, "2020-05-02", dateSchema));
      Assert.fail("Should have failed because only lower bound was specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate(date, "2020-05-02|2020-05-05|2020-05-08", dateSchema));
      Assert.fail("Should have failed because 3 bounds were specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    function = new BasicRoutingFunctions.DateNotBetweenFunction();
    Assert.assertTrue(function.evaluate(date, "2020-05-05|2020-05-06", dateSchema));
    Assert.assertFalse(function.evaluate(date, "2020-05-04|2020-05-04", dateSchema));
    Assert.assertFalse(function.evaluate(date, "2020-05-02|2020-05-05", dateSchema));
    try {
      Assert.assertFalse(function.evaluate(date, "2020-05-02", dateSchema));
      Assert.fail("Should have failed because only lower bound was specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      Assert.assertFalse(function.evaluate(date, "2020-05-02|2020-05-05|2020-05-08", dateSchema));
      Assert.fail("Should have failed because 3 bounds were specified");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
