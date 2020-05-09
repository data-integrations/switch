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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockMultiOutputEmitter;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Tests for {@link Router} with {@link BasicPortSpecification}
 */
public class BasicRouterTest extends RouterTest {

  @Test
  public void testNullRecordToErrorByDefault() throws Exception {
    testNullRecord(null, null);
  }

  @Test
  public void testNullRecordToError() throws Exception {
    testNullRecord(Router.Config.NullHandling.ERROR_PORT.value(), null);
  }

  @Test
  public void testNullRecordToNullPort() throws Exception {
    testNullRecordToNullPort("null");
  }

  @Test
  public void testNullRecordToDefaultNullPort() throws Exception {
    testNullRecordToNullPort(null);
  }

  @Test
  public void testEqualsFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:equals(supplierA),portB:equals(supplierB)");
  }

  @Test
  public void testNotEqualsFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_equals(supplierA),portB:not_equals(supplierB)");
  }

  @Test
  public void testContainsFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:contains(plier),portB:contains(flier)");
  }

  @Test
  public void testNotContainsFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_contains(plier),portB:not_contains(flier)");
  }

  @Test
  public void testInFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:in(supplierA|supplierB),portB:in(suppA|suppB)");
  }

  @Test
  public void testNotInFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_in(supplierA|supplierB),portB:not_in(s|suppB)");
  }

  @Test
  public void testMatchesFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:matches(.*plierA$),portB:matches(.*flierA$)");
  }

  @Test
  public void testNotMatchesFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_matches(.*plierA$),portB:not_matches(.*xyz$)");
  }

  @Test
  public void testStartsWithFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:starts_with(sup),portB:starts_with(upp)");
  }

  @Test
  public void testNotStartsWithFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_starts_with(sup),portB:not_starts_with(upp)");
  }

  @Test
  public void testEndsWithFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:ends_with(lierA),portB:ends_with(lier)");
  }

  @Test
  public void testNotEndsWithFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_ends_with(lierA),portB:not_ends_with(lier)");
  }

  @Test
  public void testAvailableFunctions() {
    Assert.assertTrue(validateFunction("equals").isEmpty());
    Assert.assertTrue(validateFunction("not_equals").isEmpty());
    Assert.assertTrue(validateFunction("contains").isEmpty());
    Assert.assertTrue(validateFunction("not_contains").isEmpty());
    Assert.assertTrue(validateFunction("in").isEmpty());
    Assert.assertTrue(validateFunction("not_in").isEmpty());
    Assert.assertTrue(validateFunction("matches").isEmpty());
    Assert.assertTrue(validateFunction("not_matches").isEmpty());
    Assert.assertTrue(validateFunction("starts_with").isEmpty());
    Assert.assertTrue(validateFunction("not_starts_with").isEmpty());
    Assert.assertTrue(validateFunction("ends_with").isEmpty());
    Assert.assertTrue(validateFunction("not_ends_with").isEmpty());
    Assert.assertTrue(validateFunction("number_equals").isEmpty());
    Assert.assertTrue(validateFunction("number_not_equals").isEmpty());
    Assert.assertTrue(validateFunction("number_greater_than").isEmpty());
    Assert.assertTrue(validateFunction("number_greater_than_or_equals").isEmpty());
    Assert.assertTrue(validateFunction("number_lesser_than").isEmpty());
    Assert.assertTrue(validateFunction("number_lesser_than_or_equals").isEmpty());
    Assert.assertTrue(validateFunction("number_between").isEmpty());
    Assert.assertTrue(validateFunction("number_not_between").isEmpty());
    Assert.assertTrue(validateFunction("date_equals").isEmpty());
    Assert.assertTrue(validateFunction("date_not_equals").isEmpty());
    Assert.assertTrue(validateFunction("date_after").isEmpty());
    Assert.assertTrue(validateFunction("date_after_or_on").isEmpty());
    Assert.assertTrue(validateFunction("date_before").isEmpty());
    Assert.assertTrue(validateFunction("date_before_or_on").isEmpty());
    Assert.assertTrue(validateFunction("date_between").isEmpty());
    Assert.assertTrue(validateFunction("date_not_between").isEmpty());
    try {
      validateFunction("blah");
      Assert.fail("Validation should fail for invalid function");
    } catch (ValidationException e) {
      // expected
    }
  }

  @Test
  public void testValidationOfAllowedTypes() {
    // Make sure that all supported types can be validated during design time
    Assert.assertTrue(validateType("string_field").isEmpty());
    Assert.assertTrue(validateType("int_field").isEmpty());
    Assert.assertTrue(validateType("long_field").isEmpty());
    Assert.assertTrue(validateType("float_field").isEmpty());
    Assert.assertTrue(validateType("double_field").isEmpty());
    Assert.assertTrue(validateType("bytes_field").isEmpty());
    Assert.assertTrue(validateType("date_field").isEmpty());
    Assert.assertTrue(validateType("time_millis_field").isEmpty());
    Assert.assertTrue(validateType("time_micros_field").isEmpty());
    Assert.assertTrue(validateType("timestamp_millis_field").isEmpty());
    Assert.assertTrue(validateType("timestamp_micros_field").isEmpty());
    Assert.assertEquals(1, validateType("boolean_field").size());
    Assert.assertEquals(1, validateType("union_field").size());
    Assert.assertEquals(1, validateType("enum_field").size());
    Assert.assertEquals(1, validateType("map_field").size());
    Assert.assertEquals(1, validateType("array_field").size());
    Assert.assertEquals(1, validateType("record_field").size());
  }

  @Test
  public void testExecutionOfAllowedTypes() throws Exception {
    // Make sure that all supported types can be accepted while the pipeline runs
    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    runSingleRecord(allTypesRecord, "string_field", "portA:ends_with(lierA)", emitter);
    runSingleRecord(allTypesRecord, "int_field", "portA:number_lesser_than(2)", emitter);
    runSingleRecord(allTypesRecord, "long_field", "portA:number_greater_than(1)", emitter);
    runSingleRecord(allTypesRecord, "float_field", "portA:number_greater_than_or_equals(1.3)", emitter);
    runSingleRecord(allTypesRecord, "double_field", "portA:number_lesser_than_or_equals(1.6)", emitter);
    try {
      runSingleRecord(allTypesRecord, "boolean_field", "portA:equals(true)", emitter);
      Assert.fail("Routing on boolean fields is not supported, so should fail");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      runSingleRecord(allTypesRecord, "bytes_field", "portA:equals(bytes)", emitter);
      Assert.fail("Routing on bytes fields is not supported, so should fail");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      runSingleRecord(allTypesRecord, "union_field", "portA:number_between(3|4)", emitter);
      Assert.fail("Routing on union fields is not supported, so should fail");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      runSingleRecord(allTypesRecord, "enum_field", "portA:starts_with(EN)", emitter);
      Assert.fail("Routing on enum fields is not supported, so should fail");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      runSingleRecord(allTypesRecord, "map_field", "portA:ends_with(1)", emitter);
      Assert.fail("Routing on map fields is not supported, so should fail");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      runSingleRecord(allTypesRecord, "array_field", "portA:number_not_between(1|9)", emitter);
      Assert.fail("Routing on array fields is not supported, so should fail");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      runSingleRecord(allTypesRecord, "record_field", "portA:number_lesser_than_or_equals(9)", emitter);
      Assert.fail("Routing on record fields is not supported, so should fail");
    } catch (IllegalArgumentException e) {
      // expected
    }
    runSingleRecord(allTypesRecord, "date_field", "portA:date_after_or_on(2020-05-04)", emitter);
    runSingleRecord(allTypesRecord, "time_millis_field", "portA:date_before_or_on(02:03:04)", emitter);
    runSingleRecord(allTypesRecord, "time_micros_field", "portA:date_after(00:01:02)", emitter);
    runSingleRecord(allTypesRecord, "timestamp_millis_field",
                    "portA:date_after_or_on(2020-05-02T00:03:20-07:00[America/Los_Angeles])", emitter);
    runSingleRecord(allTypesRecord, "timestamp_micros_field",
                    "portA:date_after_or_on(2020-05-02T00:03:20-07:00[America/Los_Angeles])", emitter);
    runSingleRecord(allTypesRecord, "decimal", "portA:number_between(9999999.997|9999999.999)", emitter);
  }

  private void testNullRecordToNullPort(@Nullable String nullPortName) throws Exception {
    testNullRecord(Router.Config.NullHandling.NULL_PORT.value(), nullPortName);
  }

  private void testNullRecord(@Nullable String nullHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", null)
      .set("part_id", "2")
      .set("count", "3")
      .build();

    Router.Config config = new Router.Config(getMode(), "supplier_id", PORT_SPECIFICATION, null, null, null,
                                             nullHandling, outputPortName);
    SplitterTransform<StructuredRecord, StructuredRecord> router = new Router(config);
    router.initialize(new MockTransformContext());

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    router.transform(testRecord, emitter);

    StructuredRecord record;
    if (nullHandling == null) {
      nullHandling = config.getNullHandling().value();
    }
    if (Router.Config.NullHandling.ERROR_PORT.value().equalsIgnoreCase(nullHandling)) {
      InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
      record = invalidEntry.getInvalidRecord();
    } else {
      outputPortName = outputPortName == null ? Router.Config.DEFAULT_NULL_PORT_NAME : outputPortName;
      List<Object> objects = emitter.getEmitted().get(outputPortName);
      if (Router.Config.DefaultHandling.SKIP.value().equalsIgnoreCase(nullHandling)) {
        Assert.assertNull(objects);
        return;
      }
      record = (StructuredRecord) objects.get(0);
    }
    Assert.assertEquals(testRecord, record);
  }

  private void testBasicFunction(String portToRouteTo, String portToNotRouteTo,
                                 String portSpecification) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", "supplierA")
      .set("part_id", "2")
      .set("count", "3")
      .build();

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    runSingleRecord(testRecord, "supplier_id", portSpecification, emitter);

    List<Object> objects = emitter.getEmitted().get(portToRouteTo);
    StructuredRecord record = (StructuredRecord) objects.get(0);
    Assert.assertEquals(testRecord, record);
    objects = emitter.getEmitted().get(portToNotRouteTo);
    Assert.assertNull(objects);
  }

  private void runSingleRecord(StructuredRecord record, String routingField, String portSpecification,
                               MockMultiOutputEmitter<StructuredRecord> emitter) throws Exception {
    Router.Config config = new Router.Config(
      getMode(), routingField, portSpecification, null, null, null, null, null
    );
    SplitterTransform<StructuredRecord, StructuredRecord> router = new Router(config);
    router.initialize(new MockTransformContext());
    router.transform(record, emitter);
  }

  private List<ValidationFailure> validateFunction(String functionName) {
    String portSpecification = String.format("Supplier 1:%s(supplier1)", functionName);
    Router.Config config = new Router.Config(getMode(), "supplier_id", portSpecification, null, null, null, null, null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(INPUT, collector);
    return collector.getValidationFailures();
  }

  private List<ValidationFailure> validateType(String fieldName) {
    Router.Config config = new Router.Config(getMode(), fieldName, PORT_SPECIFICATION, null, null, null, null, null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(ALL_TYPES_SCHEMA, collector);
    return collector.getValidationFailures();
  }

  @Override
  public String getMode() {
    return Router.Config.BASIC_MODE_NAME;
  }
}
