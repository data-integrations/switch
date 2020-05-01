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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.cdap.etl.api.TransformContext;
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
 * Tests for {@link RoutingSwitch} with {@link BasicPortSpecification}
 */
public class BasicRoutingSwitchTest extends RoutingSwitchTest {

  @Test
  public void testNullRecordToErrorByDefault() throws Exception {
    testNullRecord(null, null);
  }

  @Test
  public void testNullRecordToError() throws Exception {
    testNullRecord(RoutingSwitch.Config.NullHandling.ERROR_PORT.value(), null);
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
    try {
      validateFunction("blah");
      Assert.fail("Validation should fail for invalid function");
    } catch (ValidationException e) {
      // expected
    }
  }

  private void testNullRecordToNullPort(@Nullable String nullPortName) throws Exception {
    testNullRecord(RoutingSwitch.Config.NullHandling.NULL_PORT.value(), nullPortName);
  }

  private void testNullRecord(@Nullable String nullHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", null)
      .set("part_id", "2")
      .set("count", "3")
      .build();

    RoutingSwitch.Config config = new RoutingSwitch.Config("supplier_id", PORT_SPECIFICATION, null, null,
                                                           nullHandling, outputPortName);
    SplitterTransform<StructuredRecord, StructuredRecord> routingSwitch = new RoutingSwitch(config);
    routingSwitch.initialize(new MockTransformContext());

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    routingSwitch.transform(testRecord, emitter);

    StructuredRecord record;
    if (nullHandling == null) {
      nullHandling = config.getNullHandling().value();
    }
    if (RoutingSwitch.Config.NullHandling.ERROR_PORT.value().equalsIgnoreCase(nullHandling)) {
      InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
      record = invalidEntry.getInvalidRecord();
    } else {
      outputPortName = outputPortName == null ? RoutingSwitch.Config.DEFAULT_NULL_PORT_NAME : outputPortName;
      List<Object> objects = emitter.getEmitted().get(outputPortName);
      if (RoutingSwitch.Config.DefaultHandling.SKIP.value().equalsIgnoreCase(nullHandling)) {
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

    RoutingSwitch.Config config = new RoutingSwitch.Config(
      "supplier_id", portSpecification, null, null, null, null
    );
    SplitterTransform<StructuredRecord, StructuredRecord> routingSwitch = new RoutingSwitch(config);
    routingSwitch.initialize(new MockTransformContext());

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    routingSwitch.transform(testRecord, emitter);

    List<Object> objects = emitter.getEmitted().get(portToRouteTo);
    StructuredRecord record = (StructuredRecord) objects.get(0);
    Assert.assertEquals(testRecord, record);
    objects = emitter.getEmitted().get(portToNotRouteTo);
    Assert.assertNull(objects);
  }

  private List<ValidationFailure> validateFunction(String functionName) {
    String portSpecification = String.format("Supplier 1:%s(supplier1)", functionName);
    RoutingSwitch.Config config = new RoutingSwitch.Config("supplier_id", portSpecification, null, null, null, null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(INPUT, collector);
    return collector.getValidationFailures();
  }
}
