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
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockMultiOutputEmitter;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Tests for {@link RoutingSwitch} that apply to all {@link PortSpecification port specifications}
 */
public abstract class RoutingSwitchTest {
  static final Schema INPUT =
    Schema.recordOf("input",
                    Schema.Field.of("supplier_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("part_id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("count", Schema.of(Schema.Type.INT)));
  static final String PORT_SPECIFICATION = "a_port:equals(10)";
  static final Schema ALL_TYPES_SCHEMA = Schema.recordOf("input",
                                                         Schema.Field.of("supplier_id", Schema.of(Schema.Type.STRING)),
                                                         Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                                                         Schema.Field.of("int", Schema.of(Schema.Type.INT)),
                                                         Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
                                                         Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
                                                         Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
                                                         Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
                                                         Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
                                                         Schema.Field.of(
                                                           "union",
                                                           Schema.unionOf(
                                                             Schema.of(Schema.Type.INT), Schema.of(Schema.Type.BOOLEAN)
                                                           )
                                                         ),
                                                         Schema.Field.of("enum", Schema.enumWith("ENUM1", "ENUM2")),
                                                         Schema.Field.of(
                                                           "map",
                                                           Schema.mapOf(
                                                             Schema.of(Schema.Type.STRING),
                                                             Schema.of(Schema.Type.STRING)
                                                           )
                                                         ),
                                                         Schema.Field.of(
                                                           "array", Schema.arrayOf(Schema.of(Schema.Type.INT))
                                                         ),
                                                         Schema.Field.of(
                                                           "record",
                                                           Schema.recordOf(
                                                             "record", Objects.requireNonNull(INPUT.getFields())
                                                           )
                                                         ));

  // TODO: Add an abstract method getMode(), so test can be executed for all modes after adding jexl support

  @Test
  public void testDefaultedRecordToErrorByDefault() throws Exception {
    testDefaultedRecord(null, null);
  }

  @Test
  public void testDefaultedRecordToError() throws Exception {
    testDefaultedRecord(RoutingSwitch.Config.DefaultHandling.ERROR_PORT.value(), null);
  }

  @Test
  public void testDefaultedRecordToDefaultDefaultPort() throws Exception {
    testDefaultedRecordToDefaultPort(null);
  }

  @Test
  public void testDefaultedRecordToDefaultPort() throws Exception {
    testDefaultedRecordToDefaultPort("Custom Default Port");
  }

  @Test
  public void testDefaultedRecordSkipped() throws Exception {
    testDefaultedRecord(RoutingSwitch.Config.DefaultHandling.SKIP.value(), null);
  }

  @Test
  public void testNullRecordSkipped() throws Exception {
    testDefaultedRecord(RoutingSwitch.Config.NullHandling.SKIP.value(), null);
  }

  @Test
  public void testRouting() throws Exception {
    StructuredRecord testRecord1 = StructuredRecord.builder(INPUT)
      .set("supplier_id", "supplier1")
      .set("part_id", "1")
      .set("count", "10")
      .build();

    StructuredRecord testRecord2 = StructuredRecord.builder(INPUT)
      .set("supplier_id", "supplier2")
      .set("part_id", "2")
      .set("count", "20")
      .build();

    StructuredRecord testRecord3 = StructuredRecord.builder(INPUT)
      .set("supplier_id", "supplier3")
      .set("part_id", "3")
      .set("count", "30")
      .build();

    String portSpecification = "Supplier 1:equals(supplier1),Supplier 2:equals(supplier2),Supplier 3:equals(supplier3)";
    RoutingSwitch.Config config = new RoutingSwitch.Config(
      "supplier_id", portSpecification, RoutingSwitch.Config.DefaultHandling.DEFAULT_PORT.value(), "Default Port",
      RoutingSwitch.Config.NullHandling.NULL_PORT.value(), "Null Port"
    );
    SplitterTransform<StructuredRecord, StructuredRecord> routingSwitch = new RoutingSwitch(config);
    routingSwitch.initialize(new MockTransformContext());

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();

    routingSwitch.transform(testRecord1, emitter);
    routingSwitch.transform(testRecord2, emitter);
    routingSwitch.transform(testRecord3, emitter);

    Assert.assertEquals(testRecord1, emitter.getEmitted().get("Supplier 1").get(0));
    Assert.assertEquals(testRecord2, emitter.getEmitted().get("Supplier 2").get(0));
    Assert.assertEquals(testRecord3, emitter.getEmitted().get("Supplier 3").get(0));
  }

  @Test
  public void testAllowedTypes() {
    Assert.assertTrue(validateType("string").isEmpty());
    Assert.assertTrue(validateType("int").isEmpty());
    Assert.assertTrue(validateType("long").isEmpty());
    Assert.assertTrue(validateType("float").isEmpty());
    Assert.assertTrue(validateType("double").isEmpty());
    Assert.assertTrue(validateType("bytes").isEmpty());
    Assert.assertEquals(1, validateType("boolean").size());
    Assert.assertEquals(1, validateType("union").size());
    Assert.assertEquals(1, validateType("enum").size());
    Assert.assertEquals(1, validateType("map").size());
    Assert.assertEquals(1, validateType("array").size());
    Assert.assertEquals(1, validateType("record").size());
  }

  private void testDefaultedRecordToDefaultPort(@Nullable String defaultPortName) throws Exception {
    testDefaultedRecord(RoutingSwitch.Config.DefaultHandling.DEFAULT_PORT.value(), defaultPortName);
  }

  private void testDefaultedRecord(@Nullable String defaultHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", "1")
      .set("part_id", "2")
      .set("count", "3")
      .build();

    RoutingSwitch.Config config = new RoutingSwitch.Config("supplier_id", PORT_SPECIFICATION,
                                                           defaultHandling, outputPortName,
                                                           null, null);
    SplitterTransform<StructuredRecord, StructuredRecord> routingSwitch = new RoutingSwitch(config);
    routingSwitch.initialize(new MockTransformContext());

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    routingSwitch.transform(testRecord, emitter);
    StructuredRecord record;
    if (defaultHandling == null) {
      defaultHandling = config.getDefaultHandling().value();
    }
    if (RoutingSwitch.Config.DefaultHandling.ERROR_PORT.value().equalsIgnoreCase(defaultHandling)) {
      InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
      record = invalidEntry.getInvalidRecord();
    } else {
      outputPortName = outputPortName == null ? RoutingSwitch.Config.DEFAULT_PORT_NAME : outputPortName;
      List<Object> objects = emitter.getEmitted().get(outputPortName);
      if (RoutingSwitch.Config.DefaultHandling.SKIP.value().equalsIgnoreCase(defaultHandling)) {
        Assert.assertNull(objects);
        return;
      }
      record = (StructuredRecord) objects.get(0);
    }
    Assert.assertEquals(testRecord, record);
  }

  private List<ValidationFailure> validateType(String fieldName) {
    String portSpecification = "Supplier 1:equals(supplier1)";
    RoutingSwitch.Config config = new RoutingSwitch.Config(fieldName, portSpecification, null, null, null, null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(ALL_TYPES_SCHEMA, collector);
    return collector.getValidationFailures();
  }
}
