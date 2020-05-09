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
import io.cdap.cdap.etl.mock.common.MockMultiOutputEmitter;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Tests for {@link Router} that apply to all {@link PortSpecification port specifications}
 */
public abstract class RouterTest {
  static final Schema INPUT =
    Schema.recordOf("input",
                    Schema.Field.of("supplier_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("part_id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("count", Schema.of(Schema.Type.INT)));
  static final String PORT_SPECIFICATION = "a_port:equals(10)";
  static final String MVEL_PORT_SPECIFICATION = "a_port:supplier_id.equals(\"10\")";
  static final Schema ALL_TYPES_SCHEMA =
    Schema.recordOf("input",
                    Schema.Field.of("supplier_id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
                    Schema.Field.of("float_field", Schema.of(Schema.Type.FLOAT)),
                    Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
                    Schema.Field.of("double_field", Schema.of(Schema.Type.DOUBLE)),
                    Schema.Field.of("boolean_field", Schema.of(Schema.Type.BOOLEAN)),
                    Schema.Field.of("bytes_field", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of(
                      "union_field",
                      Schema.unionOf(
                        Schema.of(Schema.Type.INT), Schema.of(Schema.Type.BOOLEAN)
                      )
                    ),
                    Schema.Field.of("enum_field", Schema.enumWith("ENUM1", "ENUM2")),
                    Schema.Field.of(
                      "map_field",
                      Schema.mapOf(
                        Schema.of(Schema.Type.STRING),
                        Schema.of(Schema.Type.STRING)
                      )
                    ),
                    Schema.Field.of("array_field", Schema.arrayOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of(
                      "record_field",
                      Schema.recordOf(
                        "record", Objects.requireNonNull(INPUT.getFields())
                      )
                    ),
                    Schema.Field.of("date_field", Schema.of(Schema.LogicalType.DATE)),
                    Schema.Field.of("time_millis_field", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                    Schema.Field.of("time_micros_field", Schema.of(Schema.LogicalType.TIME_MICROS)),
                    Schema.Field.of("timestamp_millis_field", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                    Schema.Field.of("timestamp_micros_field", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("decimal_field", Schema.decimalOf(10, 3)));
  static StructuredRecord allTypesRecord;

  public abstract String getMode();

  @BeforeClass
  public static void setup() {
    StructuredRecord inner = StructuredRecord.builder(INPUT)
      .set("supplier_id", "1")
      .set("part_id", "2")
      .set("count", 3)
      .build();

    LocalDate testDate = LocalDate.of(2020, 5, 4);
    LocalTime testTimeMillis = LocalTime.of(1, 2, 3);
    LocalTime testTimeMicros = LocalTime.of(4, 5, 6);
    allTypesRecord = StructuredRecord.builder(ALL_TYPES_SCHEMA)
      .set("supplier_id", "supplier1")
      .set("string_field", "test")
      .set("int_field", 1)
      .set("long_field", 2L)
      .set("float_field", 3.14f)
      .set("double_field", 3.14)
      .set("boolean_field", true)
      .set("bytes_field", "bytes".getBytes(Charset.defaultCharset()))
      .set("union_field", 3)
      .set("enum_field", "ENUM1")
      .set("map_field", Collections.EMPTY_MAP)
      .set("array_field", new int[] {4, 5})
      .set("record_field", inner)
      .setDate("date_field", testDate)
      .setTime("time_millis_field", testTimeMillis)
      .setTime("time_micros_field", testTimeMicros)
      .setTimestamp("timestamp_millis_field", ZonedDateTime.of(testDate, testTimeMillis, ZoneId.systemDefault()))
      .setTimestamp("timestamp_micros_field", ZonedDateTime.of(testDate, testTimeMicros, ZoneId.systemDefault()))
      .setDecimal("decimal_field", new BigDecimal("9999999.998"))
      .build();
  }

  @Test
  public void testDefaultedRecordToErrorByDefault() throws Exception {
    testDefaultedRecord(null, null);
  }

  @Test
  public void testDefaultedRecordToError() throws Exception {
    testDefaultedRecord(Router.Config.DefaultHandling.ERROR_PORT.value(), null);
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
    testDefaultedRecord(Router.Config.DefaultHandling.SKIP.value(), null);
  }

  @Test
  public void testNullRecordSkipped() throws Exception {
    testDefaultedRecord(Router.Config.NullHandling.SKIP.value(), null);
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
    String mvelPortSpecification = "Supplier 1:supplier_id.equals(\"supplier1\")," +
      "Supplier 2:supplier_id.equals(\"supplier2\")," +
      "Supplier 3:supplier_id.equals(\"supplier3\")";
    Router.Config config = new Router.Config(
      getMode(), "supplier_id", portSpecification, mvelPortSpecification,
      Router.Config.DefaultHandling.DEFAULT_PORT.value(), "Default Port",
      Router.Config.NullHandling.NULL_PORT.value(), "Null Port"
    );
    SplitterTransform<StructuredRecord, StructuredRecord> router = new Router(config);
    router.initialize(new MockTransformContext() {
      @Override
      public Schema getInputSchema() {
        return INPUT;
      }
    });

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();

    router.transform(testRecord1, emitter);
    router.transform(testRecord2, emitter);
    router.transform(testRecord3, emitter);

    Assert.assertEquals(testRecord1, emitter.getEmitted().get("Supplier 1").get(0));
    Assert.assertEquals(testRecord2, emitter.getEmitted().get("Supplier 2").get(0));
    Assert.assertEquals(testRecord3, emitter.getEmitted().get("Supplier 3").get(0));
  }

  private void testDefaultedRecordToDefaultPort(@Nullable String defaultPortName) throws Exception {
    testDefaultedRecord(Router.Config.DefaultHandling.DEFAULT_PORT.value(), defaultPortName);
  }

  private void testDefaultedRecord(@Nullable String defaultHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", "1")
      .set("part_id", "2")
      .set("count", "3")
      .build();

    Router.Config config = new Router.Config(getMode(), "supplier_id", PORT_SPECIFICATION, MVEL_PORT_SPECIFICATION,
                                             defaultHandling, outputPortName, null, null);
    SplitterTransform<StructuredRecord, StructuredRecord> router = new Router(config);
    router.initialize(new MockTransformContext() {
      @Override
      public Schema getInputSchema() {
        return INPUT;
      }
    });

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    router.transform(testRecord, emitter);
    StructuredRecord record;
    if (defaultHandling == null) {
      defaultHandling = config.getDefaultHandling().value();
    }
    if (Router.Config.DefaultHandling.ERROR_PORT.value().equalsIgnoreCase(defaultHandling)) {
      InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
      record = invalidEntry.getInvalidRecord();
    } else {
      outputPortName = outputPortName == null ? Router.Config.DEFAULT_PORT_NAME : outputPortName;
      List<Object> objects = emitter.getEmitted().get(outputPortName);
      if (Router.Config.DefaultHandling.SKIP.value().equalsIgnoreCase(defaultHandling)) {
        Assert.assertNull(objects);
        return;
      }
      record = (StructuredRecord) objects.get(0);
    }
    Assert.assertEquals(testRecord, record);
  }
}
