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
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockMultiOutputEmitter;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

public class MVELRouterTest extends RouterTest {
  @Override
  public String getMode() {
    return Router.Config.MVEL_MODE_NAME;
  }

  @Test
  public void testExpressionValidity() {
    Assert.assertTrue(validatePortSpec("port1:\"string\".startsWith(\"st\")").isEmpty());
    Assert.assertTrue(validatePortSpec("port1:supplier_id.equals(\"suppA\")").isEmpty());
    Assert.assertTrue(validatePortSpec("port1:((String)supplier_id).endsWith(\"lier1\")").isEmpty());
    Assert.assertTrue(validatePortSpec("port1:count.equals(100)").isEmpty());
    Assert.assertTrue(validatePortSpec("port1:Math.ceil(count) == 100").isEmpty());
    Assert.assertTrue(validatePortSpec("port1:Strings.isNullOrEmpty(part_id)").isEmpty());
    Assert.assertTrue(
      validatePortSpec("port1:Strings.isNullOrEmpty(part_id),port2:!Strings.isNullOrEmpty(part_id)").isEmpty()
    );
  }

  @Test
  public void testEndToEnd() throws Exception {
    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    runSingleRecord(
      allTypesRecord, "portA:supplier_id.endsWith(\"lier1\") && !Strings.isNullOrEmpty(string_field)", emitter
    );
    Assert.assertEquals(allTypesRecord, emitter.getEmitted().get("portA").get(0));
    emitter.clear();
    runSingleRecord(
      allTypesRecord, "portA:supplier_id.startsWith(\"supp\"),portB:float_field<4", emitter
    );
    Assert.assertEquals(allTypesRecord, emitter.getEmitted().get("portA").get(0));
    // TODO: This is the use case where record is routed to multiple ports. Support it?
    Assert.assertNull(emitter.getEmitted().get("portB"));
    emitter.clear();
    runSingleRecord(
      allTypesRecord, "portA:float_field<4", emitter
    );
    Assert.assertEquals(allTypesRecord, emitter.getEmitted().get("portA").get(0));
    // test URL Encoding
    emitter.clear();
    runSingleRecord(
      allTypesRecord,
      "portA:StringUtils.startsWith(string_field%2C\"tes\"),portB:StringUtils.startsWith(string_field%2C\"set\")",
      emitter
    );
    Assert.assertEquals(allTypesRecord, emitter.getEmitted().get("portA").get(0));
  }

  private List<ValidationFailure> validatePortSpec(String portSpec) {
    String mvelPortSpec = String.format("%s", portSpec);
    Router.Config config = new Router.Config(getMode(), null, null, mvelPortSpec, null, null, null, null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(INPUT, collector);
    return collector.getValidationFailures();
  }

  private void runSingleRecord(StructuredRecord record, String mvelPortSpec,
                               MockMultiOutputEmitter<StructuredRecord> emitter) throws Exception {
    Router.Config config = new Router.Config(getMode(), null, null, mvelPortSpec, null, null, null, null);
    SplitterTransform<StructuredRecord, StructuredRecord> router = new Router(config);
    router.initialize(new MockTransformContext() {
      @Override
      public Schema getInputSchema() {
        return ALL_TYPES_SCHEMA;
      }
    });
    router.transform(record, emitter);
  }
}
