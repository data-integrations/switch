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

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import org.mvel2.MVEL;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link PortSpecificationEvaluator} that evaluates port specifications in the MVEL routing mode
 */
final class MVELPortSpecificationEvaluator implements PortSpecificationEvaluator {

  private final List<MVELPortSpecification> mvelPortSpecifications;

  MVELPortSpecificationEvaluator(String mvelPortSpecification, Schema inputSchema, FailureCollector collector) {
    this.mvelPortSpecifications = parse(mvelPortSpecification, inputSchema, collector);
  }

  @Override
  public List<String> getAllPorts() {
    List<String> ports = new ArrayList<>();
    for (MVELPortSpecification portSpecification : mvelPortSpecifications) {
      ports.add(portSpecification.getName());
    }
    return ports;
  }

  @Override
  public String getPort(StructuredRecord record) throws PortNotSpecifiedException {
    List<Schema.Field> fields = record.getSchema().getFields();
    if (fields == null) {
      throw new IllegalArgumentException("Input record did not contain any fields.");
    }
    Map<String, Object> variables = new HashMap<>();
    for (Schema.Field field : fields) {
      String variableName = field.getName();
      variables.put(variableName, record.get(variableName));
    }
    for (MVELPortSpecification portSpecification : mvelPortSpecifications) {
      Boolean selectPort = MVEL.executeExpression(portSpecification.getExpression(), variables, Boolean.class);
      if (selectPort) {
        return portSpecification.getName();
      }
    }
    throw new PortNotSpecifiedException(PortNotSpecifiedException.Reason.DEFAULT);
  }

  private List<MVELPortSpecification> parse(String mvelPortSpecification, Schema inputSchema,
                                            FailureCollector collector) {
    List<MVELPortSpecification> portSpecifications = new ArrayList<>();
    Set<String> portNames = new HashSet<>();
    if (Strings.isNullOrEmpty(mvelPortSpecification)) {
      collector.addFailure(
        "Could not find any port specifications.", "At least one port specification must be provided."
      ).withConfigProperty(Router.Config.MVEL_PORT_SPECIFICATION_PROPERTY_NAME);
      throw collector.getOrThrowException();
    }
    for (String singlePortSpec : Splitter.on(',').trimResults().split(mvelPortSpecification)) {
      int colonIdx = singlePortSpec.indexOf(':');
      if (colonIdx < 0) {
        collector.addFailure(
          String.format(
            "Could not find ':' separating port name from its MVEL routing expression in '%s'.", singlePortSpec
          ), "The configuration for each port should contain a port name and its MVEL routing expression separated " +
            "by :."
        ).withConfigProperty(Router.Config.MVEL_PORT_SPECIFICATION_PROPERTY_NAME);
      }
      String portName;
      try {
        portName = URLDecoder.decode(singlePortSpec.substring(0, colonIdx).trim(), Charsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        // This should never happen
        throw new IllegalStateException("Unsupported encoding when trying to decode port name.", e);
      }
      if (!portNames.add(portName)) {
        collector.addFailure(
          String.format("Cannot create multiple ports with the same name '%s'.", portName),
          "Please specify a unique port name for each specification."
        ).withConfigProperty(Router.Config.MVEL_PORT_SPECIFICATION_PROPERTY_NAME);
      }

      String expression;
      try {
        expression = URLDecoder.decode(singlePortSpec.substring(colonIdx + 1).trim(), Charsets.UTF_8.name());
        portSpecifications.add(new MVELPortSpecification(portName, expression, inputSchema, collector));
      } catch (UnsupportedEncodingException e) {
        collector.addFailure(
          String.format("Could not decode MVEL expression. Please make sure it is encoded correctly. Message: %s.",
                        e.getMessage()),
          "Please encode the expression correctly."
        ).withConfigProperty(Router.Config.MVEL_PORT_SPECIFICATION_PROPERTY_NAME)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (portSpecifications.isEmpty()) {
      collector.addFailure("At least 1 port specification must be provided.", null)
        .withConfigProperty(Router.Config.MVEL_PORT_SPECIFICATION_PROPERTY_NAME);
    }
    return portSpecifications;
  }
}
