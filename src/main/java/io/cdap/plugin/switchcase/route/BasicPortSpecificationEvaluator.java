/*
 *  Copyright © 2020 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.switchcase.route;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link PortSpecificationEvaluator} that evaluates port specifications in the basic routing mode
 */
public class BasicPortSpecificationEvaluator implements PortSpecificationEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(BasicPortSpecificationEvaluator.class);

  private final String routingField;
  private final List<BasicPortSpecification> portSpecifications;

  BasicPortSpecificationEvaluator(String routingField, String portSpecification, FailureCollector collector) {
    this.routingField = routingField;
    this.portSpecifications = parse(portSpecification, collector);
  }

  @Override
  public List<String> getAllPorts() {
    List<String> ports = new ArrayList<>();
    for (BasicPortSpecification specification : portSpecifications) {
      ports.add(specification.getName());
    }
    return ports;
  }

  @Override
  public String getPort(StructuredRecord record) throws PortNotSpecifiedException {
    Object value = getRoutingFieldValue(record);
    Schema schema = getRoutingFieldSchema(record);
    for (BasicPortSpecification portSpecification : portSpecifications) {
      String portName = portSpecification.getName();
      BasicRoutingFunction routingFunction = portSpecification.getRoutingFunction();
      if (routingFunction.evaluate(value, portSpecification.getParameter(), schema)) {
        return portName;
      }
    }

    throw new PortNotSpecifiedException(PortNotSpecifiedException.Reason.DEFAULT);
  }

  private Schema getRoutingFieldSchema(StructuredRecord record) {
    Schema.Field field = record.getSchema().getField(routingField);
    if (field == null) {
      // this should never happen, since validation ensures that the routing field exists in the schema
      throw new IllegalArgumentException(String.format("Routing field %s not found in input schema", routingField));
    }
    return field.getSchema();
  }

  private Object getRoutingFieldValue(StructuredRecord record) throws PortNotSpecifiedException {
    Object returnValue = record.get(routingField);
    if (returnValue == null) {
      LOG.debug("Found null value for routing field {}.", routingField);
      throw new PortNotSpecifiedException(PortNotSpecifiedException.Reason.NULL);
    }
    Schema schema = getRoutingFieldSchema(record);
    Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
    switch (type) {
      case STRING:
      case FLOAT:
      case DOUBLE:
        return returnValue;
      case INT:
      case LONG:
      case BYTES:
        // INT can be INT, DATE, TIME_MILLIS
        // LONG can be LONG, TIMESTAMP_MILLIS, TIMESTAMP_MACROS, TIME_MACROS
        // BYTES can be decimal
        Schema.LogicalType logicalType = schema.getLogicalType();
        if (logicalType == null) {
          return returnValue;
        }
        return getLogicalValue(record, logicalType);
      default:
        // this should never happen since validation already validates the data types
        throw new IllegalArgumentException(
          String.format("Unsupported data type %s for routing field %s.", type, routingField)
        );
    }
  }

  private Object getLogicalValue(StructuredRecord record, Schema.LogicalType logicalType) {
    switch (logicalType) {
      case DATE:
        return record.getDate(routingField);
      case TIME_MICROS:
      case TIME_MILLIS:
        return record.getTime(routingField);
      case TIMESTAMP_MICROS:
      case TIMESTAMP_MILLIS:
        return record.getTimestamp(routingField);
      case DECIMAL:
        return record.getDecimal(routingField);
      default:
        throw new IllegalArgumentException("Unsupported logical type " + logicalType);
    }
  }

  /**
   * TODO: PLUGIN-153 Copied from GroupByAggregator. Extract to common method.
   */
  private static List<BasicPortSpecification> parse(String portSpecification, FailureCollector collector) {
    List<BasicPortSpecification> portSpecifications = new ArrayList<>();
    Set<String> portNames = new HashSet<>();
    if (Strings.isNullOrEmpty(portSpecification)) {
      collector.addFailure(
        "Could not find any port specifications.", "At least one port specification must be provided."
      ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      throw collector.getOrThrowException();
    }
    for (String singlePortSpec : Splitter.on(',').trimResults().split(portSpecification)) {
      int colonIdx = singlePortSpec.indexOf(':');
      if (colonIdx < 0) {
        collector.addFailure(
          String.format(
            "Could not find ':' separating port name from its routing specification in '%s'.", singlePortSpec
          ), "The configuration for each port should contain a port name and its routing specification separated by :."
        ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }
      String portName = singlePortSpec.substring(0, colonIdx).trim();
      if (!portNames.add(portName)) {
        collector.addFailure(
          String.format("Cannot create multiple ports with the same name '%s'.", portName),
          "Please specify a unique port name for each specification."
        ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }

      String functionAndParameter = singlePortSpec.substring(colonIdx + 1).trim();
      int leftParanIdx = functionAndParameter.indexOf('(');
      if (leftParanIdx < 0) {
        collector.addFailure(
          String.format("Could not find '(' in function '%s'. ", functionAndParameter),
          "Please specify routing functions as as function(parameter)."
        ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }
      String functionStr = functionAndParameter.substring(0, leftParanIdx).trim();
      Router.Config.FunctionType function;
      try {
        function = Router.Config.FunctionType.valueOf(functionStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        collector.addFailure(
          String.format("Invalid routing function '%s'.", functionStr),
          String.format(
            "A routing function must be  one of %s.", Joiner.on(',').join(Router.Config.FunctionType.values())
          )
        ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
        throw collector.getOrThrowException();
      }

      if (!functionAndParameter.endsWith(")")) {
        collector.addFailure(
          String.format("Could not find closing ')' in function '%s'.", functionAndParameter),
          "Functions must be specified as function(parameter)."
        ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }
      String parameter = functionAndParameter.substring(leftParanIdx + 1, functionAndParameter.length() - 1).trim();
      if (parameter.isEmpty()) {
        collector.addFailure(
          String.format("Invalid function '%s'.", functionAndParameter),
          "A parameter must be provided as an argument."
        ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }

      LOG.debug("Adding port config: name = {}; function = {}; parameter = {}", portName, function, parameter);
      portSpecifications.add(new BasicPortSpecification(portName, function, parameter, collector));
    }

    if (portSpecifications.isEmpty()) {
      collector.addFailure(
        String.format("'%s' property cannot be empty.", Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME),
        "At least 1 port specification must be provided."
      ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
    }
    return portSpecifications;
  }
}
