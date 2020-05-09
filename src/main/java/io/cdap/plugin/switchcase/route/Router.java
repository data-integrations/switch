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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.MultiOutputEmitter;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiOutputStageConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * {@link SplitterTransform} to route records to appropriate ports based on conditions specified similar to a
 * SWITCH..CASE statement. 
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("Router")
@Description("Routes a record to an appropriate port based on the evaluation of an expression on the value of " +
  "one or more of its fields.")
public class Router extends SplitterTransform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Router.class);

  private final Config config;
  private PortSpecificationEvaluator evaluator;

  Router(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    MultiOutputStageConfigurer stageConfigurer = configurer.getMultiOutputStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    config.validate(inputSchema, failureCollector);
    evaluator = config.getPortSpecificationEvaluator(inputSchema, failureCollector);
    Map<String, Schema> schemas = generateSchemas(inputSchema);
    stageConfigurer.setOutputSchemas(schemas);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    evaluator = config.getPortSpecificationEvaluator(context.getInputSchema(), context.getFailureCollector());
  }

  @Override
  public void destroy() {
    // No Op
  }

  @Override
  public void transform(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    // Emit based on the specified rules in the port specification
    String port = null;
    try {
      port = evaluator.getPort(input);
    } catch (PortNotSpecifiedException e) {
      if (e.isDefaultValue()) {
        // the value does not match any rule in the port specification, emit it as a
        // defaulting value, based on the selected default handling option
        emitDefaultValue(input, emitter);
      } else {
        emitNullValue(input, emitter);
      }
    }
    // if a port is found, emit to the port
    emitter.emit(port, input);
  }

  private void emitDefaultValue(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    if (Config.DefaultHandling.DEFAULT_PORT == config.getDefaultHandling()) {
      emitter.emit(config.defaultPort, input);
    } else if (Config.DefaultHandling.ERROR_PORT == config.getDefaultHandling()) {
      String error = String.format("Record contained unknown value for field %s", config.routingField);
      InvalidEntry<StructuredRecord> invalid = new InvalidEntry<>(1, error, input);
      emitter.emitError(invalid);
    } else if (Config.DefaultHandling.SKIP == config.getDefaultHandling()) {
      LOG.trace("Skipping record because value for field {} did not match any rule in the port specification",
                config.routingField);
    }
  }

  private void emitNullValue(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    if (Config.NullHandling.NULL_PORT == config.getNullHandling()) {
      emitter.emit(config.nullPort, input);
    } else if (Config.NullHandling.ERROR_PORT == config.getNullHandling()) {
      String error = String.format("Record contained null value for field %s", config.routingField);
      InvalidEntry<StructuredRecord> invalid = new InvalidEntry<>(2, error, input);
      emitter.emitError(invalid);
    } else if (Config.NullHandling.SKIP == config.getNullHandling()) {
      LOG.trace("Skipping record because field {} is null", config.routingField);
    }
  }

  private Map<String, Schema> generateSchemas(Schema inputSchema) {
    Map<String, Schema> schemas = new HashMap<>();
    // Add all ports from the port config to the schemas
    for (String port : evaluator.getAllPorts()) {
      schemas.put(port, inputSchema);
    }
    // If defaulting records need to be sent to their own port, add that port to the schemas
    if (Config.DefaultHandling.DEFAULT_PORT == config.getDefaultHandling()) {
      schemas.put(config.defaultPort, inputSchema);
    }
    // If null values need to be sent to their own port, add that port to the schemas
    if (Config.NullHandling.NULL_PORT == config.getNullHandling()) {
      schemas.put(config.nullPort, inputSchema);
    }
    return schemas;
  }

  /**
   * {@link PluginConfig} for {@link Router}
   */
  static class Config extends PluginConfig {
    private static final List<Schema.Type> ALLOWED_TYPES = new ArrayList<>();
    static {
      ALLOWED_TYPES.add(Schema.Type.STRING);
      ALLOWED_TYPES.add(Schema.Type.INT);
      ALLOWED_TYPES.add(Schema.Type.LONG);
      ALLOWED_TYPES.add(Schema.Type.FLOAT);
      ALLOWED_TYPES.add(Schema.Type.DOUBLE);
      // only supported for LogicalType.DECIMAL
      ALLOWED_TYPES.add(Schema.Type.BYTES);
    }
    @VisibleForTesting
    static final String DEFAULT_PORT_NAME = "Default";
    @VisibleForTesting
    static final String DEFAULT_NULL_PORT_NAME = "Null";

    private static final String ROUTE_SPECIFICATION_MODE_PROPERTY_NAME = "routeSpecificationMode";
    private static final String ROUTING_FIELD_PROPERTY_NAME = "routingField";
    static final String BASIC_PORT_SPECIFICATION_PROPERTY_NAME = "portSpecification";
    static final String MVEL_PORT_SPECIFICATION_PROPERTY_NAME = "mvelPortSpecification";
    private static final String DEFAULT_HANDLING_PROPERTY_NAME = "defaultHandling";
    private static final String NULL_HANDLING_PROPERTY_NAME = "nullHandling";
    @VisibleForTesting
    static final String BASIC_MODE_NAME = "basic";
    @VisibleForTesting
    static final String MVEL_MODE_NAME = "mvel";

    @Name(ROUTE_SPECIFICATION_MODE_PROPERTY_NAME)
    @Description("The mode in which you would like to provide the routing specification. The basic mode allows you " +
      "to specify multiple simple routing rules, where each rule operates on a single field in the input schema. " +
      "For basic, the Routing Field and Port Specification are required. The MVEL mode allows you to specify " +
      "complex routing rules, which can operate on multiple input fields in a single rule. In the MVEL mode, " +
      "you can use MVEL expressions to specify the routing configuration. Also specify the MVEL Port Specification " +
      "for the MVEL mode. Defaults to Basic.")
    @Macro
    @Nullable
    private final String routeSpecificationMode;

    @Name(ROUTING_FIELD_PROPERTY_NAME)
    @Description("Specifies the field in the input schema on which the rules in the _Port Specification_ should be " +
      "applied, to determine the port where the record should be routed to. Only required when Route Specification " +
      "Mode is Basic.")
    @Macro
    @Nullable
    private final String routingField;

    @Name(BASIC_PORT_SPECIFICATION_PROPERTY_NAME)
    @Description("Specifies the rules to determine the port where the record should be routed to. Rules are applied " +
      "on the value of the routing field. The port specification is expressed as a comma-separated list of rules, " +
      "where each rule has the format [port-name]:[function-name]([parameter-name]). [port-name] is the name of the " +
      "port to route the record to if the rule is satisfied. Refer to the documentation for a list of available " +
      "functions that you can use as a [function-name]. [parameter-name] is the parameter based on which the " +
      "selected function evaluates the value of the routing field. Only required when Route Specification Mode is " +
      "Basic.")
    @Macro
    @Nullable
    private final String portSpecification;

    @Name(MVEL_PORT_SPECIFICATION_PROPERTY_NAME)
    @Description("Specifies a ',' separated list of ports, and the MVEL expression to route the record to the port " +
      "in the format [port-name]:[mvel-expression]. All the input fields are available as variables in the MVEL " +
      "expression. Additionally, utility methods from common Java classes such as Math, Guava Strings, Apache " +
      "Commons Lang StringUtils are also available for use in the port specification rules. To avoid conflicts " +
      "with delimiters, it is recommended to URL-encode the MVEL expressions. Some example expressions: " +
      "PortA:StringUtils.startsWith(part_id%2C'a'), PortB:StringUtils.contains(supplier%2C'abc'), " +
      "PortC:Math.ceil(amount) < 40. Each MVEL expression in the MVEL Port Specification must return a boolean " +
      "value. Only required when Route Specification Mode is MVEL.")
    @Macro
    @Nullable
    private final String mvelPortSpecification;

    @Name(DEFAULT_HANDLING_PROPERTY_NAME)
    @Description("Determines the way to handle records whose value for the field to match on doesn't match any of " +
      "the rules defined in the port specification. Defaulting records can either be skipped (\"Skip\"), sent to a " +
      "specific port (\"Send to default port\"), or sent to the error port (\"Send to error port\"). " +
      "By default, such records are sent to the error port.")
    @Nullable
    private final String defaultHandling;

    @Name("defaultPort")
    @Description("Determines the port to which records that do not match any of the rules in the port specification " +
      "are routed. This is only used if default handling is set to \"Send to default port\". Defaults to 'Default'.")
    @Nullable
    private final String defaultPort;

    @Name(NULL_HANDLING_PROPERTY_NAME)
    @Description("Determines the way to handle records whose value for the routing field is null. Such records can " +
      "either be skipped (\"Skip\"), sent to a specific port (\"Send to null port\"), or sent to the error port " +
      "(\"Send to error port\"). By default, such records are sent to the error port.")
    @Nullable
    private final String nullHandling;

    @Name("nullPort")
    @Description("Determines the port to which records with null values for the field to split on are routed to. " +
      "This is only used if default handling is set to \"Send to null port\". Defaults to 'Null'.")
    @Nullable
    private final String nullPort;

    Config(@Nullable String routeSpecificationMode, @Nullable String routingField, @Nullable String portSpecification,
           @Nullable String mvelPortSpecification, @Nullable String defaultHandling, @Nullable String defaultPort,
           @Nullable String nullHandling, @Nullable String nullPort) {
      this.routeSpecificationMode = routeSpecificationMode;
      this.routingField = routingField;
      this.portSpecification = portSpecification;
      this.mvelPortSpecification = mvelPortSpecification;
      this.defaultHandling = defaultHandling == null ? DefaultHandling.ERROR_PORT.value : defaultHandling;
      this.defaultPort = defaultPort == null ? DEFAULT_PORT_NAME : defaultPort;
      this.nullHandling = nullHandling == null ? NullHandling.ERROR_PORT.value : nullHandling;
      this.nullPort = nullPort == null ? DEFAULT_NULL_PORT_NAME : nullPort;
    }

    @VisibleForTesting
    void validate(Schema inputSchema, FailureCollector collector) throws IllegalArgumentException {
      if (!BASIC_MODE_NAME.equalsIgnoreCase(routeSpecificationMode) &&
        !MVEL_MODE_NAME.equalsIgnoreCase(routeSpecificationMode)) {
        collector.addFailure(String.format("Unknown route specification mode %s", routeSpecificationMode),
                             String.format("Please specify one of %s or %s", BASIC_MODE_NAME, MVEL_MODE_NAME))
          .withConfigProperty(ROUTE_SPECIFICATION_MODE_PROPERTY_NAME);
      }
      Optional<DefaultHandling> handling = DefaultHandling.fromValue(defaultHandling);
      if (!handling.isPresent()) {
        collector.addFailure(String.format("Unsupported default handling value %s", DEFAULT_HANDLING_PROPERTY_NAME),
                             String.format("Please specify one of %s", Joiner.on(",").join(DefaultHandling.values())))
          .withConfigProperty(DEFAULT_HANDLING_PROPERTY_NAME);
      }

      if (BASIC_MODE_NAME.equalsIgnoreCase(routeSpecificationMode)) {
        validateBasicPortSpecification(inputSchema, collector);
      } else if (MVEL_MODE_NAME.equalsIgnoreCase(routeSpecificationMode)) {
        if (mvelPortSpecification == null || mvelPortSpecification.isEmpty()) {
          collector.addFailure("In MVEL mode, MVEL port specification must be provided", null)
            .withConfigProperty(MVEL_PORT_SPECIFICATION_PROPERTY_NAME);
          return;
        }
      }

      // only done for validation, so ignoring return value.
      getPortSpecificationEvaluator(inputSchema, collector);
    }

    private void validateBasicPortSpecification(Schema inputSchema, FailureCollector collector) {
      if (routingField == null || routingField.isEmpty()) {
        collector.addFailure("Routing field is required when routing mode is basic.",
                             "Please provide the routing field").withConfigProperty(ROUTING_FIELD_PROPERTY_NAME);
      }
      Schema.Field field = inputSchema.getField(routingField);
      if (field == null) {
        collector.addFailure(String.format("Routing field %s not found in the input schema", routingField),
                             "Please provide a field that exists in the input schema")
          .withConfigProperty(ROUTING_FIELD_PROPERTY_NAME);
        throw collector.getOrThrowException();
      }
      Schema fieldSchema = field.getSchema();
      Schema.Type type = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (!ALLOWED_TYPES.contains(type)) {
        collector.addFailure(
          String.format("Routing field '%s' must be of one of the following types - %s. Found '%s'",
                        routingField, getSupportedTypes(), fieldSchema.getDisplayName()),
          null).withConfigProperty(ROUTING_FIELD_PROPERTY_NAME);
      }
      if (portSpecification == null || portSpecification.isEmpty()) {
        collector.addFailure("No port specifications defined.", "Please provide at least 1 port specification")
          .withConfigProperty(BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }
    }

    private List<String> getSupportedTypes() {
      List<String> supportedTypes = new ArrayList<>(ALLOWED_TYPES.size() + Schema.LogicalType.values().length);
      for (Schema.Type allowedType : ALLOWED_TYPES) {
        supportedTypes.add(allowedType.name());
      }
      for (Schema.LogicalType value : Schema.LogicalType.values()) {
        supportedTypes.add(value.name());
      }
      return supportedTypes;
    }

    private PortSpecificationEvaluator getPortSpecificationEvaluator(Schema inputSchema, FailureCollector collector) {
      if (BASIC_MODE_NAME.equalsIgnoreCase(routeSpecificationMode)) {
        return new BasicPortSpecificationEvaluator(routingField, portSpecification, collector);
      }
      if (MVEL_MODE_NAME.equalsIgnoreCase(routeSpecificationMode)) {
        return new MVELPortSpecificationEvaluator(mvelPortSpecification, inputSchema, collector);
      }
      collector.addFailure(
        String.format("Invalid route specification mode '%s'. Must be one of '%s' or '%s'",
                      routeSpecificationMode, BASIC_MODE_NAME, MVEL_MODE_NAME), null
      ).withConfigProperty("routeSpecificationMode");
      throw collector.getOrThrowException();
    }

    DefaultHandling getDefaultHandling() {
      return DefaultHandling.fromValue(defaultHandling)
        .orElseThrow(() -> new IllegalArgumentException("Unsupported default handling value: " + defaultHandling));
    }

    NullHandling getNullHandling() {
      return NullHandling.fromValue(nullHandling)
        .orElseThrow(() -> new IllegalArgumentException("Unsupported null handling value: " + nullHandling));
    }

    enum FunctionType {
      EQUALS,
      NOT_EQUALS,
      CONTAINS,
      NOT_CONTAINS,
      IN,
      NOT_IN,
      MATCHES,
      NOT_MATCHES,
      STARTS_WITH,
      NOT_STARTS_WITH,
      ENDS_WITH,
      NOT_ENDS_WITH,
      NUMBER_EQUALS,
      NUMBER_NOT_EQUALS,
      NUMBER_GREATER_THAN,
      NUMBER_GREATER_THAN_OR_EQUALS,
      NUMBER_LESSER_THAN,
      NUMBER_LESSER_THAN_OR_EQUALS,
      NUMBER_BETWEEN,
      NUMBER_NOT_BETWEEN,
      DATE_EQUALS,
      DATE_NOT_EQUALS,
      DATE_AFTER,
      DATE_AFTER_OR_ON,
      DATE_BEFORE,
      DATE_BEFORE_OR_ON,
      DATE_BETWEEN,
      DATE_NOT_BETWEEN
    }

    enum DefaultHandling {
      SKIP("Skip"),
      ERROR_PORT("Send to error port"),
      DEFAULT_PORT("Send to default port");

      private final String value;

      DefaultHandling(String value) {
        this.value = value;
      }

      public String value() {
        return value;
      }

      /**
       * Converts default handling string value into {@link DefaultHandling} enum.
       *
       * @param defaultValue default handling string value
       * @return default handling type in optional container
       */
      public static Optional<DefaultHandling> fromValue(String defaultValue) {
        return Stream.of(values())
          .filter(keyType -> keyType.value.equalsIgnoreCase(defaultValue))
          .findAny();
      }
    }

    enum NullHandling {
      SKIP("Skip"),
      ERROR_PORT("Send to error port"),
      NULL_PORT("Send to null port");

      private final String value;

      NullHandling(String value) {
        this.value = value;
      }

      public String value() {
        return value;
      }

      /**
       * Converts null handling string value into {@link NullHandling} enum.
       *
       * @param nullValue null handling string value
       * @return null handling type in optional container
       */
      public static Optional<NullHandling> fromValue(String nullValue) {
        return Stream.of(values())
          .filter(keyType -> keyType.value.equalsIgnoreCase(nullValue))
          .findAny();
      }
    }
  }
}
