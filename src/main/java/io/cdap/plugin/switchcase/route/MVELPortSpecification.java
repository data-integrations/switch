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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import org.apache.commons.lang3.StringUtils;
import org.mvel2.CompileException;
import org.mvel2.ParserContext;
import org.mvel2.compiler.CompiledExpression;
import org.mvel2.compiler.ExpressionCompiler;
import java.util.List;

final class MVELPortSpecification extends PortSpecification {
  private static final ParserContext CONTEXT = new ParserContext();
  static {
    CONTEXT.addImport("Math", Math.class);
    CONTEXT.addImport("Strings", Strings.class);
    CONTEXT.addImport("StringUtils", StringUtils.class);
    CONTEXT.setStrictTypeEnforcement(true);
  }
  private final CompiledExpression expression;

  MVELPortSpecification(String name, String expression, Schema inputSchema, FailureCollector collector) {
    super(name);
    this.expression = parse(expression, inputSchema, collector);
  }

  public CompiledExpression getExpression() {
    return expression;
  }

  private static CompiledExpression parse(String expression, Schema inputSchema, FailureCollector collector) {
    setVariables(inputSchema, collector);
    ExpressionCompiler compiler = new ExpressionCompiler(expression, CONTEXT);
    CompiledExpression compiledExpression;
    try {
      compiledExpression = compiler.compile();
    } catch (CompileException e) {
      collector.addFailure(
        String.format("Failure while compiling MVEL expression %s. Message: %s", expression, e.getMessage()),
        "Please correct the expression").withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }
    // this is not trustworthy. it returns non-boolean for conditions on primitives
//    Class<?> returnType = compiler.getReturnType();
//    if (!boolean.class.equals(returnType) && !Boolean.class.equals(returnType)) {
//      collector.addFailure(
//        String.format("Expected MVEL expression %s to return Boolean, but found %s.", expression, returnType),
//        "Please specify an expression that returns a boolean value.");
//      throw collector.getOrThrowException();
//    }
    return compiledExpression;
  }

  private static void setVariables(Schema inputSchema, FailureCollector collector) {
    List<Schema.Field> fields = inputSchema.getFields();
    if (fields == null) {
      collector.addFailure("Found schema with empty fields.", null);
      throw collector.getOrThrowException();
    }
    for (Schema.Field field : fields) {
      String name = field.getName();
      Schema schema = field.getSchema();
      Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
      CONTEXT.addVariable(name, getClassForType(type, collector));
    }
  }

  private static Class<?> getClassForType(Schema.Type type, FailureCollector collector) {
    switch (type) {
      case STRING:
        return String.class;
      case INT:
        return Integer.class;
      case LONG:
        return Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case BOOLEAN:
        return Boolean.class;
      case BYTES:
        return byte[].class;
      case UNION:
      case ENUM:
      case MAP:
      case ARRAY:
      case RECORD:
        return Object.class;
      default:
        collector.addFailure(String.format("Unsupported schema type %s.", type), null);
        throw collector.getOrThrowException();
    }
  }
}
