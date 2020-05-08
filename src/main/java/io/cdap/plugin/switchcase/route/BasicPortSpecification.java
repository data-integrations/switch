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

import com.google.common.base.Joiner;
import io.cdap.cdap.etl.api.FailureCollector;
import java.util.HashMap;
import java.util.Map;

final class BasicPortSpecification extends PortSpecification {
  private static final Map<Router.Config.FunctionType, BasicRoutingFunction> FUNCTIONS = new HashMap<>();
  static {
    FUNCTIONS.put(Router.Config.FunctionType.EQUALS, new BasicRoutingFunctions.EqualsFunction());
    FUNCTIONS.put(Router.Config.FunctionType.NOT_EQUALS, new BasicRoutingFunctions.NotEqualsFunction());
    FUNCTIONS.put(Router.Config.FunctionType.CONTAINS, new BasicRoutingFunctions.ContainsFunction());
    FUNCTIONS.put(Router.Config.FunctionType.NOT_CONTAINS, new BasicRoutingFunctions.NotContainsFunction());
    FUNCTIONS.put(Router.Config.FunctionType.IN, new BasicRoutingFunctions.InFunction());
    FUNCTIONS.put(Router.Config.FunctionType.NOT_IN, new BasicRoutingFunctions.NotInFunction());
    FUNCTIONS.put(Router.Config.FunctionType.MATCHES, new BasicRoutingFunctions.MatchesFunction());
    FUNCTIONS.put(Router.Config.FunctionType.NOT_MATCHES, new BasicRoutingFunctions.NotMatchesFunction());
    FUNCTIONS.put(Router.Config.FunctionType.STARTS_WITH, new BasicRoutingFunctions.StartsWithFunction());
    FUNCTIONS.put(Router.Config.FunctionType.NOT_STARTS_WITH, new BasicRoutingFunctions.NotStartsWithFunction());
    FUNCTIONS.put(Router.Config.FunctionType.ENDS_WITH, new BasicRoutingFunctions.EndsWithFunction());
    FUNCTIONS.put(Router.Config.FunctionType.NOT_ENDS_WITH, new BasicRoutingFunctions.NotEndsWithFunction());
    FUNCTIONS.put(Router.Config.FunctionType.NUMBER_EQUALS, new BasicRoutingFunctions.NumberEqualsFunction());
    FUNCTIONS.put(
      Router.Config.FunctionType.NUMBER_NOT_EQUALS, new BasicRoutingFunctions.NumberNotEqualsFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.NUMBER_GREATER_THAN, new BasicRoutingFunctions.GreaterThanFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.NUMBER_GREATER_THAN_OR_EQUALS,
      new BasicRoutingFunctions.GreaterThanOrEqualsFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.NUMBER_LESSER_THAN,
      new BasicRoutingFunctions.LesserThanFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.NUMBER_LESSER_THAN_OR_EQUALS,
      new BasicRoutingFunctions.LesserThanOrEqualsFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.NUMBER_BETWEEN,
      new BasicRoutingFunctions.NumberBetweenFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.NUMBER_NOT_BETWEEN,
      new BasicRoutingFunctions.NumberNotBetweenFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.DATE_EQUALS,
      new BasicRoutingFunctions.DateEqualsFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.DATE_NOT_EQUALS,
      new BasicRoutingFunctions.DateNotEqualsFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.DATE_AFTER,
      new BasicRoutingFunctions.DateAfterFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.DATE_AFTER_OR_ON,
      new BasicRoutingFunctions.DateAfterOrOnFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.DATE_BEFORE,
      new BasicRoutingFunctions.DateBeforeFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.DATE_BEFORE_OR_ON,
      new BasicRoutingFunctions.DateBeforeOrOnFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.DATE_BETWEEN,
      new BasicRoutingFunctions.DateBetweenFunction()
    );
    FUNCTIONS.put(
      Router.Config.FunctionType.DATE_NOT_BETWEEN,
      new BasicRoutingFunctions.DateNotBetweenFunction()
    );
  }
  private final BasicRoutingFunction routingFunction;
  private final String parameter;

  BasicPortSpecification(String name, Router.Config.FunctionType functionType, String parameter,
                         FailureCollector collector) {
    super(name);
    this.routingFunction = fromFunctionType(functionType, collector);
    this.parameter = parameter;
  }

  BasicRoutingFunction getRoutingFunction() {
    return routingFunction;
  }

  String getParameter() {
    return parameter;
  }

  private BasicRoutingFunction fromFunctionType(Router.Config.FunctionType functionType,
                                                FailureCollector collector) {
    if (!FUNCTIONS.containsKey(functionType)) {
        collector.addFailure(
          "Unknown routing function " + functionType,
          "Routing function must be one of " + Joiner.on(",").join(Router.Config.FunctionType.values())
        ).withConfigProperty(Router.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
        throw collector.getOrThrowException();
    }
    return FUNCTIONS.get(functionType);
  }

  @Override
  public String toString() {
    return "BasicPortSpecification{" +
      "name='" + getName() + '\'' +
      ", routingFunction=" + routingFunction +
      ", parameter='" + parameter + '\'' +
      '}';
  }
}
