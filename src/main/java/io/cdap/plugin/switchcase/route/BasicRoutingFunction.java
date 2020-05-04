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

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Defines the contract for a routing function, which decides  which port to route a record to
 */
public interface BasicRoutingFunction {
  /**
   * Evaluates the value of a field against an expected value, based on a specified routing function
   *
   * @param actualValue the value to evaluate
   * @param compareValue the value to evaluate against
   * @param schema the {@link Schema} of the routing field as defined in the record schema. The
   *                {@link Schema.Type Type} or {@link Schema.LogicalType Logical Type} of the routing field as defined
   *                in the record schema is used for parsing the data.
   * @return true if the condition is satisfied, false otherwise
   */
  boolean evaluate(Object actualValue, String compareValue, Schema schema);
}
