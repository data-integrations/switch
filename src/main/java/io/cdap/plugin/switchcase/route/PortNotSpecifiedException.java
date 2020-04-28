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

/**
 * Exception thrown when the port to route a record cannot be determined using the port specification
 */
public class PortNotSpecifiedException extends Exception {

  private final Reason reason;

  public PortNotSpecifiedException(Reason reason) {
    super(String.format("Couldn't find the right port for the record, because of a %s value", reason));
    this.reason = reason;
  }

  public boolean isDefaultValue() {
    return Reason.DEFAULT == reason;
  }

  enum Reason {
    DEFAULT,
    NULL
  }
}
