/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.Function0;


/**
 * Exception handler that handles exceptions.
 */
@Experimental
public interface ExceptionHandler {
  /**
   * @param supplier the supplier that should be handled.
   * @param defaultValue the default value should be returned when an exception is thrown from
   *                     supplier.
   * @param <T> the returned value type.
   * @return supplier.get(), or defaultValue when exception is thrown.
   */
  <T> T handleException(Function0<T> supplier, T defaultValue);
}

// End ExceptionHandler.java
