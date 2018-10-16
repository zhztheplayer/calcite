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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Function that catches and handle errors thrown from its argument.
 */
public class SqlCatchErrorFunction extends SqlFunction {
  private final ErrorBehavior errorBehavior;

  public SqlCatchErrorFunction(String name, ErrorBehavior errorBehavior) {
    super(name,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM);
    this.errorBehavior = errorBehavior;
  }

  public ErrorBehavior getErrorBehavior() {
    return errorBehavior;
  }

  @Override public String getSignatureTemplate(int operandsCount) {
    assert operandsCount == 1;
    switch (errorBehavior) {
    case EMPTY:
      return "{0}({1} EMPTY ON ERROR)";
    case ERROR:
      return "{0}({1} ERROR ON ERROR)";
    default:
      throw new IllegalStateException("illegal error behavior: " + errorBehavior);
    }
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall("CATCH_ERROR");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.keyword(errorBehavior.toString());
    writer.keyword("ON ERROR");
    writer.endFunCall(frame);
  }

  /**
   * Supported error behaviors
   */
  public enum ErrorBehavior {
    EMPTY, ERROR
  }
}

// End SqlCatchErrorFunction.java
