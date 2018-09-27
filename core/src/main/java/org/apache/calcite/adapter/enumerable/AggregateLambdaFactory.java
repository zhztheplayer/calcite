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

package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;

/**
 * Generates lambda functions used in {@link EnumerableAggregate}, this interface allows a implicit
 * accumulator type variation. (OAccumulate {@literal ->} TAccumulate)
 *
 * @param <TSource> Type of the enumerable input source.
 * @param <OAccumulate> Type of the original accumulator.
 * @param <TAccumulate> Type of the varied accumulator.
 * @param <TResult> Type of the enumerable output result.
 * @param <GroupByKey> Type of the group-by key.
 */
public interface AggregateLambdaFactory<TSource, OAccumulate, TAccumulate, TResult, GroupByKey> {
  Function0<TAccumulate> accumulatorInitializer();

  Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder();

  Function1<TAccumulate, TResult> singleGroupResultSelector(
      Function1<OAccumulate, TResult> resultSelector);

  Function2<GroupByKey, TAccumulate, TResult> resultSelector(
      Function2<GroupByKey, OAccumulate, TResult> resultSelector);
}

// End AggregateLambdaFactory.java
