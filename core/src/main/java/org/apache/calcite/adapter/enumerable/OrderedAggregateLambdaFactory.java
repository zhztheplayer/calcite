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

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Generate aggregate lambdas that sorts the input source before calling each aggregate adder.
 *
 * @param <TSource> Type of the enumerable input source.
 * @param <GroupByKey> Type of the group-by key.
 * @param <SortKey> Type of the sort key.
 * @param <OAccumulate> Type of the original accumulator.
 * @param <TResult> Type of the enumerable output result.
 */
public class OrderedAggregateLambdaFactory<TSource, GroupByKey, SortKey, OAccumulate, TResult>
    implements AggregateLambdaFactory<TSource, OAccumulate,
    OrderedAggregateLambdaFactory.LazySource<TSource>, TResult, GroupByKey> {

  private final Function0<OAccumulate> accumulatorInitializer;
  private final List<SourceSorter<OAccumulate, TSource, SortKey>> sourceSorters;

  public OrderedAggregateLambdaFactory(
      Function0<OAccumulate> accumulatorInitializer,
      List<SourceSorter<OAccumulate, TSource, SortKey>> sourceSorters) {
    this.accumulatorInitializer = accumulatorInitializer;
    this.sourceSorters = sourceSorters;
  }

  public Function0<LazySource<TSource>> accumulatorInitializer() {
    return new Function0<LazySource<TSource>>() {
      @Override public LazySource<TSource> apply() {
        return new LazySource<>();
      }
    };
  }

  public Function2<LazySource<TSource>,
      TSource, LazySource<TSource>> accumulatorAdder() {
    return new Function2<LazySource<TSource>,
        TSource, LazySource<TSource>>() {
      @Override public LazySource<TSource> apply(
          LazySource<TSource> lazySource, TSource source) {
        lazySource.add(source);
        return lazySource;
      }
    };
  }

  public Function1<LazySource<TSource>, TResult> singleGroupResultSelector(
      Function1<OAccumulate, TResult> resultSelector) {
    return new Function1<LazySource<TSource>, TResult>() {
      @Override public TResult apply(LazySource<TSource> lazySource) {
        OAccumulate accumulator = accumulatorInitializer.apply();
        for (SourceSorter<OAccumulate, TSource, SortKey> acc : sourceSorters) {
          acc.sortAndAccumulate(Iterables.unmodifiableIterable(lazySource), accumulator);
        }
        return resultSelector.apply(accumulator);
      }
    };
  }

  public Function2<GroupByKey, LazySource<TSource>, TResult> resultSelector(
      Function2<GroupByKey, OAccumulate, TResult> resultSelector) {
    return new Function2<GroupByKey, LazySource<TSource>, TResult>() {
      @Override public TResult apply(GroupByKey groupByKey, LazySource<TSource> lazySource) {
        OAccumulate accumulator = accumulatorInitializer.apply();
        for (SourceSorter<OAccumulate, TSource, SortKey> acc : sourceSorters) {
          acc.sortAndAccumulate(Iterables.unmodifiableIterable(lazySource), accumulator);
        }
        return resultSelector.apply(groupByKey, accumulator);
      }
    };
  }

  /**
   * Cache the input sources. (Will be sorted, aggregated in result selector)
   *
   * @param <TSource> Type of the enumerable input source.
   */
  public static class LazySource<TSource> implements Iterable<TSource> {
    private final List<TSource> list = new ArrayList<>();

    private void add(TSource source) {
      list.add(source);
    }

    @Override public Iterator<TSource> iterator() {
      return list.iterator();
    }
  }

}

// End OrderedAggregateLambdaFactory.java
