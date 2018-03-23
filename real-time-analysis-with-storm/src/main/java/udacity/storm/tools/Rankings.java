/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package udacity.storm.tools;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import udacity.storm.tools.Rankable;
/*
 * 提供更新、排序服务
 */
public class Rankings implements Serializable {

  private static final long serialVersionUID = -1549827195410578903L;
  private static final int DEFAULT_COUNT = 10;

  private final int maxSize;
  private final List<Rankable> rankedItems = Lists.newArrayList();

  public Rankings() {
    this(DEFAULT_COUNT);
  }

  public Rankings(int topN) {
    if (topN < 1) {
      throw new IllegalArgumentException("topN must be >= 1");
    }
    maxSize = topN;
  }

  /**
   * Copy constructor.
   * @param other
   */
  public Rankings(Rankings other) {
    this(other.maxSize());
    updateWith(other);
  }

  /**
   * @return the maximum possible number (size) of ranked objects this instance can hold
   */
  public int maxSize() {
    return maxSize;
  }

  /**
   * @return the number (size) of ranked objects this instance is currently holding
   */
  public int size() {
    return rankedItems.size();
  }

  /**
   * The returned defensive copy is only "somewhat" defensive.  We do, for instance, return a defensive copy of the
   * enclosing List instance, and we do try to defensively copy any contained Rankable objects, too.  However, the
   * contract of {@link storm.starter.tools.Rankable#copy()} does not guarantee that any Object's embedded within
   * a Rankable will be defensively copied, too.
   *
   * @return a somewhat defensive copy of ranked items
   */
  public List<Rankable> getRankings() {
    List<Rankable> copy = Lists.newLinkedList();
    for (Rankable r: rankedItems) {
      copy.add(r.copy());
    }
    return ImmutableList.copyOf(copy);
  }

  public void updateWith(Rankings other) {
    for (Rankable r : other.getRankings()) {
      updateWith(r);
    }
  }

  public void updateWith(Rankable r) {
    synchronized(rankedItems) {
      addOrReplace(r);
      rerank();
      shrinkRankingsIfNeeded();
    }
  }

  /*
   * 由于rolling-count是实时不断的累计计数，需要不断更新已存在的单词的词频数，
   * 所以需要replace
   */
  private void addOrReplace(Rankable r) {
    Integer rank = findRankOf(r);
    if (rank != null) {
      rankedItems.set(rank, r);
    }
    else {
      rankedItems.add(r);
    }
  }

  private Integer findRankOf(Rankable r) {
    Object tag = r.getObject();
    for (int rank = 0; rank < rankedItems.size(); rank++) {
      Object cur = rankedItems.get(rank).getObject();
      if (cur.equals(tag)) {//check whether there exists a tag contained in the list.
        return rank;
      }
    }
    return null;
  }

  /**
   * sort是升序排列，然后reverse按降序排列
   */
  private void rerank() {
    Collections.sort(rankedItems);
    Collections.reverse(rankedItems);
  }

  private void shrinkRankingsIfNeeded() {
    if (rankedItems.size() > maxSize) {
      rankedItems.remove(maxSize);//每更新一次都会调用shrink，因此最多只超过容量一个元素，所以删除操作最多一次
    }
  }

  /**
   * Removes ranking entries that have a count of zero.
   * Use iterator is a better choice.
   */
  public void pruneZeroCounts() {
    int i = 0;
    while (i < rankedItems.size()) {
      if (rankedItems.get(i).getCount() == 0) {
        rankedItems.remove(i);
      }
      else {
        i++;
      }
    }
  }

  public String toString() {
    return rankedItems.toString();
  }

  /**
   * Creates a (defensive) copy of itself.内容是浅拷贝，外壳是新地址
   */
  public Rankings copy() {
    return new Rankings(this);
  }
}
