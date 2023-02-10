/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.aggregation.timerangeiterator;

import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.util.List;

/**
 * This interface used for iteratively generating aggregated time windows in GROUP BY query.
 *
 * <p>It will return a leftCloseRightClose time window, by decreasing maxTime if leftCloseRightOpen
 * and increasing minTime if leftOpenRightClose.
 */
public interface ITimeRangeIterator {

  /** return the first time range by sorting order */
  TimeRange getFirstTimeRange();

  /** @return whether current iterator has next time range */
  boolean hasNextTimeRange();
  /**
   * return the next time range according to curStartTime (the start time of the last returned time
   * range)
   */
  TimeRange nextTimeRange();

  boolean isAscending();

  default TimeRange getFinalTimeRange(TimeRange timeRange, boolean leftCRightO) {
    return leftCRightO
        ? new TimeRange(timeRange.getMin(), timeRange.getMax() - 1)
        : new TimeRange(timeRange.getMin() + 1, timeRange.getMax());
  }

  /**
   * As there is only one timestamp can be output for a time range, this method will return the
   * output time based on leftCloseRightOpen or not.
   *
   * @return minTime if leftCloseRightOpen, else maxTime.
   */
  long currentOutputTime();

  long getTotalIntervalNum();

  /**
   * Return the first group tag of the related timestamp. Need to filter out the unsatisfied tuples:
   * Tx > ED OR Tx < ST --> should be filtered ahead by optimizer. Only for slidingWindow: st +
   * i*step <= currTimestamp <= st + i*step + interval; return the first group id satisfying the
   * predicates: i. Return -1 if curr timestamp does not belong to any window Example: <--w1--> *T1
   * <--w2-->...
   *
   * @param currTimestamp: the timestamp of a time-series tuple
   * @return tag: treat tuples within a window as a group
   */
  int getFirstRelatedWindowAsTag(long currTimestamp);

  /**
   * Return the last group tag of the related timestamp. Need to filter out the unsatisfied tuples:
   * Tx > ED OR Tx < ST --> should be filtered ahead by optimizer. Only for slidingWindow: st +
   * i*step <= currTimestamp <= st + i*step + interval return the final group id satisfying the
   * predicates: i. Return -1 if curr timestamp does not belong to any window Example: <--w1--> *T1
   * <--w2-->...
   *
   * @param currTimestamp: the timestamp of a time-series tuple
   * @return tag: treat tuples within a window as a group
   */
  int getLastRelatedWindowAsTag(long currTimestamp);

  /**
   * Return all group tags of the related timestamp. Need to filter out the unsatisfied tuples: Tx >
   * ED OR Tx < ST --> should be filtered ahead by optimizer. For any window-based aggregation: st +
   * i*step <= currTimestamp <= st + i*step + interval return all group ids satisfying the
   * predicates: list[i]. Return emptyList if curr timestamp does not belong to any window
   *
   * @param currTimestamp: the timestamp of a time-series tuple
   * @return tags: treat all tuples within a window as a group
   */
  List<Integer> getRelatedWindowTags(long currTimestamp);

  /**
   * Return the start timestamp of a window according to the tag (int).
   *
   * @param windowTag: the group id.
   * @return timestamp(long): the start timestamp.
   */
  long getWindowStartTimestampByTag(int windowTag);

  /**
   * Return the end timestamp of a window according to the tag (int).
   *
   * @param windowTag: the group id.
   * @return timestamp(long): the end timestamp.
   */
  long getWindowEndTimestampByTag(int windowTag);
}
