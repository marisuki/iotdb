package org.apache.iotdb.db.mpp.simd;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Typical Sliding Window Hash @Input Start_Timestamp, End_timestamp, Sliding_sz, Window_sz @Output
 * GroupId \subset [N] FSM f: T \to [N], [N] as the set of groups, with mark: unique.
 */
public class TypicalSWHashing {
  private long wsz, step;
  private long start, end;

  public TypicalSWHashing(long st, long ed, long window_step, long window_sz) {
    wsz = window_sz;
    step = window_step;
    start = st;
    end = ed;
  }

  public LongVector hashFinalInclude(LongVector v_timeTag) {
    LongVector tmp = v_timeTag.add(-start);
    // tmp = tmp.add(step);
    return tmp.div(step);
  }

  public LongVector hashFirstInclude(LongVector v_TimeTag) {
    long cst = -start - wsz + step;
    LongVector tmp = v_TimeTag.add(cst);
    return tmp.div(step);
  }

  public static void main(String[] args) {
    final VectorSpecies<Long> SPECIES = LongVector.SPECIES_256;
    System.out.println(SPECIES.length());
    long[] a = {1L, 2L, 3L, 4L};
    LongVector av = LongVector.fromArray(SPECIES, a, 0);
    LongVector res = av.div(2L);
    long[] rss = res.toArray();
    for (long x : rss) System.out.println(x);
  }
}
