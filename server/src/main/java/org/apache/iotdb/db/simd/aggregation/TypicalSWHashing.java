package org.apache.iotdb.db.simd.aggregation;


import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.iotdb.db.simd.fsm.FiniteStateMachine;

/**
 * Typical Sliding Window Hash
 * @Input Start_Timestamp, End_timestamp, Sliding_sz, Window_sz
 * @Output FSM f: T \to [N], [N] as the set of groups, with mark: unique.
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

    public IntVector hashFirstInclude(LongVector timeTagVec) {
        LongVector tmp = timeTagVec.add(-start);
        tmp.div(step);
        return null;
    }

    public static void main(String[] args) {
        final VectorSpecies<Long> SPECIES = LongVector.SPECIES_256;
        long[] a = {1L, 2L, 3L, 4L};
        LongVector av = LongVector.fromArray(SPECIES, a, 0);
        LongVector res = av.div(2L);
        long[] rss = res.toArray();
        for(long x: rss) System.out.println(x);
    }
}
