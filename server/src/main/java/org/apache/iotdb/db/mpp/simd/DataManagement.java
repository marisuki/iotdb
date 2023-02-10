package org.apache.iotdb.db.mpp.simd;

import jdk.incubator.vector.*;

import java.util.ArrayList;
import java.util.List;

public class DataManagement {
  public final VectorSpecies<Long> SPECIES = LongVector.SPECIES_256;
  public final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_256;
  public final long L3CACHE = (1L << 31) * 12 * 8; // bit
  public final int FACTOR_DATA_CACHE = 8; // 1/4 for curr data blk, 1/2 for keys
  // occupy 1/8 * (3 + 1 -> agg)

  public final long TUPLE_LOAD_SZ = L3CACHE / (FACTOR_DATA_CACHE * 64);
  public final long VECTOR_LOAD_SZ = TUPLE_LOAD_SZ / SPECIES.length();

  private LongVector[] cpu_ks;
  private LongVector[] cpu_terminal_ks;
  private List<DoubleVector> cpu_val;

  public int cpu_data_sz;
  public int cpu_vk_sz;

  // private FiniteStateMachine fsm;
  private TypicalSWHashing swHashing;

  public DataManagement(TypicalSWHashing hash) {
    this.swHashing = hash;
    cpu_val = new ArrayList<>();
  }

  public int init_keys(long[] keys, int offset) {
    // hash
    cpu_data_sz = (int) Math.min(TUPLE_LOAD_SZ, keys.length);
    cpu_vk_sz = cpu_data_sz / SPECIES.length();
    cpu_ks = new LongVector[cpu_vk_sz];
    cpu_terminal_ks = new LongVector[cpu_vk_sz];
    for (int i = 0; i < cpu_vk_sz; i += 1) {
      LongVector tmp = LongVector.fromArray(SPECIES, keys, offset);
      cpu_terminal_ks[i] = swHashing.hashFinalInclude(tmp);
      cpu_ks[i] = swHashing.hashFirstInclude(tmp);
      offset += SPECIES.length();
    }
    return offset;
  }

  public void init_val_apply(double[] external_data, int offset, int cpu_data_sz, int cpu_vk_sz) {
    int end_interval = (int) (cpu_terminal_ks[0].lane(3) - cpu_terminal_ks[0].lane(0));
    // cpu_val = new DoubleVector[base];
    int st_interval = (int) (cpu_ks[0].lane(3) - cpu_ks[0].lane(0));
    int ss_count = cpu_terminal_ks[0].lt(cpu_ks[0]).trueCount();

    if (st_interval < 2) {
      if (ss_count > 0) {
        apply_SSWS(external_data, offset, cpu_data_sz, cpu_vk_sz, -end_interval);
      } else if (end_interval < 2) {
        apply_SSWD(
            external_data, offset, cpu_data_sz, cpu_vk_sz, (1 << log2(cpu_terminal_ks[0].lane(3))));
      } else {
        apply_SDWD(
            external_data, offset, cpu_data_sz, cpu_vk_sz, (1 << log2(cpu_terminal_ks[0].lane(3))));
      }
    } else {
      apply_SDWS(
          external_data, offset, cpu_data_sz, cpu_vk_sz, (1 << log2(cpu_terminal_ks[0].lane(3))));
    }
  }

  public void apply_SSWS(
      double[] external_data, int offset, int cpu_data_sz, int cpu_vk_sz, int base) {
    int curr_ptr = offset;
    int curr_val = 0;
    for (int i = 0; i < cpu_vk_sz; i++) {
      LongVector st = cpu_ks[i], ed = cpu_terminal_ks[i];
      int max_ed = (int) ed.reduceLanes(VectorOperators.MAX);
      if (max_ed > cpu_val.size()) {
        int allocate = 1 << log2(max_ed);

        while (allocate != 0) {
          cpu_val.add(DoubleVector.broadcast(DOUBLE_SPECIES, 0.d));
          allocate--;
        }
      }
      // TODO: use locality?
      // double local = external_data[curr_ptr];
      for (int j = 0; j < SPECIES.length(); j++) {
        if (ed.lane(j) > 0 && st.lane(j) > 0) {
          add((int) st.lane(j), (int) ed.lane(j), external_data[curr_ptr]);
        }
        curr_ptr += 1;
      }
    }
  }

  private void apply_SSWD(
      double[] external_data, int offset, int cpu_data_sz, int cpu_vk_sz, int base) {
    for (int i = 0; i < cpu_vk_sz; i += SPECIES.length()) {
      DoubleVector tmp = DoubleVector.broadcast(DOUBLE_SPECIES, 0.0d);
    }
  }

  private void apply_SDWD(
      double[] external_data, int offset, int cpu_data_sz, int cpu_vk_sz, int base) {
    for (int i = 0; i < cpu_vk_sz; i += SPECIES.length()) {
      DoubleVector tmp = DoubleVector.broadcast(DOUBLE_SPECIES, 0.0d);
    }
  }

  private void apply_SDWS(
      double[] external_data, int offset, int cpu_data_sz, int cpu_vk_sz, int base) {
    for (int i = 0; i < cpu_vk_sz; i += SPECIES.length()) {
      DoubleVector tmp = DoubleVector.broadcast(DOUBLE_SPECIES, 0.0d);
    }
  }

  private void add(int st, int ed, double val) {
    int mask = SPECIES.length() - 1;
    int pre = mask & st;
    int post = mask & ed;
    int begin = st / SPECIES.length();
    int end = ed / SPECIES.length();
    for (int i = begin; i <= end; i++) {
      cpu_val.set(i, cpu_val.get(i).add(val));
    }
    if (pre != 0 && begin > 0) {
      VectorMask<Double> mk = generate_mask(pre);
      cpu_val.set(begin - 1, cpu_val.get(begin - 1).add(val, mk));
    }
    if (post != 0 && end > 0) {
      VectorMask<Double> mk = generate_mask(post);
      cpu_val.set(end + 1, cpu_val.get(end + 1).add(val, mk));
    }
  }

  private VectorMask<Double> generate_mask(int pre) {
    int mask_val = 0;
    switch (pre) {
      case 1:
        mask_val = 14;
        break;
      case 2:
        mask_val = 12;
        break;
      case 3:
        mask_val = 8;
        break;
    }
    return VectorMask.fromLong(DOUBLE_SPECIES, mask_val);
  }

  public static int log2(long val) {
    int ans = 0;
    while (val != 0) {
      val = val >> 1;
      ans++;
    }
    return ans;
  }

  public static void main(String[] args) {
    int sz = 10000000;
    TypicalSWHashing hash = new TypicalSWHashing(0, sz << 1, 1, 10);

    long[] ks = new long[sz];
    double[] val = new double[sz];
    for (int i = 0; i < sz; i++) {
      ks[i] = i * 2;
      val[i] = Math.sin(i);
    }
    int offset = 0;
    long st = System.currentTimeMillis();
    while (offset < sz) {
      DataManagement management = new DataManagement(hash);
      int res_offset = management.init_keys(ks, offset);
      management.apply_SSWS(val, offset, management.cpu_data_sz, management.cpu_vk_sz, 4);
      offset = res_offset;
    }
    System.out.println("Time cost: " + (System.currentTimeMillis() - st));
  }
}
