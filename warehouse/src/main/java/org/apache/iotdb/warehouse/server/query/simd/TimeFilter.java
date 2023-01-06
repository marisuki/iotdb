package org.apache.iotdb.warehouse.server.query.simd;

import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.warehouse.common.Dataset;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class TimeFilter extends UnaryOperator {

    long st, ed;
    boolean incl1, incl2;

    public TimeFilter(long from, long to, boolean incl_start, boolean incl_end) {
        st = from; ed = to; incl1 = incl_start; incl2 = incl_end;
    }

    @Override
    public Dataset transform(Dataset data, int refer_id) {
        Dataset ans = new Dataset("" + refer_id);
        ans.setSchema(data.getSchema());
        ans.filter_pos = data.filter_pos;
        ans.version_pos = data.version_pos;
        Stream<RowRecord> d = data.buffer.getDataByName(data.getName());
        Stream<RowRecord> trans = d.filter(new Predicate<RowRecord>() {
            @Override
            public boolean test(RowRecord rowRecord) {
                if((incl1 && rowRecord.getTimestamp() >= st) ||
                        (!incl1 && rowRecord.getTimestamp() > st)) {
                    return (incl2 && rowRecord.getTimestamp() <= ed) ||
                            (!incl2 && rowRecord.getTimestamp() < ed);
                }
                return false;
            }
        });
        ans.setBuffer(data.getBuffer());
        data.getBuffer().createBuffer(""+refer_id, trans);
        return ans;
    }

    @Override
    public String getOperatorName() {
        return "TimeFilter()";
    }
}
