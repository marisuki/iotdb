package org.apache.iotdb.db.queryengine.version.common;

import org.apache.iotdb.db.queryengine.version.exception.VersionOperatorTypeException;
import org.apache.iotdb.db.queryengine.version.operators.InnerJoinOperator;
import org.apache.iotdb.db.queryengine.version.operators.LazyComputeOperator;

public enum BinaryOperator {
    INNER_JOIN,
    LEFT_OUTER_JOIN,
    RIGHT_OUTER_JOIN,
    FULL_OUTER_JOIN,
    UNION,
    INTERSECT,
    EMPTY;

    public static VersionOperator get(BinaryOperator operator) {
        switch (operator) {
            case INNER_JOIN: return new InnerJoinOperator();
            default: throw new VersionOperatorTypeException("This Version Operator is not supported");
        }
    }
}
