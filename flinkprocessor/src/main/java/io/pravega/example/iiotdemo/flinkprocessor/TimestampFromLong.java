package io.pravega.example.iiotdemo.flinkprocessor;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP;

// Flink SQL user-defined function to convert a long timestamp (milliseconds since epoch) to SQL TIMESTAMP type.
public class TimestampFromLong extends ScalarFunction {
    public long eval(long t) {
        return t;
    }

    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return TIMESTAMP;
    }
}
