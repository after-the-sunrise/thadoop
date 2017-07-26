package com.after_sunrise.oss.thadoop.writable.partitioner;

import com.after_sunrise.oss.thadoop.writable.TWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

/**
 * {@link Partitioner} implementation to partition {@link TWritable} value
 * instances by hashing the specified field values.
 *
 * @author takanori.takase
 */
public class TValuePartitioner<F extends Enum<F> & TFieldIdEnum> extends
        TBasePartitioner<F, Writable, TWritable<? extends TBase<?, F>>> {

    /**
     * Create partitioner using all elements.
     *
     * @param clazz {@code _Field} class.
     */
    protected TValuePartitioner(Class<F> clazz) {
        super(clazz);
    }

    /**
     * Create partitioner using only the specified elements.
     *
     * @param fields {@code _Field} elements to use.
     */
    protected TValuePartitioner(F... fields) {
        super(fields);
    }

    /**
     * Calculate partition index from the given value.
     *
     * @see TBasePartitioner#getPartition(TWritable, int)
     */
    @Override
    public int getPartition(Writable key,
                            TWritable<? extends TBase<?, F>> value, int numPartitions) {
        return super.getPartition(value, numPartitions);
    }

}
