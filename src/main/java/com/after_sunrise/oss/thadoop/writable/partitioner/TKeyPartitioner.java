package com.after_sunrise.oss.thadoop.writable.partitioner;

import com.after_sunrise.oss.thadoop.writable.TWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

/**
 * {@link Partitioner} implementation to partition {@link TWritable} key
 * instances by hashing the specified field values.
 *
 * @author takanori.takase
 */
public class TKeyPartitioner<F extends Enum<F> & TFieldIdEnum> extends
        TBasePartitioner<F, TWritable<? extends TBase<?, F>>, Writable> {

    /**
     * Create partitioner using all elements.
     *
     * @param clazz {@code _Field} class.
     */
    protected TKeyPartitioner(Class<F> clazz) {
        super(clazz);
    }

    /**
     * Create partitioner using only the specified elements.
     *
     * @param fields {@code _Field} elements to use.
     */
    protected TKeyPartitioner(F... fields) {
        super(fields);
    }

    /**
     * Calculate partition index from the given key.
     *
     * @see TBasePartitioner#getPartition(TWritable, int)
     */
    @Override
    public int getPartition(TWritable<? extends TBase<?, F>> key,
                            Writable value, int numPartitions) {
        return super.getPartition(key, numPartitions);
    }

}
