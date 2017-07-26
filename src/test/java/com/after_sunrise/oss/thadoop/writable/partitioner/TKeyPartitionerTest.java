package com.after_sunrise.oss.thadoop.writable.partitioner;

import com.after_sunrise.oss.thadoop.sample.ThadoopStruct;
import com.after_sunrise.oss.thadoop.sample.ThadoopStruct._Fields;
import com.after_sunrise.oss.thadoop.writable.TWritable;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author takanori.takase
 */
public class TKeyPartitionerTest {

    private static class TSample extends TWritable<ThadoopStruct> {

        private final ThadoopStruct value = new ThadoopStruct();

        @Override
        public ThadoopStruct get() {
            return value;
        }

    }

    @Test
    public void testTKeyPartitioner_Class() {

        TKeyPartitioner<_Fields> target = new TKeyPartitioner<_Fields>(
                _Fields.class);

        int cluster = 8;

        int partition = target.getPartition(new TSample(), null, cluster);

        assertTrue("Partition : " + partition, partition < cluster);

        assertTrue("Partition : " + partition, partition >= 0);

    }

    @Test
    public void testTKeyPartitioner_Array() {

        TKeyPartitioner<_Fields> target = new TKeyPartitioner<_Fields>(
                _Fields.values());

        int cluster = 8;

        int partition = target.getPartition(new TSample(), null, cluster);

        assertTrue("Partition : " + partition, partition < cluster);

        assertTrue("Partition : " + partition, partition >= 0);

    }

}
