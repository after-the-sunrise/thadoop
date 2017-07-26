package com.after_sunrise.oss.thadoop.writable.partitioner;

import com.after_sunrise.oss.thadoop.sample.ThadoopStruct;
import com.after_sunrise.oss.thadoop.sample.ThadoopStruct._Fields;
import com.after_sunrise.oss.thadoop.writable.TWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * @author takanori.takase
 */
public class TBasePartitionerTest {

    private TBasePartitioner<_Fields, Writable, Writable> target;

    @Before
    public void setUp() throws Exception {
        target = new TBasePartitioner<_Fields, Writable, Writable>(
                _Fields.class) {
            @Override
            public int getPartition(Writable key, Writable value,
                                    int numPartitions) {
                return -1;
            }
        };
    }

    @Test
    public void testGetPartition() {

        class TSampleWritable extends TWritable<ThadoopStruct> {

            private final ThadoopStruct value = new ThadoopStruct();

            @Override
            public ThadoopStruct get() {
                return value;
            }

        }

        TSampleWritable w = new TSampleWritable();

        Random random = new Random();

        for (int i = 0; i < 1000; i++) {

            w.get().setVAuthor(String.valueOf(random.nextDouble()));

            int cluster = Math.abs(random.nextInt() % 1234) + 1;

            int partition = target.getPartition(w, cluster);

            assertTrue("Partition : " + partition, partition < cluster);

            assertTrue("Partition : " + partition, partition >= 0);

        }

    }
}
