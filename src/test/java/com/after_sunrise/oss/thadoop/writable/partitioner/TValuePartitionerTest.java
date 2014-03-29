package com.after_sunrise.oss.thadoop.writable.partitioner;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.after_sunrise.oss.thadoop.sample.ThadoopStruct;
import com.after_sunrise.oss.thadoop.sample.ThadoopStruct._Fields;
import com.after_sunrise.oss.thadoop.writable.TWritable;

/**
 * @author takanori.takase
 */
public class TValuePartitionerTest {

	private static class TSample extends TWritable<ThadoopStruct> {

		private final ThadoopStruct value = new ThadoopStruct();

		@Override
		public ThadoopStruct get() {
			return value;
		}

	}

	@Test
	public void testTValuePartitioner_Class() {

		TValuePartitioner<_Fields> target = new TValuePartitioner<_Fields>(
				_Fields.class);

		int cluster = 8;

		int partition = target.getPartition(null, new TSample(), cluster);

		assertTrue("Partition : " + partition, partition < cluster);

		assertTrue("Partition : " + partition, partition >= 0);

	}

	@Test
	public void testTValuePartitioner_Array() {

		TValuePartitioner<_Fields> target = new TValuePartitioner<_Fields>(
				_Fields.values());

		int cluster = 8;

		int partition = target.getPartition(null, new TSample(), cluster);

		assertTrue("Partition : " + partition, partition < cluster);

		assertTrue("Partition : " + partition, partition >= 0);

	}

}
