package com.after_sunrise.oss.thadoop.partitioner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import com.after_sunrise.oss.thadoop.writable.TWritable;

/**
 * @author takanori.takase
 */
public class TValuePartitioner<F extends TFieldIdEnum> extends
		Partitioner<Writable, TWritable<? extends TBase<?, F>>> {

	private static final int INIT = 31;

	private final F[] fields;

	protected TValuePartitioner(F... fields) {
		this.fields = fields.clone();
	}

	@Override
	public int getPartition(Writable key,
			TWritable<? extends TBase<?, F>> value, int numPartitions) {

		TBase<?, F> base = value.get();

		int hash = INIT;

		for (int i = 0; i < fields.length; i++) {

			F field = fields[i];

			if (base.isSet(field)) {

				Object val = base.getFieldValue(field);

				hash *= val.hashCode();

			}

		}

		return Math.abs(hash % numPartitions);

	}

}
