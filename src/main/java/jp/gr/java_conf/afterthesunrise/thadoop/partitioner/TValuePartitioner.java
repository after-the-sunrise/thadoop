package jp.gr.java_conf.afterthesunrise.thadoop.partitioner;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

/**
 * @author takanori.takase
 */
public class TValuePartitioner<F extends TFieldIdEnum, T extends TBase<?, F>>
		extends Partitioner<Writable, T> {

	private static final int INIT = 31;

	private final F[] fields;

	protected TValuePartitioner(F... fields) {
		this.fields = fields.clone();
	}

	@Override
	public int getPartition(Writable key, T value, int numPartitions) {

		int hash = INIT;

		for (int i = 0; i < fields.length; i++) {

			F field = fields[i];

			if (value.isSet(field)) {

				Object val = value.getFieldValue(field);

				hash *= val.hashCode();

			}

		}

		return Math.abs(hash % numPartitions);

	}

}