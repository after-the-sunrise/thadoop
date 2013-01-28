package jp.gr.java_conf.afterthesunrise.thadoop.partitioner;

import jp.gr.java_conf.afterthesunrise.thadoop.writable.TWritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

/**
 * @author takanori.takase
 */
public class TKeyPartitioner<F extends TFieldIdEnum> extends
		Partitioner<TWritable<? extends TBase<?, F>>, Writable> {

	private static final int INIT = 31;

	private final F[] fields;

	protected TKeyPartitioner(F... fields) {
		this.fields = fields.clone();
	}

	@Override
	public int getPartition(TWritable<? extends TBase<?, F>> key,
			Writable value, int numPartitions) {

		TBase<?, F> base = key.get();

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
