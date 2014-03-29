package com.after_sunrise.oss.thadoop.writable.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import com.after_sunrise.oss.thadoop.writable.TWritable;

/**
 * {@link Partitioner} helper implementation to partition {@link TWritable}
 * instances by hashing the specified field values.
 * 
 * @author takanori.takase
 */
public abstract class TBasePartitioner<F extends Enum<F> & TFieldIdEnum, K, V>
		extends Partitioner<K, V> {

	private static final int INIT = 31;

	private final F[] fields;

	/**
	 * Create instance using all elements.
	 * 
	 * @param clazz
	 *            {@code _Field} class.
	 */
	protected TBasePartitioner(Class<F> clazz) {
		this(clazz.getEnumConstants());
	}

	/**
	 * Create instance using only the specified elements.
	 * 
	 * @param fields
	 *            {@code _Field} elements to use.
	 */
	protected TBasePartitioner(F... fields) {
		this.fields = fields.clone();
	}

	/**
	 * Calculate partition index from the {@link TWritable} instance. {@code F}
	 * values are used for extracting the field values.
	 * 
	 * Each field values are hashed individually, and modular of the product is
	 * calculated. To avoid negative index, absolute is used as final result. If
	 * a field is null, a constant value is used as a substitute.
	 * 
	 * @param value
	 *            Instance to calculate the hash
	 * @param numPartitions
	 *            Number of partitions.
	 * @return Calculated partition index
	 */
	protected int getPartition(TWritable<? extends TBase<?, F>> value,
			int numPartitions) {

		TBase<?, F> base = value.get();

		int hash = INIT;

		for (int i = 0; i < fields.length; i++) {

			F field = fields[i];

			if (base.isSet(field)) {

				Object val = base.getFieldValue(field);

				hash *= val.hashCode();

			} else {

				hash *= INIT;

			}

		}

		return Math.abs(hash % numPartitions);

	}

}
