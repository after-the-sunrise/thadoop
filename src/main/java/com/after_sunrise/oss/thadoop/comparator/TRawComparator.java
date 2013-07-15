package com.after_sunrise.oss.thadoop.comparator;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOError;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import com.after_sunrise.oss.thadoop.writable.TWritable;

/**
 * @author takanori.takase
 */
public class TRawComparator<F extends TFieldIdEnum> implements
		RawComparator<TWritable<? extends TBase<?, F>>> {

	private final DataInputBuffer buffer = new DataInputBuffer();

	private final TWritable<? extends TBase<?, F>> writable1;

	private final TWritable<? extends TBase<?, F>> writable2;

	private final TComparator<F> comparator;

	protected TRawComparator(
			Class<? extends TWritable<? extends TBase<?, F>>> clazz,
			F... fields) {
		this(clazz, new TComparator<F>(fields));
	}

	protected TRawComparator(
			Class<? extends TWritable<? extends TBase<?, F>>> clazz,
			TComparator<F> comparator) {

		try {

			this.writable1 = clazz.newInstance();

			this.writable2 = clazz.newInstance();

			this.comparator = checkNotNull(comparator);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		try {

			buffer.reset(b1, s1, l1);
			writable1.get().clear();
			writable1.readFields(buffer);

			buffer.reset(b2, s2, l2);
			writable2.get().clear();
			writable2.readFields(buffer);

		} catch (IOException e) {
			throw new IOError(e);
		}

		return compare(writable1, writable2);

	}

	@Override
	public int compare(TWritable<? extends TBase<?, F>> o1,
			TWritable<? extends TBase<?, F>> o2) {

		TBase<?, F> b1 = o1.get();

		TBase<?, F> b2 = o2.get();

		return comparator.compare(b1, b2);

	}

}
