package com.after_sunrise.oss.thadoop.common;

import static org.apache.commons.collections.ComparatorUtils.NATURAL_COMPARATOR;
import static org.apache.commons.collections.ComparatorUtils.nullHighComparator;
import static org.apache.commons.lang.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import java.util.Comparator;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

/**
 * Comparator for {@link TBase} using {@link TFieldIdEnum}. Each field is
 * extracted and compared until the sort order is determined.
 * 
 * This implementations implicitly assumes that all given {@link TFieldIdEnum}
 * fields are {@link Comparable}. Null values are treated as higher than
 * non-null values.
 * 
 * @author takanori.takase
 * @param <F>
 *            Field type used for extracting field values from instances.
 */
public class TComparator<F extends Enum<F> & TFieldIdEnum> implements
		Comparator<TBase<?, F>> {

	@SuppressWarnings("unchecked")
	private static final Comparator<Object> COMPARATOR = nullHighComparator(NATURAL_COMPARATOR);

	private final F[] fields;

	/**
	 * Create comparator using all elements.
	 * 
	 * @param clazz
	 *            {@code _Field} class.
	 */
	public TComparator(Class<F> clazz) {
		this(clazz.getEnumConstants());
	}

	/**
	 * Create comparator using only the specified elements.
	 * 
	 * @param fields
	 *            {@code _Field} elements to use.
	 */
	public TComparator(F... fields) {
		this.fields = fields.clone();
	}

	@Override
	public String toString() {
		ToStringBuilder b = new ToStringBuilder(this, SHORT_PREFIX_STYLE);
		b.append(fields);
		return b.toString();
	}

	@Override
	public int compare(TBase<?, F> o1, TBase<?, F> o2) {

		int result = 0;

		for (int i = 0; i < fields.length && result == 0; i++) {

			F field = fields[i];

			Object v1 = o1.isSet(field) ? o1.getFieldValue(field) : null;

			Object v2 = o2.isSet(field) ? o2.getFieldValue(field) : null;

			result = compare(field, v1, v2);

		}

		return result;

	}

	/**
	 * Override this method for customized value comparison.
	 */
	protected int compare(F field, Object o1, Object o2) {
		return COMPARATOR.compare(o1, o2);
	}

}
