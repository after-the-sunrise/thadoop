package jp.gr.java_conf.afterthesunrise.thadoop.comparator;

import static org.apache.commons.collections.ComparatorUtils.NATURAL_COMPARATOR;
import static org.apache.commons.collections.ComparatorUtils.nullHighComparator;

import java.util.Comparator;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

/**
 * @author takanori.takase
 */
public class TComparator<F extends TFieldIdEnum> implements
		Comparator<TBase<?, F>> {

	@SuppressWarnings("unchecked")
	private static final Comparator<Object> COMPARATOR = nullHighComparator(NATURAL_COMPARATOR);

	private final F[] fields;

	public TComparator(F... fields) {
		this.fields = fields.clone();
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

	protected int compare(F field, Object o1, Object o2) {
		return COMPARATOR.compare(o1, o2);
	}

}
