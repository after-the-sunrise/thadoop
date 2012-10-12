package jp.gr.java_conf.afterthesunrise.thadoop;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.thrift.TFieldIdEnum;

/**
 * Compare natural ordering of {@code TFieldIdEnum#getThriftFieldId()}.
 * 
 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
 * 
 * @author takanori.takase
 * @param <F>
 *            Thrift type to compare.
 */
public final class TFieldIdEnumComparator<T extends TFieldIdEnum> implements
		Comparator<T>, Serializable {

	private static final long serialVersionUID = 7750731494329633039L;

	@SuppressWarnings("rawtypes")
	private static final Comparator INSTANCE = new TFieldIdEnumComparator<>();

	@SuppressWarnings("unchecked")
	public static <T extends TFieldIdEnum> Comparator<T> get() {
		return INSTANCE;
	}

	private TFieldIdEnumComparator() {
	}

	@Override
	public int compare(T o1, T o2) {
		return Short.compare(o1.getThriftFieldId(), o2.getThriftFieldId());
	}

}
