package com.after_sunrise.oss.thadoop.common;

import org.apache.thrift.TFieldIdEnum;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare natural ordering of {@code TFieldIdEnum#getThriftFieldId()}.
 *
 * @param <T> Thrift fields to compare.
 * @author takanori.takase
 */
public final class TFieldIdEnumComparator<T extends TFieldIdEnum> implements
        Comparator<T>, Serializable {

    private static final long serialVersionUID = 7750731494329633039L;

    @SuppressWarnings("rawtypes")
    private static final Comparator INSTANCE = new TFieldIdEnumComparator();

    /**
     * Retrieve a singleton instance of the comparator.
     *
     * @return singleton instance
     */
    @SuppressWarnings("unchecked")
    public static <T extends TFieldIdEnum> Comparator<T> get() {
        return INSTANCE;
    }

    private TFieldIdEnumComparator() {
    }

    @Override
    public int compare(T o1, T o2) {
        return o1.getThriftFieldId() - o2.getThriftFieldId();
    }

}
