package com.after_sunrise.oss.thadoop.writable;

import com.after_sunrise.oss.thadoop.common.TComparator;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import java.io.IOError;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Comparator for {@link TWritable} using {@link TFieldIdEnum}. Each field is
 * extracted from the underlying {@link TBase} instance and compared until the
 * sort order is determined.
 *
 * @param <F> Field type used for extracting field values from instances.
 * @author takanori.takase
 */
public class TWritableComparator<F extends Enum<F> & TFieldIdEnum> implements
        RawComparator<TWritable<? extends TBase<?, F>>> {

    private final DataInputBuffer buffer = new DataInputBuffer();

    private final TWritable<? extends TBase<?, F>> writable1;

    private final TWritable<? extends TBase<?, F>> writable2;

    private final TComparator<F> comparator;

    /**
     * Create comparator using all elements.
     * <p>
     * Uses {@link TComparator} for instance comparison.
     *
     * @param clazz      Class to create instance for deserialization.
     * @param fieldClazz {@code _Field} class.
     */
    public TWritableComparator(
            Class<? extends TWritable<? extends TBase<?, F>>> clazz,
            Class<F> fieldClazz) {
        this(clazz, fieldClazz.getEnumConstants());
    }

    /**
     * Create comparator using only the specified elements.
     * <p>
     * Uses {@link TComparator} for instance comparison.
     *
     * @param clazz  Class to create instance for deserialization.
     * @param fields {@code _Field} instances.
     */
    public TWritableComparator(
            Class<? extends TWritable<? extends TBase<?, F>>> clazz,
            F... fields) {
        this(clazz, new TComparator<F>(fields));
    }

    /**
     * Create comparator using the specified underlying comparator.
     * <p>
     * Uses {@link TComparator} for instance comparison.
     *
     * @param clazz      Class to create instance for deserialization.
     * @param comparator Comparator to use for comparing the underlying {@link TBase}
     *                   instances.
     */
    public TWritableComparator(
            Class<? extends TWritable<? extends TBase<?, F>>> clazz,
            TComparator<F> comparator) {

        try {

            this.writable1 = clazz.newInstance();

            this.writable2 = clazz.newInstance();

            this.comparator = checkNotNull(comparator);

        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

    }

    /**
     * Deserialize byte arrays to instance and compare. Actual comparison is
     * delegated to {@link TWritableComparator#compare(TWritable, TWritable)}
     * method.
     */
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

    /**
     * Compare underlying {@link TBase} instances.
     */
    @Override
    public int compare(TWritable<? extends TBase<?, F>> o1,
                       TWritable<? extends TBase<?, F>> o2) {

        TBase<?, F> b1 = o1.get();

        TBase<?, F> b2 = o2.get();

        return comparator.compare(b1, b2);

    }

}
