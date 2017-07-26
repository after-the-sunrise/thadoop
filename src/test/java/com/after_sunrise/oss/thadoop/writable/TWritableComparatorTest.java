package com.after_sunrise.oss.thadoop.writable;

import com.after_sunrise.oss.thadoop.sample.ThadoopStruct;
import com.after_sunrise.oss.thadoop.sample.ThadoopStruct._Fields;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author takanori.takase
 */
public class TWritableComparatorTest {

    static class TSample extends TWritable<ThadoopStruct> {

        private final ThadoopStruct value = new ThadoopStruct();

        @Override
        public ThadoopStruct get() {
            return value;
        }

    }

    private static class TPrivate extends TSample {
    }

    private TWritableComparator<_Fields> target;

    @Before
    public void setUp() throws Exception {
        target = new TWritableComparator<_Fields>(TSample.class, _Fields.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrivateClass() {
        target = new TWritableComparator<_Fields>(TPrivate.class, _Fields.class);
    }

    @Test
    public void testCompare_Bytes() throws IOException {

        TSample s1 = new TSample();
        TSample s2 = new TSample();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(out);

        s1.write(ds);
        ds.flush();
        byte[] b1 = out.toByteArray();

        out.reset();

        s2.write(ds);
        ds.flush();
        byte[] b2 = out.toByteArray();

        assertEquals(0, target.compare(b1, 0, b1.length, b2, 0, b2.length));

    }

    @Test
    public void testCompare_Bytes_WithValues() throws IOException {

        TSample s1 = new TSample();
        TSample s2 = new TSample();

        s1.get().setVAuthor("foo");
        s2.get().setVAuthor("bar");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(out);

        s1.write(ds);
        ds.flush();
        byte[] b1 = out.toByteArray();

        out.reset();

        s2.write(ds);
        ds.flush();
        byte[] b2 = out.toByteArray();

        assertTrue(target.compare(b1, 0, b1.length, b2, 0, b2.length) > 0);
        assertTrue(target.compare(b2, 0, b2.length, b1, 0, b1.length) < 0);

    }

    @Test(expected = IOError.class)
    public void testCompare_Error() throws IOException {

        byte[] b1 = {0, 1, 2};

        byte[] b2 = {3, 4, 5};

        target.compare(b1, 0, b1.length, b2, 0, b2.length);

    }

}
