package com.after_sunrise.oss.thadoop.common;

import com.after_sunrise.oss.thadoop.sample.ThadoopSample;
import com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author takanori.takase
 */
public class TComparatorTest {

    private TComparator<_Fields> target;

    @Before
    public void setUp() throws Exception {
        target = new TComparator<_Fields>(V_SHORT, V_INT, V_LONG);
    }

    @Test
    public void testToString() {

        assertEquals("TComparator[{V_SHORT,V_INT,V_LONG}]", target.toString());

        assertEquals("TComparator[{" //
                        + "V_BOOLEAN,V_BYTE,V_SHORT,V_INT,V_LONG," //
                        + "V_DOUBLE,V_STRING,V_BINARY,V_LIST," //
                        + "V_SET,V_MAP,V_STRUCT}]", //
                new TComparator<_Fields>(_Fields.class).toString());

    }

    @Test
    public void testCompare() {

        ThadoopSample s1 = new ThadoopSample();
        ThadoopSample s2 = new ThadoopSample();

        List<ThadoopSample> list = new ArrayList<ThadoopSample>();
        list.add(s1);
        list.add(s2);

        Collections.sort(list, target);

        assertEquals(2, list.size());

    }

    @Test
    public void testCompare_WithValues() {

        ThadoopSample s1 = new ThadoopSample();
        ThadoopSample s2 = new ThadoopSample();
        ThadoopSample s3 = new ThadoopSample();
        ThadoopSample s4 = new ThadoopSample();

        s1.setVShort((short) 2);
        s2.setVShort((short) 2);

        s2.setVInt(4);
        s3.setVInt(4);

        s3.setVLong(3L);
        s1.setVLong(3L);

        List<ThadoopSample> list = new ArrayList<ThadoopSample>();
        list.add(s1);
        list.add(s2);
        list.add(s3);
        list.add(s4);

        Collections.shuffle(list);

        Collections.sort(list, target);

        assertEquals(4, list.size());
        assertSame(s2, list.get(0));
        assertSame(s1, list.get(1));
        assertSame(s3, list.get(2));
        assertSame(s4, list.get(3));

    }

}
