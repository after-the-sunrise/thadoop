package com.after_sunrise.oss.thadoop.common;

import org.apache.thrift.protocol.TType;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.*;

/**
 * @author takanori.takase
 */
public class TFieldTypeTest {

    @Test
    public void testFind() {

        for (TFieldType type : TFieldType.values()) {
            assertSame(type, TFieldType.find(type.getId()));
        }

        assertNull(TFieldType.find(Byte.MIN_VALUE));

        assertNull(TFieldType.find(Byte.MAX_VALUE));

    }

    @Test
    public void testCoverage() throws Exception {

        Field[] fields = TType.class.getFields();

        int count = 0;

        for (Field f : fields) {

            int modifier = f.getModifiers();

            if (!Modifier.isPublic(modifier)) {
                continue;
            }

            if (!Modifier.isStatic(modifier)) {
                continue;
            }

            byte id = (Byte) f.get(null);

            assertNotNull(f.getName(), TFieldType.find(id));

            count++;

        }

        assertEquals(TFieldType.values().length, count);

    }

}
