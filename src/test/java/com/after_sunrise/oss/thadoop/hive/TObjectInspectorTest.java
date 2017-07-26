package com.after_sunrise.oss.thadoop.hive;

import com.after_sunrise.oss.thadoop.sample.ThadoopSample;
import com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.TType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * @author takanori.takase
 */
public class TObjectInspectorTest {

    private TObjectInspector<ThadoopSample> target;

    @Before
    public void setUp() throws Exception {
        target = new TObjectInspector<ThadoopSample>(ThadoopSample.class);
    }

    @Test
    public void testToString() {
        assertEquals("TObjectInspector[clazz=ThadoopSample]", target.toString());
    }

    @Test
    public void testList() {

        List<StructField> fields = target.list(ThadoopSample.metaDataMap);

        for (int i = 0; i < _Fields.values().length; i++) {

            _Fields f = _Fields.values()[i];

            StructField sf = fields.get(i);

            assertEquals(f.getFieldName().toLowerCase(), sf.getFieldName());

        }

    }

    @Test
    public void testRetrieveInspector() {

        FieldMetaData m = ThadoopSample.metaDataMap.get(_Fields.V_STRUCT);

        ObjectInspector i = target.retrieveInspector(m.valueMetaData);

        assertTrue(i instanceof TObjectInspector);

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRetrieveInspector_Unsupported() {

        FieldValueMetaData md = new FieldValueMetaData(TType.VOID);

        target.retrieveInspector(md);

    }

    @Test
    public void testMapFieldName() {

        ObjectInspector i = mock(ObjectInspector.class);

        List<StructField> fields = Lists.newArrayList();
        fields.add(new TStructField<TFieldIdEnum>(_Fields.V_SHORT, TType.I16, i));
        fields.add(new TStructField<TFieldIdEnum>(_Fields.V_INT, TType.I32, i));

        Map<String, StructField> map = target.mapFieldName(fields);
        assertEquals(fields.size(), map.size());
        assertSame(fields.get(0), map.get("vshort"));
        assertSame(fields.get(1), map.get("vint"));

    }

    @Test
    public void testGetCategory() {
        assertSame(Category.STRUCT, target.getCategory());
    }

    @Test
    public void testGetTypeName() {
        assertEquals("ThadoopSample", target.getTypeName());
    }

    @Test
    public void testGetAllStructFieldRefs() {

        List<? extends StructField> fields = target.getAllStructFieldRefs();

        for (int i = 0; i < _Fields.values().length; i++) {

            _Fields f = _Fields.values()[i];

            StructField sf = fields.get(i);

            assertEquals(f.getFieldName().toLowerCase(), sf.getFieldName());

        }

    }

    @Test
    public void testGetStructFieldRef() {

        StructField field = target.getStructFieldRef("vshort");

        assertEquals("vshort", field.getFieldName());

        assertNull(target.getStructFieldRef("foo"));

        assertNull(target.getStructFieldRef(null));

    }

    @Test
    public void testGetStructFieldData() {

        ThadoopSample data = new ThadoopSample();
        data.setVString("hoge");

        _Fields f = _Fields.V_STRING;
        byte t = ThadoopSample.metaDataMap.get(f).valueMetaData.type;
        ObjectInspector i = javaStringObjectInspector;
        TStructField<_Fields> sf = new TStructField<_Fields>(f, t, i);

        assertEquals("hoge", target.getStructFieldData(data, sf));

    }

    @Test
    public void testGetStructFieldsDataAsList() {

        ThadoopSample data = new ThadoopSample();

        List<Object> values = target.getStructFieldsDataAsList(data);

        assertEquals(_Fields.values().length, values.size());

    }

    @Test
    public void testExtract() {

        ThadoopSample data = new ThadoopSample();
        data.setVString("hoge");

        _Fields f = _Fields.V_STRING;
        byte t = ThadoopSample.metaDataMap.get(f).valueMetaData.type;
        ObjectInspector i = javaStringObjectInspector;
        TStructField<_Fields> sf = new TStructField<_Fields>(f, t, i);

        assertEquals("hoge", target.extract(data, sf));

    }

}
