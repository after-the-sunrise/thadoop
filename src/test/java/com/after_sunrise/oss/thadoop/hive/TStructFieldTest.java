package com.after_sunrise.oss.thadoop.hive;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.thrift.protocol.TType;
import org.junit.Before;
import org.junit.Test;

import com.after_sunrise.oss.thadoop.sample.ThadoopStruct;
import com.after_sunrise.oss.thadoop.sample.ThadoopStruct._Fields;

/**
 * @author takanori.takase
 */
public class TStructFieldTest {

	private TStructField<_Fields> target;

	@Before
	public void setUp() throws Exception {

		_Fields f = _Fields.V_AUTHOR;

		byte t = ThadoopStruct.metaDataMap.get(f).valueMetaData.type;

		ObjectInspector i = javaStringObjectInspector;

		target = new TStructField<_Fields>(f, t, i);

	}

	@Test
	public void testToString() {
		assertEquals(
				"TStructField[field=V_AUTHOR,type=11,inspector=JavaStringObjectInspector]",
				target.toString());
	}

	@Test
	public void testGetField() {
		assertEquals(_Fields.V_AUTHOR, target.getField());
	}

	@Test
	public void testGetType() {
		assertEquals(TType.STRING, target.getType());
	}

	@Test
	public void testGetFieldName() {
		assertEquals("vauthor", target.getFieldName());
	}

	@Test
	public void testGetFieldComment() {
		assertEquals(target.toString(), target.getFieldComment());
	}

	@Test
	public void testGetFieldObjectInspector() {
		assertSame(javaStringObjectInspector, target.getFieldObjectInspector());
	}

}
