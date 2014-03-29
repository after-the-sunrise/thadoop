package com.after_sunrise.oss.thadoop.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Before;
import org.junit.Test;

import com.after_sunrise.oss.thadoop.sample.ThadoopStruct;
import com.after_sunrise.oss.thadoop.writable.TWritable;

/**
 * @author takanori.takase
 */
public class TSerDeTest {

	static class TSample extends TWritable<ThadoopStruct> {

		private final ThadoopStruct value = new ThadoopStruct();

		@Override
		public ThadoopStruct get() {
			return value;
		}

	}

	private TSerDe target;

	@Before
	public void setUp() throws Exception {
		target = new TSerDe(TSample.class);
	}

	@Test
	public void testInitialize_FromProperty() throws SerDeException {

		Properties prop = new Properties();
		prop.put(TSerDe.TWRITABLE, TSample.class.getName());

		target = new TSerDe();

		assertNull(target.getObjectInspector());

		target.initialize(null, prop);

		assertNotNull(target.getObjectInspector());

	}

	@Test(expected = SerDeException.class)
	public void testInitialize_FromProperty_Invalid() throws SerDeException {

		Properties prop = new Properties();

		new TSerDe().initialize(null, prop);

	}

	@Test
	public void testInitialize() throws SerDeException {

		assertNull(target.getObjectInspector());

		target.initialize(null, null);

		assertNotNull(target.getObjectInspector());

	}

	@Test
	public void testGetObjectInspector() throws SerDeException {
		assertNull(target.getObjectInspector());
	}

	@Test
	public void testDeserialize() throws SerDeException {

		TSample writable = new TSample();

		Object o = target.deserialize(writable);

		assertSame(writable.get(), o);

	}

	@Test
	public void testGetSerDeStats() {
		assertNull(target.getSerDeStats());
	}

	@Test
	public void testGetSerializedClass() throws SerDeException {

		target.initialize(null, null);

		assertSame(TSample.class, target.getSerializedClass());

	}

	@Test
	public void testSerialize() throws SerDeException {

		target.initialize(null, null);

		ThadoopStruct data = new ThadoopStruct();

		data.setVAuthor("test");

		TSample writable = (TSample) target.serialize(data, null);

		assertEquals("test", writable.get().getVAuthor());

	}

	@Test(expected = SerDeException.class)
	public void testSerialize_Exception() throws SerDeException, TException {

		target.initialize(null, null);

		ThadoopStruct data = spy(new ThadoopStruct());

		doThrow(new TException("test")).when(data).write(any(TProtocol.class));

		target.serialize(data, null);

	}

}
