package jp.gr.java_conf.afterthesunrise.thadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample;

import org.junit.Test;

/**
 * @author takanori.takase
 */
public class AbstractTWritableTest {

	/**
	 * Sample subclass implementation to use in this test case. Refer to
	 * "src/thrift/idl/thadoop.thrift" for the type template.
	 */
	private static class SampleWritable extends
			AbstractTWritable<ThadoopSample> {

		private final ThadoopSample base = new ThadoopSample();

		@Override
		public ThadoopSample get() {
			return base;
		}

	}

	private void process(SampleWritable r, SampleWritable w) throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutput output = new ObjectOutputStream(out);
		r.write(output);
		output.close();

		InputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectInput input = new ObjectInputStream(in);
		w.readFields(input);
		input.close();

	}

	/**
	 * Test by using byte-array-backed ObjectOutput/ObjectInput to simulate
	 * serialize and desrialize operations.
	 */
	@Test
	public void testReadWrite() throws IOException {

		// Instantiate writable first
		SampleWritable read = new SampleWritable();
		SampleWritable write = new SampleWritable();

		//
		// Plain
		//

		process(read, write);

		assertFalse(write.get().isSetFieldBoolean());
		assertFalse(write.get().isSetFieldByte());
		assertFalse(write.get().isSetFieldShort());
		assertFalse(write.get().isSetFieldInt());
		assertFalse(write.get().isSetFieldLong());
		assertFalse(write.get().isSetFieldDouble());
		assertFalse(write.get().isSetFieldString());

		//
		// With Partial Values
		//

		read.get().clear();
		read.get().setFieldBoolean(Boolean.TRUE);
		read.get().setFieldInt(Integer.MAX_VALUE);
		read.get().setFieldDouble(Double.MAX_VALUE);

		process(read, write);

		assertTrue(write.get().isSetFieldBoolean());
		assertFalse(write.get().isSetFieldByte());
		assertFalse(write.get().isSetFieldShort());
		assertTrue(write.get().isSetFieldInt());
		assertFalse(write.get().isSetFieldLong());
		assertTrue(write.get().isSetFieldDouble());
		assertFalse(write.get().isSetFieldString());

		assertEquals(Boolean.TRUE, write.get().isFieldBoolean());
		assertEquals(Integer.MAX_VALUE, write.get().getFieldInt());
		assertEquals(Double.MAX_VALUE, write.get().getFieldDouble(), 0.1);

		//
		// Plain
		//

		read.get().clear();

		process(read, write);

		assertFalse(write.get().isSetFieldBoolean());
		assertFalse(write.get().isSetFieldByte());
		assertFalse(write.get().isSetFieldShort());
		assertFalse(write.get().isSetFieldInt());
		assertFalse(write.get().isSetFieldLong());
		assertFalse(write.get().isSetFieldDouble());
		assertFalse(write.get().isSetFieldString());

		//
		// With All Values
		//

		read.get().clear();
		read.get().setFieldBoolean(Boolean.TRUE);
		read.get().setFieldByte(Byte.MAX_VALUE);
		read.get().setFieldShort(Short.MAX_VALUE);
		read.get().setFieldInt(Integer.MAX_VALUE);
		read.get().setFieldLong(Long.MAX_VALUE);
		read.get().setFieldDouble(Double.MAX_VALUE);
		read.get().setFieldString(toString());

		process(read, write);

		assertTrue(write.get().isSetFieldBoolean());
		assertTrue(write.get().isSetFieldByte());
		assertTrue(write.get().isSetFieldShort());
		assertTrue(write.get().isSetFieldInt());
		assertTrue(write.get().isSetFieldLong());
		assertTrue(write.get().isSetFieldDouble());
		assertTrue(write.get().isSetFieldString());

		assertEquals(Boolean.TRUE, write.get().isFieldBoolean());
		assertEquals(Byte.MAX_VALUE, write.get().getFieldByte());
		assertEquals(Short.MAX_VALUE, write.get().getFieldShort());
		assertEquals(Integer.MAX_VALUE, write.get().getFieldInt());
		assertEquals(Long.MAX_VALUE, write.get().getFieldLong());
		assertEquals(Double.MAX_VALUE, write.get().getFieldDouble(), 0.1);
		assertEquals(toString(), write.get().getFieldString());

	}

}
