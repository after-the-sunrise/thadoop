package jp.gr.java_conf.afterthesunrise.thadoop.writable;

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
import java.util.Arrays;
import java.util.Collections;

import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample;
import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample._Fields;
import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopStruct;
import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopType;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * @author takanori.takase
 */
public class TWritableTest {

	/**
	 * Sample subclass implementation to use in this test case. Refer to
	 * "src/thrift/idl/thadoop.thrift" for the type template.
	 */
	public static class SampleWritable extends TWritable<ThadoopSample> {

		private final ThadoopSample base = new ThadoopSample();

		@Override
		public ThadoopSample get() {
			return base;
		}

	}

	private ThadoopSample sample1;

	private ThadoopSample sample2;

	private SampleWritable writable1;

	private SampleWritable writable2;

	@Before
	public void setUp() {

		writable1 = new SampleWritable();
		writable2 = new SampleWritable();

		sample1 = writable1.get();
		sample2 = writable2.get();

	}

	/**
	 * Convenient method to read from r, and write to w.
	 */
	private void process(TWritable<?> r, TWritable<?> w) throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutput output = new ObjectOutputStream(out);
		r.write(output);
		output.close();

		InputStream in = new ByteArrayInputStream(out.toByteArray());
		ObjectInput input = new ObjectInputStream(in);
		w.readFields(input);
		input.close();

	}

	@Test
	public void testReadWrite_NoValues() throws IOException {

		sample1.clear();

		process(writable1, writable2);

		for (_Fields f : _Fields.values()) {
			assertFalse(sample2.isSet(f));
		}

	}

	@Test
	public void testReadWrite_AllValues() throws IOException {

		sample1.clear();

		ThadoopStruct struct = new ThadoopStruct();
		struct.setVAuthor("Tak");
		struct.setVType(ThadoopType.FOO);
		struct.setVVersion(1);

		sample1.setVBinary(new byte[] { 1, 2, 3 });
		sample1.setVBoolean(true);
		sample1.setVByte((byte) 1);
		sample1.setVDouble(1.1);
		sample1.setVInt(2);
		sample1.setVList(Arrays.asList(3));
		sample1.setVLong(4L);
		sample1.setVMap(Collections.singletonMap(5, 6L));
		sample1.setVSet(Sets.newHashSet(7));
		sample1.setVShort((short) 8);
		sample1.setVString("test");
		sample1.setVStruct(struct);

		// Check all fields are set.
		for (_Fields f : _Fields.values()) {
			assertTrue(sample1.isSet(f));
		}

		process(writable1, writable2);

		for (_Fields f : _Fields.values()) {

			assertTrue(sample2.isSet(f));

			Object actual = sample2.getFieldValue(f);

			Object expect = sample1.getFieldValue(f);

			if (f == _Fields.V_BINARY) {
				assertTrue(Arrays.equals((byte[]) expect, (byte[]) actual));
			} else {
				assertEquals(expect, actual);
			}

		}

	}

	@Test
	public void testReadWrite_Sequence() throws IOException {

		testReadWrite_NoValues();

		testReadWrite_AllValues();

		testReadWrite_NoValues();

		testReadWrite_NoValues();

		testReadWrite_AllValues();

		testReadWrite_AllValues();

		testReadWrite_NoValues();

	}

}
