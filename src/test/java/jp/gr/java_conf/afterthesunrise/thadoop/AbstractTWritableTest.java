package jp.gr.java_conf.afterthesunrise.thadoop;

import static java.nio.ByteBuffer.allocate;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample;
import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSampleCollection;
import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopType;

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
			AbstractTWritable<ThadoopSampleCollection> {

		private final ThadoopSampleCollection base = new ThadoopSampleCollection();

		@Override
		public ThadoopSampleCollection get() {
			return base;
		}

	}

	/**
	 * Test by using byte-array-backed ObjectOutput/ObjectInput to simulate
	 * serialize and desrialize operations.
	 */
	@Test
	public void testReadWrite() throws IOException {

		// Prepare internal instance
		ThadoopSample sample = new ThadoopSample();
		sample.setFieldBoolean(Boolean.TRUE);
		sample.setFieldByte(Byte.MAX_VALUE);
		sample.setFieldShort(Short.MAX_VALUE);
		sample.setFieldInt(Integer.MAX_VALUE);
		sample.setFieldLong(Long.MAX_VALUE);
		sample.setFieldDouble(Double.MAX_VALUE);
		sample.setFieldString(toString());

		// Instantiate writable first
		SampleWritable target = new SampleWritable();
		target.get().setFieldList(Arrays.asList(sample));
		target.get().setFieldSet(Collections.singleton(ThadoopType.HOGE));
		target.get().setFieldMap(Collections.singletonMap("T", allocate(0)));

		// Check multiple invocation
		for (int i = 0; i < 3; i++) {

			// Serialize
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ObjectOutput output = new ObjectOutputStream(out);
			target.write(output);
			output.close();

			// Deserialize
			SampleWritable result = new SampleWritable();
			InputStream in = new ByteArrayInputStream(out.toByteArray());
			ObjectInput input = new ObjectInputStream(in);
			result.readFields(input);
			input.close();

			// Verify In/Out (Collection)
			assertThat(result.get().getFieldSet().size(), is(1));
			assertThat(result.get().getFieldMap().size(), is(1));

			// List
			List<ThadoopSample> list = result.get().getFieldList();
			assertThat(list.size(), is(1));
			assertEquals(sample.isFieldBoolean(), list.get(0).isFieldBoolean());
			assertEquals(sample.getFieldByte(), list.get(0).getFieldByte());
			assertEquals(sample.getFieldShort(), list.get(0).getFieldShort());
			assertEquals(sample.getFieldInt(), list.get(0).getFieldInt());
			assertEquals(sample.getFieldLong(), list.get(0).getFieldLong());
			assertEquals(sample.getFieldDouble(), list.get(0).getFieldDouble());
			assertEquals(sample.getFieldString(), list.get(0).getFieldString());

			// Set
			Set<ThadoopType> set = result.get().getFieldSet();
			assertThat(set.size(), is(1));
			assertTrue(set.contains(ThadoopType.HOGE));

			// Map
			Map<String, ByteBuffer> map = result.get().getFieldMap();
			assertThat(map.size(), is(1));
			assertEquals(allocate(0), map.get("T"));

		}

	}

}
