package com.after_sunrise.oss.thadoop.pig;

import static com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields.V_BOOLEAN;
import static com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields.V_BYTE;
import static com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields.V_DOUBLE;
import static com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields.V_INT;
import static com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields.V_LONG;
import static com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields.V_SHORT;
import static com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields.V_STRING;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.thrift.meta_data.FieldMetaData;
import org.junit.Before;
import org.junit.Test;

import com.after_sunrise.oss.thadoop.sample.ThadoopSample;
import com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields;
import com.after_sunrise.oss.thadoop.writable.TWritable;

/**
 * @author takanori.takase
 */
public class TStorageTest {

	/**
	 * Sample writable implementation for test.
	 */
	private class SampleWritable extends TWritable<ThadoopSample> {

		private final ThadoopSample value = new ThadoopSample();

		@Override
		public ThadoopSample get() {
			return value;
		}

	}

	/**
	 * Sample storage implementation for test.
	 */
	private class SampleStorage extends TStorage<_Fields, SampleWritable> {

		protected SampleStorage() {
			super(SampleWritable.class, ThadoopSample.metaDataMap);
		}

	}

	private TStorage<_Fields, SampleWritable> target;

	@Before
	public void setUp() {
		target = new SampleStorage();
	}

	@Test
	public void testSetLocation() throws IOException {

		Job job = Job.getInstance();

		target.setLocation("hdfs://localhost:80/test", job);

		assertEquals("hdfs://localhost:80/test",
				job.getConfiguration().get(FileInputFormat.INPUT_DIR));

	}

	@Test
	public void testGetInputFormat() throws IOException {
		assertTrue(target.getInputFormat() instanceof SequenceFileInputFormat);
	}

	@Test
	public void testGetNext() throws IOException, InterruptedException {

		// Sample thrift instance
		SampleWritable writable = new SampleWritable();
		ThadoopSample sample = writable.get();
		sample.setVBoolean(Boolean.TRUE);
		sample.setVByte(Byte.MAX_VALUE);
		sample.setVShort(Short.MAX_VALUE);
		sample.setVInt(Integer.MAX_VALUE);
		sample.setVLong(Long.MAX_VALUE);
		sample.setVDouble(Double.MAX_VALUE);
		sample.setVString(toString());

		// Mocked reader
		@SuppressWarnings("unchecked")
		RecordReader<?, SampleWritable> reader = mock(RecordReader.class);
		when(reader.nextKeyValue()).thenReturn(true, false);
		when(reader.getCurrentValue()).thenReturn(writable);
		target.prepareToRead(reader, null);

		// First record (null)
		Tuple tuple = target.getNext();
		assertThat(tuple, notNullValue());
		assertThat(tuple.size(), equalTo(12));
		assertEquals(sample.isVBoolean(), tuple.get(0));
		assertEquals(sample.getVByte(), tuple.get(1));
		assertEquals(sample.getVShort(), tuple.get(2));
		assertEquals(sample.getVInt(), tuple.get(3));
		assertEquals(sample.getVLong(), tuple.get(4));
		assertEquals(sample.getVDouble(), tuple.get(5));
		assertEquals(sample.getVString(), tuple.get(6));

		// Second record (null)
		assertNull(target.getNext());

	}

	@Test(expected = IOException.class)
	public void testGetNext_Interrupted() throws Exception {

		@SuppressWarnings("unchecked")
		RecordReader<?, SampleWritable> reader = mock(RecordReader.class);

		doThrow(new InterruptedException("test")).when(reader).nextKeyValue();

		target.prepareToRead(reader, null);

		target.getNext();

	}

	@Test
	public void testGetSchema() throws IOException {

		ResourceSchema schema = target.getSchema(null, null);

		ResourceFieldSchema[] fields = schema.getFields();

		assertThat(fields.length, equalTo(12));

		// Check field names
		assertThat(fields[0].getName(), equalTo(V_BOOLEAN.getFieldName()));
		assertThat(fields[1].getName(), equalTo(V_BYTE.getFieldName()));
		assertThat(fields[2].getName(), equalTo(V_SHORT.getFieldName()));
		assertThat(fields[3].getName(), equalTo(V_INT.getFieldName()));
		assertThat(fields[4].getName(), equalTo(V_LONG.getFieldName()));
		assertThat(fields[5].getName(), equalTo(V_DOUBLE.getFieldName()));
		assertThat(fields[6].getName(), equalTo(V_STRING.getFieldName()));

		// Check field data types
		assertThat(fields[0].getType(), equalTo(DataType.BOOLEAN));
		assertThat(fields[1].getType(), equalTo(DataType.BYTE));
		assertThat(fields[2].getType(), equalTo(DataType.INTEGER));
		assertThat(fields[3].getType(), equalTo(DataType.INTEGER));
		assertThat(fields[4].getType(), equalTo(DataType.LONG));
		assertThat(fields[5].getType(), equalTo(DataType.DOUBLE));
		assertThat(fields[6].getType(), equalTo(DataType.CHARARRAY));

	}

	@Test(expected = IOException.class)
	public void testGetSchema_NoPigDataType() throws IOException {

		new SampleStorage() {
			@Override
			protected Byte extractDataType(_Fields f, FieldMetaData d,
					Map<Byte, Byte> m) {
				return null;
			}

		}.getSchema(null, null);

	}

	@Test
	public void testGetStatistics() {
		assertNull(target.getStatistics(null, null));
	}

	@Test
	public void testGetPartitionKeys() {
		assertNull(target.getPartitionKeys(null, null));
	}

	@Test
	public void testSetPartitionFilter() {
		target.setPartitionFilter(null);
	}

}
