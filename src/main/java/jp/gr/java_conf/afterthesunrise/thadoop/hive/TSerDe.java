package jp.gr.java_conf.afterthesunrise.thadoop.hive;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Properties;

import jp.gr.java_conf.afterthesunrise.thadoop.writable.TWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import com.google.common.annotations.VisibleForTesting;

/**
 * @author takanori.takase
 */
public class TSerDe<W extends TWritable<?>> implements SerDe {

	private final W writable;

	private ObjectInspector inspector;

	public TSerDe(W writable) {
		this.writable = checkNotNull(writable);
	}

	@Override
	public void initialize(Configuration conf, Properties prop)
			throws SerDeException {

		inspector = create();

	}

	@VisibleForTesting
	<T extends TBase<?, ?>> ObjectInspector create() {

		@SuppressWarnings("unchecked")
		Class<T> clazz = (Class<T>) writable.get().getClass();

		return new TObjectInspector<T>(clazz);

	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return inspector;
	}

	@Override
	public Object deserialize(Writable writable) throws SerDeException {

		@SuppressWarnings("unchecked")
		W w = (W) writable;

		return w.get();

	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return writable.getClass();
	}

	@Override
	public Writable serialize(Object data, ObjectInspector inspector)
			throws SerDeException {

		TBase<?, ?> from = (TBase<?, ?>) data;

		TBase<?, ?> to = writable.get();

		TMemoryBuffer buffer = new TMemoryBuffer(Byte.MAX_VALUE);

		TProtocol protocol = new TBinaryProtocol(buffer);

		try {

			from.write(protocol);

			to.clear();

			to.read(protocol);

		} catch (TException e) {
			throw new SerDeException(e);
		}

		return writable;

	}

}
