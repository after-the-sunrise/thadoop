package jp.gr.java_conf.afterthesunrise.thadoop.hive;

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

/**
 * @author takanori.takase
 */
public class TSerDe implements SerDe {

	public static final String TWRITABLE = "tserde.writable";

	private final Class<? extends TWritable<?>> clazz;

	private TWritable<?> writable;

	private TObjectInspector<?> inspector;

	public TSerDe() {
		this(null);
	}

	public TSerDe(Class<? extends TWritable<?>> clazz) {
		this.clazz = clazz;
	}

	@Override
	public void initialize(Configuration conf, Properties prop)
			throws SerDeException {

		try {

			if (clazz == null) {

				String className = prop.getProperty(TWRITABLE);

				Class<?> clz = Class.forName(className);

				writable = (TWritable<?>) clz.newInstance();

			} else {

				writable = clazz.newInstance();

			}

			TBase<?, ?> base = writable.get();

			inspector = create(base.getClass());

		} catch (Exception e) {
			throw new SerDeException(e);
		}

	}

	private <T extends TBase<?, ?>> TObjectInspector<T> create(Class<T> clazz) {
		return new TObjectInspector<T>(clazz);
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return inspector;
	}

	@Override
	public Object deserialize(Writable writable) throws SerDeException {

		TWritable<?> w = (TWritable<?>) writable;

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
