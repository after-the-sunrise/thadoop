package jp.gr.java_conf.afterthesunrise.thadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * <p>
 * Template implementation to serialize and deserialize a thrift instance via
 * Hadoop's {@link Writable} interface.
 * </p>
 * 
 * <p>
 * Subclass must implement {@code get()} to return the actual thrift instance to
 * be used. Given instance will always be initialized before deserialization,
 * using the {@code TBase.clear()} method, so it is safe to reuse a same
 * instance. Use {@link TBase#deepCopy()} or subtype's copy constructor in case
 * if you need to keep each desrialized object individually. A typical subclass
 * implementation may be similar to the below example:
 * </p>
 * 
 * <pre>
 * public class SomeWritable extends AbstractTWritable&lt;SomeThriftType&gt; {
 * 
 * 	private final SomeThriftType instance = new SomeThriftType();
 * 
 * 	{@code @Override}
 * 	protected SomeThriftType get() {
 * 		return instance;
 * 	}
 * 
 * }
 * </pre>
 * 
 * <p>
 * Serialized bytes consist of two parts: the data length and the data. First
 * part contains the byte length of the following data bytes, and the length is
 * serialized using {@link WritableUtils#writeVInt(DataOutput, int)}. Therefore,
 * the serialized data length must not exceed the integer's maximum value
 * (2,147,483,647 bytes). Second part contain the data in primitive byte array
 * format, using the {@link TBinaryProtocol}.
 * </p>
 * 
 * @author takanori.takase
 * 
 * @param <T>
 *            Thrift sub-type to handle.
 */
public abstract class AbstractTWritable<T extends TBase<?, ?>> implements
		Writable {

	/**
	 * Retrieve T instance. This method will only be invoked once per read or
	 * write operation.
	 * 
	 * @return T instance
	 */
	protected abstract T get();

	@Override
	public void readFields(DataInput in) throws IOException {

		int length = WritableUtils.readVInt(in);

		byte[] bytes = new byte[length];

		in.readFully(bytes);

		ByteArrayInputStream stream = new ByteArrayInputStream(bytes);

		TIOStreamTransport transport = new TIOStreamTransport(stream);

		TBinaryProtocol protocol = new TBinaryProtocol(transport);

		T base = get();

		base.clear();

		try {
			base.read(protocol);
		} catch (TException e) {
			throw new IOException(e);
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {

		ByteArrayOutputStream stream = new ByteArrayOutputStream();

		TIOStreamTransport transport = new TIOStreamTransport(stream);

		TBinaryProtocol protocol = new TBinaryProtocol(transport);

		try {
			get().write(protocol);
		} catch (TException e) {
			throw new IOException(e);
		}

		byte[] bytes = stream.toByteArray();

		WritableUtils.writeVInt(out, bytes.length);

		out.write(bytes);

	}

}
