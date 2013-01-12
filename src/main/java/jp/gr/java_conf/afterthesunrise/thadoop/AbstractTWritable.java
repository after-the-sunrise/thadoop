package jp.gr.java_conf.afterthesunrise.thadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.apache.thrift.transport.TMemoryInputTransport;

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
 * if you need to keep each deserialized object individually. A typical subclass
 * implementation may be similar to the below example:
 * </p>
 * 
 * <pre>
 * public class SomeWritable extends AbstractTWritable&lt;SomeThriftType&gt; {
 * 
 * 	private final SomeThriftType instance = new SomeThriftType();
 * 
 * 	{@code @Override}
 * 	public SomeThriftType get() {
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
 * format, using the {@link TCompactProtocol}.
 * </p>
 * 
 * @author takanori.takase
 * 
 * @param <T>
 *            Thrift sub-type to handle.
 */
public abstract class AbstractTWritable<T extends TBase<?, ?>> implements
		Writable {

	private static final int INIT = 128;

	private static final double GROWTH = 1.2;

	private final TMemoryInputTransport inputTransport;

	private final TProtocol inputProtocol;

	private final AutoExpandingBufferWriteTransport outputTransport;

	private final TProtocol outputProtocol;

	public AbstractTWritable() {
		this(new TCompactProtocol.Factory(), INIT, GROWTH);
	}

	public AbstractTWritable(TProtocolFactory factory, int init, double growth) {
		inputTransport = new TMemoryInputTransport(new byte[init]);
		inputProtocol = factory.getProtocol(inputTransport);
		outputTransport = new AutoExpandingBufferWriteTransport(init, growth);
		outputProtocol = factory.getProtocol(outputTransport);
	}

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

		byte[] bytes = inputTransport.getBuffer();

		if (bytes.length < length) {
			bytes = new byte[length];
		}

		in.readFully(bytes, 0, length);

		inputTransport.reset(bytes, 0, length);

		T base = get();

		base.clear();

		try {
			base.read(inputProtocol);
		} catch (TException e) {
			throw new IOException(e);
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {

		outputTransport.reset();

		try {
			get().write(outputProtocol);
		} catch (TException e) {
			throw new IOException(e);
		}

		int length = outputTransport.getPos();

		WritableUtils.writeVInt(out, length);

		out.write(outputTransport.getBuf().array(), 0, length);

	}

}
