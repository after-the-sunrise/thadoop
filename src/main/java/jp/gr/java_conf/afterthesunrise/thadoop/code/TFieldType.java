package jp.gr.java_conf.afterthesunrise.thadoop.code;

import org.apache.thrift.protocol.TType;

/**
 * @author takanori.takase
 */
public enum TFieldType {

	STOP(TType.STOP),

	VOID(TType.VOID),

	BOOL(TType.BOOL),

	BYTE(TType.BYTE),

	DOUBLE(TType.DOUBLE),

	I16(TType.I16),

	I32(TType.I32),

	I64(TType.I64),

	STRING(TType.STRING),

	STRUCT(TType.STRUCT),

	MAP(TType.MAP),

	SET(TType.SET),

	LIST(TType.LIST),

	ENUM(TType.ENUM);

	private final byte id;

	private TFieldType(byte id) {
		this.id = id;
	}

	public byte getId() {
		return id;
	}

	private static final TFieldType[] VALS;

	static {

		int min = Byte.MAX_VALUE;
		int max = Byte.MIN_VALUE;

		for (TFieldType type : values()) {
			min = Math.min(min, type.id);
			max = Math.max(max, type.id);
		}

		VALS = new TFieldType[Math.abs(min) + max + 1];

		for (TFieldType type : values()) {
			VALS[type.id + Math.abs(min)] = type;
		}

	}

	public static TFieldType find(byte id) {

		if (id < 0 || VALS.length <= id) {
			return null;
		}

		return VALS[id];

	}

}
