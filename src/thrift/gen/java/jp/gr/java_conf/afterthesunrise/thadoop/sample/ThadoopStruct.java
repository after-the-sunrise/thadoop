/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package jp.gr.java_conf.afterthesunrise.thadoop.sample;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThadoopStruct implements org.apache.thrift.TBase<ThadoopStruct, ThadoopStruct._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ThadoopStruct");

  private static final org.apache.thrift.protocol.TField V_VERSION_FIELD_DESC = new org.apache.thrift.protocol.TField("vVersion", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField V_AUTHOR_FIELD_DESC = new org.apache.thrift.protocol.TField("vAuthor", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField V_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("vType", org.apache.thrift.protocol.TType.I32, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ThadoopStructStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ThadoopStructTupleSchemeFactory());
  }

  private int vVersion; // optional
  private String vAuthor; // optional
  private ThadoopType vType; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    V_VERSION((short)1, "vVersion"),
    V_AUTHOR((short)2, "vAuthor"),
    /**
     * 
     * @see ThadoopType
     */
    V_TYPE((short)3, "vType");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // V_VERSION
          return V_VERSION;
        case 2: // V_AUTHOR
          return V_AUTHOR;
        case 3: // V_TYPE
          return V_TYPE;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __VVERSION_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.V_VERSION,_Fields.V_AUTHOR,_Fields.V_TYPE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.V_VERSION, new org.apache.thrift.meta_data.FieldMetaData("vVersion", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.V_AUTHOR, new org.apache.thrift.meta_data.FieldMetaData("vAuthor", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.V_TYPE, new org.apache.thrift.meta_data.FieldMetaData("vType", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, ThadoopType.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ThadoopStruct.class, metaDataMap);
  }

  public ThadoopStruct() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ThadoopStruct(ThadoopStruct other) {
    __isset_bitfield = other.__isset_bitfield;
    this.vVersion = other.vVersion;
    if (other.isSetVAuthor()) {
      this.vAuthor = other.vAuthor;
    }
    if (other.isSetVType()) {
      this.vType = other.vType;
    }
  }

  public ThadoopStruct deepCopy() {
    return new ThadoopStruct(this);
  }

  @Override
  public void clear() {
    setVVersionIsSet(false);
    this.vVersion = 0;
    this.vAuthor = null;
    this.vType = null;
  }

  public int getVVersion() {
    return this.vVersion;
  }

  public void setVVersion(int vVersion) {
    this.vVersion = vVersion;
    setVVersionIsSet(true);
  }

  public void unsetVVersion() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __VVERSION_ISSET_ID);
  }

  /** Returns true if field vVersion is set (has been assigned a value) and false otherwise */
  public boolean isSetVVersion() {
    return EncodingUtils.testBit(__isset_bitfield, __VVERSION_ISSET_ID);
  }

  public void setVVersionIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __VVERSION_ISSET_ID, value);
  }

  public String getVAuthor() {
    return this.vAuthor;
  }

  public void setVAuthor(String vAuthor) {
    this.vAuthor = vAuthor;
  }

  public void unsetVAuthor() {
    this.vAuthor = null;
  }

  /** Returns true if field vAuthor is set (has been assigned a value) and false otherwise */
  public boolean isSetVAuthor() {
    return this.vAuthor != null;
  }

  public void setVAuthorIsSet(boolean value) {
    if (!value) {
      this.vAuthor = null;
    }
  }

  /**
   * 
   * @see ThadoopType
   */
  public ThadoopType getVType() {
    return this.vType;
  }

  /**
   * 
   * @see ThadoopType
   */
  public void setVType(ThadoopType vType) {
    this.vType = vType;
  }

  public void unsetVType() {
    this.vType = null;
  }

  /** Returns true if field vType is set (has been assigned a value) and false otherwise */
  public boolean isSetVType() {
    return this.vType != null;
  }

  public void setVTypeIsSet(boolean value) {
    if (!value) {
      this.vType = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case V_VERSION:
      if (value == null) {
        unsetVVersion();
      } else {
        setVVersion((Integer)value);
      }
      break;

    case V_AUTHOR:
      if (value == null) {
        unsetVAuthor();
      } else {
        setVAuthor((String)value);
      }
      break;

    case V_TYPE:
      if (value == null) {
        unsetVType();
      } else {
        setVType((ThadoopType)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case V_VERSION:
      return Integer.valueOf(getVVersion());

    case V_AUTHOR:
      return getVAuthor();

    case V_TYPE:
      return getVType();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case V_VERSION:
      return isSetVVersion();
    case V_AUTHOR:
      return isSetVAuthor();
    case V_TYPE:
      return isSetVType();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ThadoopStruct)
      return this.equals((ThadoopStruct)that);
    return false;
  }

  public boolean equals(ThadoopStruct that) {
    if (that == null)
      return false;

    boolean this_present_vVersion = true && this.isSetVVersion();
    boolean that_present_vVersion = true && that.isSetVVersion();
    if (this_present_vVersion || that_present_vVersion) {
      if (!(this_present_vVersion && that_present_vVersion))
        return false;
      if (this.vVersion != that.vVersion)
        return false;
    }

    boolean this_present_vAuthor = true && this.isSetVAuthor();
    boolean that_present_vAuthor = true && that.isSetVAuthor();
    if (this_present_vAuthor || that_present_vAuthor) {
      if (!(this_present_vAuthor && that_present_vAuthor))
        return false;
      if (!this.vAuthor.equals(that.vAuthor))
        return false;
    }

    boolean this_present_vType = true && this.isSetVType();
    boolean that_present_vType = true && that.isSetVType();
    if (this_present_vType || that_present_vType) {
      if (!(this_present_vType && that_present_vType))
        return false;
      if (!this.vType.equals(that.vType))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_vVersion = true && (isSetVVersion());
    builder.append(present_vVersion);
    if (present_vVersion)
      builder.append(vVersion);

    boolean present_vAuthor = true && (isSetVAuthor());
    builder.append(present_vAuthor);
    if (present_vAuthor)
      builder.append(vAuthor);

    boolean present_vType = true && (isSetVType());
    builder.append(present_vType);
    if (present_vType)
      builder.append(vType.getValue());

    return builder.toHashCode();
  }

  public int compareTo(ThadoopStruct other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ThadoopStruct typedOther = (ThadoopStruct)other;

    lastComparison = Boolean.valueOf(isSetVVersion()).compareTo(typedOther.isSetVVersion());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVVersion()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.vVersion, typedOther.vVersion);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetVAuthor()).compareTo(typedOther.isSetVAuthor());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVAuthor()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.vAuthor, typedOther.vAuthor);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetVType()).compareTo(typedOther.isSetVType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.vType, typedOther.vType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ThadoopStruct(");
    boolean first = true;

    if (isSetVVersion()) {
      sb.append("vVersion:");
      sb.append(this.vVersion);
      first = false;
    }
    if (isSetVAuthor()) {
      if (!first) sb.append(", ");
      sb.append("vAuthor:");
      if (this.vAuthor == null) {
        sb.append("null");
      } else {
        sb.append(this.vAuthor);
      }
      first = false;
    }
    if (isSetVType()) {
      if (!first) sb.append(", ");
      sb.append("vType:");
      if (this.vType == null) {
        sb.append("null");
      } else {
        sb.append(this.vType);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ThadoopStructStandardSchemeFactory implements SchemeFactory {
    public ThadoopStructStandardScheme getScheme() {
      return new ThadoopStructStandardScheme();
    }
  }

  private static class ThadoopStructStandardScheme extends StandardScheme<ThadoopStruct> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ThadoopStruct struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // V_VERSION
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.vVersion = iprot.readI32();
              struct.setVVersionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // V_AUTHOR
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.vAuthor = iprot.readString();
              struct.setVAuthorIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // V_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.vType = ThadoopType.findByValue(iprot.readI32());
              struct.setVTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ThadoopStruct struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.isSetVVersion()) {
        oprot.writeFieldBegin(V_VERSION_FIELD_DESC);
        oprot.writeI32(struct.vVersion);
        oprot.writeFieldEnd();
      }
      if (struct.vAuthor != null) {
        if (struct.isSetVAuthor()) {
          oprot.writeFieldBegin(V_AUTHOR_FIELD_DESC);
          oprot.writeString(struct.vAuthor);
          oprot.writeFieldEnd();
        }
      }
      if (struct.vType != null) {
        if (struct.isSetVType()) {
          oprot.writeFieldBegin(V_TYPE_FIELD_DESC);
          oprot.writeI32(struct.vType.getValue());
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ThadoopStructTupleSchemeFactory implements SchemeFactory {
    public ThadoopStructTupleScheme getScheme() {
      return new ThadoopStructTupleScheme();
    }
  }

  private static class ThadoopStructTupleScheme extends TupleScheme<ThadoopStruct> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ThadoopStruct struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetVVersion()) {
        optionals.set(0);
      }
      if (struct.isSetVAuthor()) {
        optionals.set(1);
      }
      if (struct.isSetVType()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetVVersion()) {
        oprot.writeI32(struct.vVersion);
      }
      if (struct.isSetVAuthor()) {
        oprot.writeString(struct.vAuthor);
      }
      if (struct.isSetVType()) {
        oprot.writeI32(struct.vType.getValue());
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ThadoopStruct struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.vVersion = iprot.readI32();
        struct.setVVersionIsSet(true);
      }
      if (incoming.get(1)) {
        struct.vAuthor = iprot.readString();
        struct.setVAuthorIsSet(true);
      }
      if (incoming.get(2)) {
        struct.vType = ThadoopType.findByValue(iprot.readI32());
        struct.setVTypeIsSet(true);
      }
    }
  }

}

