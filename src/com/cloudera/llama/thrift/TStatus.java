/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.cloudera.llama.thrift;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import java.util.*;

public class TStatus implements org.apache.thrift.TBase<TStatus, TStatus._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TStatus");

  private static final org.apache.thrift.protocol.TField STATUS_CODE_FIELD_DESC = new org.apache.thrift.protocol.TField("status_code", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField ERROR_CODE_FIELD_DESC = new org.apache.thrift.protocol.TField("error_code", org.apache.thrift.protocol.TType.I16, (short)2);
  private static final org.apache.thrift.protocol.TField ERROR_MSGS_FIELD_DESC = new org.apache.thrift.protocol.TField("error_msgs", org.apache.thrift.protocol.TType.LIST, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TStatusStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TStatusTupleSchemeFactory());
  }

  /**
   * 
   * @see TStatusCode
   */
  public TStatusCode status_code; // required
  public short error_code; // required
  public List<String> error_msgs; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see TStatusCode
     */
    STATUS_CODE((short)1, "status_code"),
    ERROR_CODE((short)2, "error_code"),
    ERROR_MSGS((short)3, "error_msgs");

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
        case 1: // STATUS_CODE
          return STATUS_CODE;
        case 2: // ERROR_CODE
          return ERROR_CODE;
        case 3: // ERROR_MSGS
          return ERROR_MSGS;
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
  private static final int __ERROR_CODE_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATUS_CODE, new org.apache.thrift.meta_data.FieldMetaData("status_code", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TStatusCode.class)));
    tmpMap.put(_Fields.ERROR_CODE, new org.apache.thrift.meta_data.FieldMetaData("error_code", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I16)));
    tmpMap.put(_Fields.ERROR_MSGS, new org.apache.thrift.meta_data.FieldMetaData("error_msgs", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TStatus.class, metaDataMap);
  }

  public TStatus() {
  }

  public TStatus(
    TStatusCode status_code,
    short error_code,
    List<String> error_msgs)
  {
    this();
    this.status_code = status_code;
    this.error_code = error_code;
    setError_codeIsSet(true);
    this.error_msgs = error_msgs;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TStatus(TStatus other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetStatus_code()) {
      this.status_code = other.status_code;
    }
    this.error_code = other.error_code;
    if (other.isSetError_msgs()) {
      List<String> __this__error_msgs = new ArrayList<String>();
      for (String other_element : other.error_msgs) {
        __this__error_msgs.add(other_element);
      }
      this.error_msgs = __this__error_msgs;
    }
  }

  public TStatus deepCopy() {
    return new TStatus(this);
  }

  @Override
  public void clear() {
    this.status_code = null;
    setError_codeIsSet(false);
    this.error_code = 0;
    this.error_msgs = null;
  }

  /**
   * 
   * @see TStatusCode
   */
  public TStatusCode getStatus_code() {
    return this.status_code;
  }

  /**
   * 
   * @see TStatusCode
   */
  public TStatus setStatus_code(TStatusCode status_code) {
    this.status_code = status_code;
    return this;
  }

  public void unsetStatus_code() {
    this.status_code = null;
  }

  /** Returns true if field status_code is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus_code() {
    return this.status_code != null;
  }

  public void setStatus_codeIsSet(boolean value) {
    if (!value) {
      this.status_code = null;
    }
  }

  public short getError_code() {
    return this.error_code;
  }

  public TStatus setError_code(short error_code) {
    this.error_code = error_code;
    setError_codeIsSet(true);
    return this;
  }

  public void unsetError_code() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ERROR_CODE_ISSET_ID);
  }

  /** Returns true if field error_code is set (has been assigned a value) and false otherwise */
  public boolean isSetError_code() {
    return EncodingUtils.testBit(__isset_bitfield, __ERROR_CODE_ISSET_ID);
  }

  public void setError_codeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ERROR_CODE_ISSET_ID, value);
  }

  public int getError_msgsSize() {
    return (this.error_msgs == null) ? 0 : this.error_msgs.size();
  }

  public java.util.Iterator<String> getError_msgsIterator() {
    return (this.error_msgs == null) ? null : this.error_msgs.iterator();
  }

  public void addToError_msgs(String elem) {
    if (this.error_msgs == null) {
      this.error_msgs = new ArrayList<String>();
    }
    this.error_msgs.add(elem);
  }

  public List<String> getError_msgs() {
    return this.error_msgs;
  }

  public TStatus setError_msgs(List<String> error_msgs) {
    this.error_msgs = error_msgs;
    return this;
  }

  public void unsetError_msgs() {
    this.error_msgs = null;
  }

  /** Returns true if field error_msgs is set (has been assigned a value) and false otherwise */
  public boolean isSetError_msgs() {
    return this.error_msgs != null;
  }

  public void setError_msgsIsSet(boolean value) {
    if (!value) {
      this.error_msgs = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case STATUS_CODE:
      if (value == null) {
        unsetStatus_code();
      } else {
        setStatus_code((TStatusCode)value);
      }
      break;

    case ERROR_CODE:
      if (value == null) {
        unsetError_code();
      } else {
        setError_code((Short)value);
      }
      break;

    case ERROR_MSGS:
      if (value == null) {
        unsetError_msgs();
      } else {
        setError_msgs((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case STATUS_CODE:
      return getStatus_code();

    case ERROR_CODE:
      return Short.valueOf(getError_code());

    case ERROR_MSGS:
      return getError_msgs();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case STATUS_CODE:
      return isSetStatus_code();
    case ERROR_CODE:
      return isSetError_code();
    case ERROR_MSGS:
      return isSetError_msgs();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TStatus)
      return this.equals((TStatus)that);
    return false;
  }

  public boolean equals(TStatus that) {
    if (that == null)
      return false;

    boolean this_present_status_code = true && this.isSetStatus_code();
    boolean that_present_status_code = true && that.isSetStatus_code();
    if (this_present_status_code || that_present_status_code) {
      if (!(this_present_status_code && that_present_status_code))
        return false;
      if (!this.status_code.equals(that.status_code))
        return false;
    }

    boolean this_present_error_code = true;
    boolean that_present_error_code = true;
    if (this_present_error_code || that_present_error_code) {
      if (!(this_present_error_code && that_present_error_code))
        return false;
      if (this.error_code != that.error_code)
        return false;
    }

    boolean this_present_error_msgs = true && this.isSetError_msgs();
    boolean that_present_error_msgs = true && that.isSetError_msgs();
    if (this_present_error_msgs || that_present_error_msgs) {
      if (!(this_present_error_msgs && that_present_error_msgs))
        return false;
      if (!this.error_msgs.equals(that.error_msgs))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(TStatus other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TStatus typedOther = (TStatus)other;

    lastComparison = Boolean.valueOf(isSetStatus_code()).compareTo(typedOther.isSetStatus_code());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus_code()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status_code, typedOther.status_code);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetError_code()).compareTo(typedOther.isSetError_code());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetError_code()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.error_code, typedOther.error_code);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetError_msgs()).compareTo(typedOther.isSetError_msgs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetError_msgs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.error_msgs, typedOther.error_msgs);
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
    StringBuilder sb = new StringBuilder("TStatus(");
    boolean first = true;

    sb.append("status_code:");
    if (this.status_code == null) {
      sb.append("null");
    } else {
      sb.append(this.status_code);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("error_code:");
    sb.append(this.error_code);
    first = false;
    if (!first) sb.append(", ");
    sb.append("error_msgs:");
    if (this.error_msgs == null) {
      sb.append("null");
    } else {
      sb.append(this.error_msgs);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (status_code == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status_code' was not present! Struct: " + toString());
    }
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

  private static class TStatusStandardSchemeFactory implements SchemeFactory {
    public TStatusStandardScheme getScheme() {
      return new TStatusStandardScheme();
    }
  }

  private static class TStatusStandardScheme extends StandardScheme<TStatus> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TStatus struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STATUS_CODE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.status_code = TStatusCode.findByValue(iprot.readI32());
              struct.setStatus_codeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ERROR_CODE
            if (schemeField.type == org.apache.thrift.protocol.TType.I16) {
              struct.error_code = iprot.readI16();
              struct.setError_codeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ERROR_MSGS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.error_msgs = new ArrayList<String>(_list0.size);
                for (int _i1 = 0; _i1 < _list0.size; ++_i1)
                {
                  String _elem2; // required
                  _elem2 = iprot.readString();
                  struct.error_msgs.add(_elem2);
                }
                iprot.readListEnd();
              }
              struct.setError_msgsIsSet(true);
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

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TStatus struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.status_code != null) {
        oprot.writeFieldBegin(STATUS_CODE_FIELD_DESC);
        oprot.writeI32(struct.status_code.getValue());
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(ERROR_CODE_FIELD_DESC);
      oprot.writeI16(struct.error_code);
      oprot.writeFieldEnd();
      if (struct.error_msgs != null) {
        oprot.writeFieldBegin(ERROR_MSGS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.error_msgs.size()));
          for (String _iter3 : struct.error_msgs)
          {
            oprot.writeString(_iter3);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TStatusTupleSchemeFactory implements SchemeFactory {
    public TStatusTupleScheme getScheme() {
      return new TStatusTupleScheme();
    }
  }

  private static class TStatusTupleScheme extends TupleScheme<TStatus> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TStatus struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI32(struct.status_code.getValue());
      BitSet optionals = new BitSet();
      if (struct.isSetError_code()) {
        optionals.set(0);
      }
      if (struct.isSetError_msgs()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetError_code()) {
        oprot.writeI16(struct.error_code);
      }
      if (struct.isSetError_msgs()) {
        {
          oprot.writeI32(struct.error_msgs.size());
          for (String _iter4 : struct.error_msgs)
          {
            oprot.writeString(_iter4);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TStatus struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.status_code = TStatusCode.findByValue(iprot.readI32());
      struct.setStatus_codeIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.error_code = iprot.readI16();
        struct.setError_codeIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list5 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.error_msgs = new ArrayList<String>(_list5.size);
          for (int _i6 = 0; _i6 < _list5.size; ++_i6)
          {
            String _elem7; // required
            _elem7 = iprot.readString();
            struct.error_msgs.add(_elem7);
          }
        }
        struct.setError_msgsIsSet(true);
      }
    }
  }

}

