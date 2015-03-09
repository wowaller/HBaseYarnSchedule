/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.cloudera.llama.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import java.util.*;

public class TLlamaAMReservationExpansionRequest implements org.apache.thrift.TBase<TLlamaAMReservationExpansionRequest, TLlamaAMReservationExpansionRequest._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TLlamaAMReservationExpansionRequest");

  private static final org.apache.thrift.protocol.TField VERSION_FIELD_DESC = new org.apache.thrift.protocol.TField("version", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField AM_HANDLE_FIELD_DESC = new org.apache.thrift.protocol.TField("am_handle", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField EXPANSION_OF_FIELD_DESC = new org.apache.thrift.protocol.TField("expansion_of", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField RESOURCE_FIELD_DESC = new org.apache.thrift.protocol.TField("resource", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField EXPANSION_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("expansion_id", org.apache.thrift.protocol.TType.STRUCT, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TLlamaAMReservationExpansionRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TLlamaAMReservationExpansionRequestTupleSchemeFactory());
  }

  /**
   * 
   * @see TLlamaServiceVersion
   */
  public TLlamaServiceVersion version; // required
  public TUniqueId am_handle; // required
  public TUniqueId expansion_of; // required
  public TResource resource; // required
  public TUniqueId expansion_id; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see TLlamaServiceVersion
     */
    VERSION((short)1, "version"),
    AM_HANDLE((short)2, "am_handle"),
    EXPANSION_OF((short)3, "expansion_of"),
    RESOURCE((short)4, "resource"),
    EXPANSION_ID((short)5, "expansion_id");

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
        case 1: // VERSION
          return VERSION;
        case 2: // AM_HANDLE
          return AM_HANDLE;
        case 3: // EXPANSION_OF
          return EXPANSION_OF;
        case 4: // RESOURCE
          return RESOURCE;
        case 5: // EXPANSION_ID
          return EXPANSION_ID;
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
  private _Fields optionals[] = {_Fields.EXPANSION_ID};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.VERSION, new org.apache.thrift.meta_data.FieldMetaData("version", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TLlamaServiceVersion.class)));
    tmpMap.put(_Fields.AM_HANDLE, new org.apache.thrift.meta_data.FieldMetaData("am_handle", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TUniqueId.class)));
    tmpMap.put(_Fields.EXPANSION_OF, new org.apache.thrift.meta_data.FieldMetaData("expansion_of", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TUniqueId.class)));
    tmpMap.put(_Fields.RESOURCE, new org.apache.thrift.meta_data.FieldMetaData("resource", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TResource.class)));
    tmpMap.put(_Fields.EXPANSION_ID, new org.apache.thrift.meta_data.FieldMetaData("expansion_id", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TUniqueId.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TLlamaAMReservationExpansionRequest.class, metaDataMap);
  }

  public TLlamaAMReservationExpansionRequest() {
  }

  public TLlamaAMReservationExpansionRequest(
    TLlamaServiceVersion version,
    TUniqueId am_handle,
    TUniqueId expansion_of,
    TResource resource)
  {
    this();
    this.version = version;
    this.am_handle = am_handle;
    this.expansion_of = expansion_of;
    this.resource = resource;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TLlamaAMReservationExpansionRequest(TLlamaAMReservationExpansionRequest other) {
    if (other.isSetVersion()) {
      this.version = other.version;
    }
    if (other.isSetAm_handle()) {
      this.am_handle = new TUniqueId(other.am_handle);
    }
    if (other.isSetExpansion_of()) {
      this.expansion_of = new TUniqueId(other.expansion_of);
    }
    if (other.isSetResource()) {
      this.resource = new TResource(other.resource);
    }
    if (other.isSetExpansion_id()) {
      this.expansion_id = new TUniqueId(other.expansion_id);
    }
  }

  public TLlamaAMReservationExpansionRequest deepCopy() {
    return new TLlamaAMReservationExpansionRequest(this);
  }

  @Override
  public void clear() {
    this.version = null;
    this.am_handle = null;
    this.expansion_of = null;
    this.resource = null;
    this.expansion_id = null;
  }

  /**
   * 
   * @see TLlamaServiceVersion
   */
  public TLlamaServiceVersion getVersion() {
    return this.version;
  }

  /**
   * 
   * @see TLlamaServiceVersion
   */
  public TLlamaAMReservationExpansionRequest setVersion(TLlamaServiceVersion version) {
    this.version = version;
    return this;
  }

  public void unsetVersion() {
    this.version = null;
  }

  /** Returns true if field version is set (has been assigned a value) and false otherwise */
  public boolean isSetVersion() {
    return this.version != null;
  }

  public void setVersionIsSet(boolean value) {
    if (!value) {
      this.version = null;
    }
  }

  public TUniqueId getAm_handle() {
    return this.am_handle;
  }

  public TLlamaAMReservationExpansionRequest setAm_handle(TUniqueId am_handle) {
    this.am_handle = am_handle;
    return this;
  }

  public void unsetAm_handle() {
    this.am_handle = null;
  }

  /** Returns true if field am_handle is set (has been assigned a value) and false otherwise */
  public boolean isSetAm_handle() {
    return this.am_handle != null;
  }

  public void setAm_handleIsSet(boolean value) {
    if (!value) {
      this.am_handle = null;
    }
  }

  public TUniqueId getExpansion_of() {
    return this.expansion_of;
  }

  public TLlamaAMReservationExpansionRequest setExpansion_of(TUniqueId expansion_of) {
    this.expansion_of = expansion_of;
    return this;
  }

  public void unsetExpansion_of() {
    this.expansion_of = null;
  }

  /** Returns true if field expansion_of is set (has been assigned a value) and false otherwise */
  public boolean isSetExpansion_of() {
    return this.expansion_of != null;
  }

  public void setExpansion_ofIsSet(boolean value) {
    if (!value) {
      this.expansion_of = null;
    }
  }

  public TResource getResource() {
    return this.resource;
  }

  public TLlamaAMReservationExpansionRequest setResource(TResource resource) {
    this.resource = resource;
    return this;
  }

  public void unsetResource() {
    this.resource = null;
  }

  /** Returns true if field resource is set (has been assigned a value) and false otherwise */
  public boolean isSetResource() {
    return this.resource != null;
  }

  public void setResourceIsSet(boolean value) {
    if (!value) {
      this.resource = null;
    }
  }

  public TUniqueId getExpansion_id() {
    return this.expansion_id;
  }

  public TLlamaAMReservationExpansionRequest setExpansion_id(TUniqueId expansion_id) {
    this.expansion_id = expansion_id;
    return this;
  }

  public void unsetExpansion_id() {
    this.expansion_id = null;
  }

  /** Returns true if field expansion_id is set (has been assigned a value) and false otherwise */
  public boolean isSetExpansion_id() {
    return this.expansion_id != null;
  }

  public void setExpansion_idIsSet(boolean value) {
    if (!value) {
      this.expansion_id = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case VERSION:
      if (value == null) {
        unsetVersion();
      } else {
        setVersion((TLlamaServiceVersion)value);
      }
      break;

    case AM_HANDLE:
      if (value == null) {
        unsetAm_handle();
      } else {
        setAm_handle((TUniqueId)value);
      }
      break;

    case EXPANSION_OF:
      if (value == null) {
        unsetExpansion_of();
      } else {
        setExpansion_of((TUniqueId)value);
      }
      break;

    case RESOURCE:
      if (value == null) {
        unsetResource();
      } else {
        setResource((TResource)value);
      }
      break;

    case EXPANSION_ID:
      if (value == null) {
        unsetExpansion_id();
      } else {
        setExpansion_id((TUniqueId)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case VERSION:
      return getVersion();

    case AM_HANDLE:
      return getAm_handle();

    case EXPANSION_OF:
      return getExpansion_of();

    case RESOURCE:
      return getResource();

    case EXPANSION_ID:
      return getExpansion_id();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case VERSION:
      return isSetVersion();
    case AM_HANDLE:
      return isSetAm_handle();
    case EXPANSION_OF:
      return isSetExpansion_of();
    case RESOURCE:
      return isSetResource();
    case EXPANSION_ID:
      return isSetExpansion_id();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TLlamaAMReservationExpansionRequest)
      return this.equals((TLlamaAMReservationExpansionRequest)that);
    return false;
  }

  public boolean equals(TLlamaAMReservationExpansionRequest that) {
    if (that == null)
      return false;

    boolean this_present_version = true && this.isSetVersion();
    boolean that_present_version = true && that.isSetVersion();
    if (this_present_version || that_present_version) {
      if (!(this_present_version && that_present_version))
        return false;
      if (!this.version.equals(that.version))
        return false;
    }

    boolean this_present_am_handle = true && this.isSetAm_handle();
    boolean that_present_am_handle = true && that.isSetAm_handle();
    if (this_present_am_handle || that_present_am_handle) {
      if (!(this_present_am_handle && that_present_am_handle))
        return false;
      if (!this.am_handle.equals(that.am_handle))
        return false;
    }

    boolean this_present_expansion_of = true && this.isSetExpansion_of();
    boolean that_present_expansion_of = true && that.isSetExpansion_of();
    if (this_present_expansion_of || that_present_expansion_of) {
      if (!(this_present_expansion_of && that_present_expansion_of))
        return false;
      if (!this.expansion_of.equals(that.expansion_of))
        return false;
    }

    boolean this_present_resource = true && this.isSetResource();
    boolean that_present_resource = true && that.isSetResource();
    if (this_present_resource || that_present_resource) {
      if (!(this_present_resource && that_present_resource))
        return false;
      if (!this.resource.equals(that.resource))
        return false;
    }

    boolean this_present_expansion_id = true && this.isSetExpansion_id();
    boolean that_present_expansion_id = true && that.isSetExpansion_id();
    if (this_present_expansion_id || that_present_expansion_id) {
      if (!(this_present_expansion_id && that_present_expansion_id))
        return false;
      if (!this.expansion_id.equals(that.expansion_id))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(TLlamaAMReservationExpansionRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TLlamaAMReservationExpansionRequest typedOther = (TLlamaAMReservationExpansionRequest)other;

    lastComparison = Boolean.valueOf(isSetVersion()).compareTo(typedOther.isSetVersion());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVersion()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.version, typedOther.version);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAm_handle()).compareTo(typedOther.isSetAm_handle());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAm_handle()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.am_handle, typedOther.am_handle);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetExpansion_of()).compareTo(typedOther.isSetExpansion_of());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetExpansion_of()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.expansion_of, typedOther.expansion_of);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetResource()).compareTo(typedOther.isSetResource());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetResource()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.resource, typedOther.resource);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetExpansion_id()).compareTo(typedOther.isSetExpansion_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetExpansion_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.expansion_id, typedOther.expansion_id);
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
    StringBuilder sb = new StringBuilder("TLlamaAMReservationExpansionRequest(");
    boolean first = true;

    sb.append("version:");
    if (this.version == null) {
      sb.append("null");
    } else {
      sb.append(this.version);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("am_handle:");
    if (this.am_handle == null) {
      sb.append("null");
    } else {
      sb.append(this.am_handle);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("expansion_of:");
    if (this.expansion_of == null) {
      sb.append("null");
    } else {
      sb.append(this.expansion_of);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("resource:");
    if (this.resource == null) {
      sb.append("null");
    } else {
      sb.append(this.resource);
    }
    first = false;
    if (isSetExpansion_id()) {
      if (!first) sb.append(", ");
      sb.append("expansion_id:");
      if (this.expansion_id == null) {
        sb.append("null");
      } else {
        sb.append(this.expansion_id);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (version == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'version' was not present! Struct: " + toString());
    }
    if (am_handle == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'am_handle' was not present! Struct: " + toString());
    }
    if (expansion_of == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'expansion_of' was not present! Struct: " + toString());
    }
    if (resource == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'resource' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (am_handle != null) {
      am_handle.validate();
    }
    if (expansion_of != null) {
      expansion_of.validate();
    }
    if (resource != null) {
      resource.validate();
    }
    if (expansion_id != null) {
      expansion_id.validate();
    }
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TLlamaAMReservationExpansionRequestStandardSchemeFactory implements SchemeFactory {
    public TLlamaAMReservationExpansionRequestStandardScheme getScheme() {
      return new TLlamaAMReservationExpansionRequestStandardScheme();
    }
  }

  private static class TLlamaAMReservationExpansionRequestStandardScheme extends StandardScheme<TLlamaAMReservationExpansionRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TLlamaAMReservationExpansionRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // VERSION
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.version = TLlamaServiceVersion.findByValue(iprot.readI32());
              struct.setVersionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // AM_HANDLE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.am_handle = new TUniqueId();
              struct.am_handle.read(iprot);
              struct.setAm_handleIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // EXPANSION_OF
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.expansion_of = new TUniqueId();
              struct.expansion_of.read(iprot);
              struct.setExpansion_ofIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // RESOURCE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.resource = new TResource();
              struct.resource.read(iprot);
              struct.setResourceIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // EXPANSION_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.expansion_id = new TUniqueId();
              struct.expansion_id.read(iprot);
              struct.setExpansion_idIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TLlamaAMReservationExpansionRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.version != null) {
        oprot.writeFieldBegin(VERSION_FIELD_DESC);
        oprot.writeI32(struct.version.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.am_handle != null) {
        oprot.writeFieldBegin(AM_HANDLE_FIELD_DESC);
        struct.am_handle.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.expansion_of != null) {
        oprot.writeFieldBegin(EXPANSION_OF_FIELD_DESC);
        struct.expansion_of.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.resource != null) {
        oprot.writeFieldBegin(RESOURCE_FIELD_DESC);
        struct.resource.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.expansion_id != null) {
        if (struct.isSetExpansion_id()) {
          oprot.writeFieldBegin(EXPANSION_ID_FIELD_DESC);
          struct.expansion_id.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TLlamaAMReservationExpansionRequestTupleSchemeFactory implements SchemeFactory {
    public TLlamaAMReservationExpansionRequestTupleScheme getScheme() {
      return new TLlamaAMReservationExpansionRequestTupleScheme();
    }
  }

  private static class TLlamaAMReservationExpansionRequestTupleScheme extends TupleScheme<TLlamaAMReservationExpansionRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TLlamaAMReservationExpansionRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI32(struct.version.getValue());
      struct.am_handle.write(oprot);
      struct.expansion_of.write(oprot);
      struct.resource.write(oprot);
      BitSet optionals = new BitSet();
      if (struct.isSetExpansion_id()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetExpansion_id()) {
        struct.expansion_id.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TLlamaAMReservationExpansionRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.version = TLlamaServiceVersion.findByValue(iprot.readI32());
      struct.setVersionIsSet(true);
      struct.am_handle = new TUniqueId();
      struct.am_handle.read(iprot);
      struct.setAm_handleIsSet(true);
      struct.expansion_of = new TUniqueId();
      struct.expansion_of.read(iprot);
      struct.setExpansion_ofIsSet(true);
      struct.resource = new TResource();
      struct.resource.read(iprot);
      struct.setResourceIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.expansion_id = new TUniqueId();
        struct.expansion_id.read(iprot);
        struct.setExpansion_idIsSet(true);
      }
    }
  }

}
