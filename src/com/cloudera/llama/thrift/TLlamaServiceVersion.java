/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.cloudera.llama.thrift;


import org.apache.thrift.TEnum;

public enum TLlamaServiceVersion implements org.apache.thrift.TEnum {
  V1(0);

  private final int value;

  private TLlamaServiceVersion(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static TLlamaServiceVersion findByValue(int value) { 
    switch (value) {
      case 0:
        return V1;
      default:
        return null;
    }
  }
}