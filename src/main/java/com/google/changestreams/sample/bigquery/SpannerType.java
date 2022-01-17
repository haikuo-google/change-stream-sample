package com.google.changestreams.sample.bigquery;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.spanner.v1.TypeCode;
import com.google.spanner.v1.Type.Builder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
@DefaultCoder(AvroCoder.class)
public final class SpannerType implements Serializable {
  private static final SpannerType TYPE_BOOL;
  private static final SpannerType TYPE_INT64;
  private static final SpannerType TYPE_FLOAT64;
  private static final SpannerType TYPE_NUMERIC;
  private static final SpannerType TYPE_STRING;
  private static final SpannerType TYPE_BYTES;
  private static final SpannerType TYPE_TIMESTAMP;
  private static final SpannerType TYPE_DATE;
  private static final SpannerType TYPE_ARRAY_BOOL;
  private static final SpannerType TYPE_ARRAY_INT64;
  private static final SpannerType TYPE_ARRAY_FLOAT64;
  private static final SpannerType TYPE_ARRAY_NUMERIC;
  private static final SpannerType TYPE_ARRAY_STRING;
  private static final SpannerType TYPE_ARRAY_BYTES;
  private static final SpannerType TYPE_ARRAY_TIMESTAMP;
  private static final SpannerType TYPE_ARRAY_DATE;
  private static final int AMBIGUOUS_FIELD = -1;
  private static final long serialVersionUID = -3076152125004114582L;
  @org.apache.avro.reflect.Nullable
  private final SpannerType.Code code;
  @org.apache.avro.reflect.Nullable
  private final SpannerType arrayElementType;

  // TODO: Add default type.
  public SpannerType() {
    code = Code.BOOL;
    arrayElementType = TYPE_BOOL;
  }

  public static SpannerType bool() {
    return TYPE_BOOL;
  }

  public static SpannerType int64() {
    return TYPE_INT64;
  }

  public static SpannerType float64() {
    return TYPE_FLOAT64;
  }

  public static SpannerType numeric() {
    return TYPE_NUMERIC;
  }

  public static SpannerType string() {
    return TYPE_STRING;
  }

  public static SpannerType bytes() {
    return TYPE_BYTES;
  }

  public static SpannerType timestamp() {
    return TYPE_TIMESTAMP;
  }

  public static SpannerType date() {
    return TYPE_DATE;
  }

  public static SpannerType array(SpannerType elementType) {
    Preconditions.checkNotNull(elementType);
    switch(elementType.getCode()) {
      case BOOL:
        return TYPE_ARRAY_BOOL;
      case INT64:
        return TYPE_ARRAY_INT64;
      case FLOAT64:
        return TYPE_ARRAY_FLOAT64;
      case NUMERIC:
        return TYPE_ARRAY_NUMERIC;
      case STRING:
        return TYPE_ARRAY_STRING;
      case BYTES:
        return TYPE_ARRAY_BYTES;
      case TIMESTAMP:
        return TYPE_ARRAY_TIMESTAMP;
      case DATE:
        return TYPE_ARRAY_DATE;
      default:
        return new SpannerType(SpannerType.Code.ARRAY, elementType);
    }
  }

  private SpannerType(SpannerType.Code code, @Nullable SpannerType arrayElementType) {
    this.code = code;
    this.arrayElementType = arrayElementType;
  }

  public SpannerType.Code getCode() {
    return this.code;
  }

  public SpannerType getArrayElementType() {
    Preconditions.checkState(this.code == SpannerType.Code.ARRAY, "Illegal call for non-ARRAY type");
    return this.arrayElementType;
  }

  void toString(StringBuilder b) {
    if (this.code == SpannerType.Code.ARRAY) {
      b.append("ARRAY<");
      this.arrayElementType.toString(b);
      b.append('>');
    } else {
      b.append(this.code.toString());
    }
  }

  public String toString() {
    StringBuilder b = new StringBuilder();
    this.toString(b);
    return b.toString();
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      SpannerType that = (SpannerType)o;
      return this.code == that.code && Objects.equals(this.arrayElementType, that.arrayElementType);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.code, this.arrayElementType});
  }

  com.google.spanner.v1.Type toProto() {
    Builder proto = com.google.spanner.v1.Type.newBuilder();
    proto.setCode(this.code.protoCode());
    if (this.code == SpannerType.Code.ARRAY) {
      proto.setArrayElementType(this.arrayElementType.toProto());
    }

    return proto.build();
  }

  static SpannerType fromProto(com.google.spanner.v1.Type proto) {
    SpannerType.Code type = SpannerType.Code.fromProtoCode(proto.getCode());
    switch(type) {
      case BOOL:
        return bool();
      case INT64:
        return int64();
      case FLOAT64:
        return float64();
      case NUMERIC:
        return numeric();
      case STRING:
        return string();
      case BYTES:
        return bytes();
      case TIMESTAMP:
        return timestamp();
      case DATE:
        return date();
      case ARRAY:
        Preconditions.checkArgument(proto.hasArrayElementType(), "Missing expected 'array_element_type' field in 'Type' message: %s", proto);

        SpannerType elementType;
        try {
          elementType = fromProto(proto.getArrayElementType());
        } catch (IllegalArgumentException var7) {
          throw new IllegalArgumentException("Could not parse 'array_element_type' attribute in 'Type' message: " + proto, var7);
        }

        return array(elementType);
      default:
        throw new AssertionError("Unimplemented case: " + type);
    }
  }

  static {
    TYPE_BOOL = new SpannerType(SpannerType.Code.BOOL, (SpannerType)null);
    TYPE_INT64 = new SpannerType(SpannerType.Code.INT64, (SpannerType)null);
    TYPE_FLOAT64 = new SpannerType(SpannerType.Code.FLOAT64, (SpannerType)null);
    TYPE_NUMERIC = new SpannerType(SpannerType.Code.NUMERIC, (SpannerType)null);
    TYPE_STRING = new SpannerType(SpannerType.Code.STRING, (SpannerType)null);
    TYPE_BYTES = new SpannerType(SpannerType.Code.BYTES, (SpannerType)null);
    TYPE_TIMESTAMP = new SpannerType(SpannerType.Code.TIMESTAMP, (SpannerType)null);
    TYPE_DATE = new SpannerType(SpannerType.Code.DATE, (SpannerType)null);
    TYPE_ARRAY_BOOL = new SpannerType(SpannerType.Code.ARRAY, TYPE_BOOL);
    TYPE_ARRAY_INT64 = new SpannerType(SpannerType.Code.ARRAY, TYPE_INT64);
    TYPE_ARRAY_FLOAT64 = new SpannerType(SpannerType.Code.ARRAY, TYPE_FLOAT64);
    TYPE_ARRAY_NUMERIC = new SpannerType(SpannerType.Code.ARRAY, TYPE_NUMERIC);
    TYPE_ARRAY_STRING = new SpannerType(SpannerType.Code.ARRAY, TYPE_STRING);
    TYPE_ARRAY_BYTES = new SpannerType(SpannerType.Code.ARRAY, TYPE_BYTES);
    TYPE_ARRAY_TIMESTAMP = new SpannerType(SpannerType.Code.ARRAY, TYPE_TIMESTAMP);
    TYPE_ARRAY_DATE = new SpannerType(SpannerType.Code.ARRAY, TYPE_DATE);
  }

  public static enum Code {
    BOOL(TypeCode.BOOL),
    INT64(TypeCode.INT64),
    NUMERIC(TypeCode.NUMERIC),
    FLOAT64(TypeCode.FLOAT64),
    STRING(TypeCode.STRING),
    BYTES(TypeCode.BYTES),
    TIMESTAMP(TypeCode.TIMESTAMP),
    DATE(TypeCode.DATE),
    ARRAY(TypeCode.ARRAY);

    private static final Map<TypeCode, SpannerType.Code> protoToCode;
    private final TypeCode protoCode;

    private Code(TypeCode protoCode) {
      this.protoCode = protoCode;
    }

    TypeCode protoCode() {
      return this.protoCode;
    }

    static SpannerType.Code fromProtoCode(TypeCode protoCode) {
      SpannerType.Code code = (SpannerType.Code)protoToCode.get(protoCode);
      Preconditions.checkArgument(code != null, "Invalid code: %s", protoCode);
      return code;
    }

    static {
      com.google.common.collect.ImmutableMap.Builder<TypeCode, SpannerType.Code> builder = ImmutableMap.builder();
      SpannerType.Code[] var1 = values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
        SpannerType.Code code = var1[var3];
        builder.put(code.protoCode(), code);
      }

      protoToCode = builder.build();
    }
  }
}
