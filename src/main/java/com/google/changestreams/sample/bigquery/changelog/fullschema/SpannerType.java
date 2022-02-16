package com.google.changestreams.sample.bigquery.changelog.fullschema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.spanner.v1.TypeCode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Describes a SpannerType in the Cloud Spanner SpannerType system. SpannerTypes can either be primitive (for example,
 * {@code INT64} and {@code STRING}) or composite (for example, {@code ARRAY<INT64>} or {@code
 * STRUCT<INT64,STRING>}).
 *
 * <p>{@code SpannerType} instances are immutable.
 */

@Immutable
@DefaultCoder(AvroCoder.class)
public final class SpannerType implements Serializable {
  private static final SpannerType SpannerType_BOOL = new SpannerType(Code.BOOL, null);
  private static final SpannerType SpannerType_INT64 = new SpannerType(Code.INT64, null);
  private static final SpannerType SpannerType_FLOAT64 = new SpannerType(Code.FLOAT64, null);
  private static final SpannerType SpannerType_NUMERIC = new SpannerType(Code.NUMERIC, null);
  private static final SpannerType SpannerType_STRING = new SpannerType(Code.STRING, null);
  private static final SpannerType SpannerType_JSON = new SpannerType(Code.JSON, null);
  private static final SpannerType SpannerType_BYTES = new SpannerType(Code.BYTES, null);
  private static final SpannerType SpannerType_TIMESTAMP = new SpannerType(Code.TIMESTAMP, null);
  private static final SpannerType SpannerType_DATE = new SpannerType(Code.DATE, null);
  private static final SpannerType SpannerType_ARRAY_BOOL = new SpannerType(Code.ARRAY, SpannerType_BOOL);
  private static final SpannerType SpannerType_ARRAY_INT64 = new SpannerType(Code.ARRAY, SpannerType_INT64);
  private static final SpannerType SpannerType_ARRAY_FLOAT64 = new SpannerType(Code.ARRAY, SpannerType_FLOAT64);
  private static final SpannerType SpannerType_ARRAY_NUMERIC = new SpannerType(Code.ARRAY, SpannerType_NUMERIC);
  private static final SpannerType SpannerType_ARRAY_STRING = new SpannerType(Code.ARRAY, SpannerType_STRING);
  private static final SpannerType SpannerType_ARRAY_JSON = new SpannerType(Code.ARRAY, SpannerType_JSON);
  private static final SpannerType SpannerType_ARRAY_BYTES = new SpannerType(Code.ARRAY, SpannerType_BYTES);
  private static final SpannerType SpannerType_ARRAY_TIMESTAMP = new SpannerType(Code.ARRAY, SpannerType_TIMESTAMP);
  private static final SpannerType SpannerType_ARRAY_DATE = new SpannerType(Code.ARRAY, SpannerType_DATE);

  private static final int AMBIGUOUS_FIELD = -1;
  private static final long serialVersionUID = -3076152125004114582L;

  /** Returns the descriptor for the {@code BOOL SpannerType}. */
  public static SpannerType bool() {
    return SpannerType_BOOL;
  }

  /**
   * Returns the descriptor for the {@code INT64} SpannerType: an integral SpannerType with the same value domain
   * as a Java {@code long}.
   */
  public static SpannerType int64() {
    return SpannerType_INT64;
  }

  /**
   * Returns the descriptor for the {@code FLOAT64} SpannerType: a floating point SpannerType with the same value
   * domain as a Java {code double}.
   */
  public static SpannerType float64() {
    return SpannerType_FLOAT64;
  }

  /** Returns the descriptor for the {@code NUMERIC} SpannerType. */
  public static SpannerType numeric() {
    return SpannerType_NUMERIC;
  }

  /**
   * Returns the descriptor for the {@code STRING} SpannerType: a variable-length Unicode character string.
   */
  public static SpannerType string() {
    return SpannerType_STRING;
  }

  /** Returns the descriptor for the {@code JSON} SpannerType. */
  public static SpannerType json() {
    return SpannerType_JSON;
  }

  /** Returns the descriptor for the {@code BYTES} SpannerType: a variable-length byte string. */
  public static SpannerType bytes() {
    return SpannerType_BYTES;
  }

  /**
   * Returns the descriptor for the {@code TIMESTAMP} SpannerType: a nano precision timestamp in the range
   * [0000-01-01 00:00:00, 9999-12-31 23:59:59.999999999 UTC].
   */
  public static SpannerType timestamp() {
    return SpannerType_TIMESTAMP;
  }

  /**
   * Returns the descriptor for the {@code DATE} SpannerType: a timezone independent date in the range
   * [0001-01-01, 9999-12-31).
   */
  public static SpannerType date() {
    return SpannerType_DATE;
  }

  /** Returns a descriptor for an array of {@code elementSpannerType}. */
  public static SpannerType array(SpannerType elementSpannerType) {
    Preconditions.checkNotNull(elementSpannerType);
    switch (elementSpannerType.getCode()) {
      case BOOL:
        return SpannerType_ARRAY_BOOL;
      case INT64:
        return SpannerType_ARRAY_INT64;
      case FLOAT64:
        return SpannerType_ARRAY_FLOAT64;
      case NUMERIC:
        return SpannerType_ARRAY_NUMERIC;
      case STRING:
        return SpannerType_ARRAY_STRING;
      case JSON:
        return SpannerType_ARRAY_JSON;
      case BYTES:
        return SpannerType_ARRAY_BYTES;
      case TIMESTAMP:
        return SpannerType_ARRAY_TIMESTAMP;
      case DATE:
        return SpannerType_ARRAY_DATE;
      default:
        return new SpannerType(Code.ARRAY, elementSpannerType);
    }
  }

  @org.apache.avro.reflect.Nullable
  private final Code code;
  @org.apache.avro.reflect.Nullable
  private final SpannerType arrayElementSpannerType;

  // For serialization.
  private SpannerType() {
    // TODO: remove init.
    code = Code.BOOL;
    arrayElementSpannerType = SpannerType_BOOL;
  }

  private SpannerType(
    Code code,
    @Nullable SpannerType arrayElementSpannerType) {
    this.code = code;
    this.arrayElementSpannerType = arrayElementSpannerType;
  }

  public enum Code {
    BOOL(TypeCode.BOOL),
    INT64(TypeCode.INT64),
    NUMERIC(TypeCode.NUMERIC),
    FLOAT64(TypeCode.FLOAT64),
    STRING(TypeCode.STRING),
    JSON(TypeCode.JSON),
    BYTES(TypeCode.BYTES),
    TIMESTAMP(TypeCode.TIMESTAMP),
    DATE(TypeCode.DATE),
    ARRAY(TypeCode.ARRAY),
    STRUCT(TypeCode.STRUCT);

    private static final Map<TypeCode, Code> protoToCode;

    static {
      ImmutableMap.Builder<TypeCode, Code> builder = ImmutableMap.builder();
      for (Code code : Code.values()) {
        builder.put(code.protoCode(), code);
      }
      protoToCode = builder.build();
    }

    private final TypeCode protoCode;

    Code(TypeCode protoCode) {
      this.protoCode = protoCode;
    }

    TypeCode protoCode() {
      return protoCode;
    }

    static Code fromProtoCode(TypeCode protoCode) {
      Code code = protoToCode.get(protoCode);
      checkArgument(code != null, "Invalid code: %s", protoCode);
      return code;
    }
  }

  /** Returns the SpannerType code corresponding to this SpannerType. */
  public Code getCode() {
    return code;
  }

  /**
   * Returns the SpannerType descriptor for elements of this {@code ARRAY} SpannerType.
   *
   * @throws IllegalStateException if {@code code() != Code.ARRAY}
   */
  public SpannerType getArrayElementSpannerType() {
    Preconditions.checkState(code == Code.ARRAY, "Illegal call for non-ARRAY SpannerType");
    return arrayElementSpannerType;
  }

  void toString(StringBuilder b) {
    if (code == Code.ARRAY) {
      b.append("ARRAY<");
      arrayElementSpannerType.toString(b);
      b.append('>');
    } else {
      b.append(code.toString());
    }
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    toString(b);
    return b.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SpannerType that = (SpannerType) o;
    return code == that.code
      && Objects.equals(arrayElementSpannerType, that.arrayElementSpannerType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(code, arrayElementSpannerType);
  }
}
