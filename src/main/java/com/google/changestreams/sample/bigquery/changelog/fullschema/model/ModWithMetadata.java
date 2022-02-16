package com.google.changestreams.sample.bigquery.changelog.fullschema.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.encoder.TimestampEncoding;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class ModWithMetadata implements Serializable {

  private static final long serialVersionUID = 8703257194338184299L;

  private String keysJson;

  private long commitTimestampSeconds;
  private int commitTimestampNanos;

  private String serverTransactionId;
  private boolean isLastRecordInTransactionInPartition;
  private String recordSequence;
  private String tableName;
  private ModType modType;
  private long numberOfRecordsInTransaction;
  private long numberOfPartitionsInTransaction;

  /** Default constructor for serialization only. */
  private ModWithMetadata() {}

  /**
   * Constructs a data change record for a given partition, at a given timestamp, for a given
   * transaction. The data change record needs to be given information about the table modified, the
   * type of primary keys and modified columns, the modifications themselves and other metadata.
   *
   * @param keysJson JSON object as String, where the keys are the primary key column names and the
   *     values are the primary key column values
   * @param commitTimestamp the timestamp at which the modifications within were committed in Cloud
   *     Spanner
   * @param serverTransactionId the unique transaction id in which the modifications occurred
   * @param isLastRecordInTransactionInPartition indicates whether this record is the last emitted
   *     for the given transaction in the given partition
   * @param recordSequence indicates the order in which this record was put into the change stream
   *     in the scope of a partition, commit timestamp and transaction tuple
   * @param tableName the name of the table in which the modifications occurred
   * @param modType the operation that caused the modification to occur
   * @param numberOfRecordsInTransaction the total number of records for the given transaction
   * @param numberOfPartitionsInTransaction the total number of partitions within the given
   *     transaction
   */
  public ModWithMetadata(
    String keysJson,
    Timestamp commitTimestamp,
    String serverTransactionId,
    boolean isLastRecordInTransactionInPartition,
    String recordSequence,
    String tableName,
    ModType modType,
    long numberOfRecordsInTransaction,
    long numberOfPartitionsInTransaction) {
    this.keysJson = keysJson;
    this.commitTimestampSeconds = commitTimestamp.getSeconds();
    this.commitTimestampNanos = commitTimestamp.getNanos();
    this.serverTransactionId = serverTransactionId;
    this.isLastRecordInTransactionInPartition = isLastRecordInTransactionInPartition;
    this.recordSequence = recordSequence;
    this.tableName = tableName;
    this.modType = modType;
    this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
    this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
  }

  /**
   * The primary keys of this specific modification. This is always present and can not be null. The
   * keys are returned as a JSON object (stringified), where the keys are the column names and the
   * values are the column values.
   *
   * @return JSON object as String representing the primary key state for the row modified
   */
  public String getKeysJson() {
    return keysJson;
  }

  public long getCommitTimestampSeconds() {
    return commitTimestampSeconds;
  }

  public int getCommitTimestampNanos() {
    return commitTimestampNanos;
  }

  /** The unique transaction id in which the modifications occurred. */
  public String getServerTransactionId() {
    return serverTransactionId;
  }

  /**
   * Indicates whether this record is the last emitted for the given transaction in the given
   * partition.
   */
  public boolean isLastRecordInTransactionInPartition() {
    return isLastRecordInTransactionInPartition;
  }

  /**
   * Indicates the order in which this record was put into the change stream in the scope of a
   * partition, commit timestamp and transaction tuple.
   */
  public String getRecordSequence() {
    return recordSequence;
  }

  /** The name of the table in which the modifications within this record occurred. */
  public String getTableName() {
    return tableName;
  }

  /** The type of operation that caused the modifications within this record. */
  public ModType getModType() {
    return modType;
  }

  /** The total number of data change records for the given transaction. */
  public long getNumberOfRecordsInTransaction() {
    return numberOfRecordsInTransaction;
  }

  /** The total number of partitions for the given transaction. */
  public long getNumberOfPartitionsInTransaction() {
    return numberOfPartitionsInTransaction;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ModWithMetadata)) {
      return false;
    }
    ModWithMetadata that = (ModWithMetadata) o;
    return keysJson == that.keysJson
      && isLastRecordInTransactionInPartition == that.isLastRecordInTransactionInPartition
      && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
      && numberOfPartitionsInTransaction == that.numberOfPartitionsInTransaction
      && commitTimestampSeconds == that.commitTimestampSeconds
      && commitTimestampNanos == that.commitTimestampNanos
      && Objects.equals(serverTransactionId, that.serverTransactionId)
      && Objects.equals(recordSequence, that.recordSequence)
      && Objects.equals(tableName, that.tableName)
      && modType == that.modType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      keysJson,
      commitTimestampSeconds,
      commitTimestampNanos,
      serverTransactionId,
      isLastRecordInTransactionInPartition,
      recordSequence,
      tableName,
      modType,
      numberOfRecordsInTransaction,
      numberOfPartitionsInTransaction);
  }

  @Override
  public String toString() {
    return "ModWithMetadata{"
      + "keysJson='"
      + keysJson
      + '\''
      + ", commitTimestampSeconds="
      + commitTimestampSeconds
      + ", commitTimestampNanos="
      + commitTimestampNanos
      + ", serverTransactionId='"
      + serverTransactionId
      + '\''
      + ", isLastRecordInTransactionInPartition="
      + isLastRecordInTransactionInPartition
      + ", recordSequence='"
      + recordSequence
      + '\''
      + ", tableName='"
      + tableName
      + '\''
      + ", modType="
      + modType
      + ", numberOfRecordsInTransaction="
      + numberOfRecordsInTransaction
      + ", numberOfPartitionsInTransaction="
      + numberOfPartitionsInTransaction
      + '}';
  }

  public static ModWithMetadata fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, ModWithMetadata.class);
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
