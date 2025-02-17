/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.sql.datasources.osmpbf.build;

public final class Osmformat {
  private Osmformat() {}

  static {
    proto4.RuntimeVersion.validateProtobufGencodeVersion(
        proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
        /* major= */ 4,
        /* minor= */ 27,
        /* patch= */ 0,
        /* suffix= */ "",
        Osmformat.class.getName());
  }

  public static void registerAllExtensions(proto4.ExtensionRegistryLite registry) {}

  public static void registerAllExtensions(proto4.ExtensionRegistry registry) {
    registerAllExtensions((proto4.ExtensionRegistryLite) registry);
  }

  public interface HeaderBlockOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:HeaderBlock)
      proto4.MessageOrBuilder {

    /**
     * <code>optional .HeaderBBox bbox = 1;</code>
     *
     * @return Whether the bbox field is set.
     */
    boolean hasBbox();

    /**
     * <code>optional .HeaderBBox bbox = 1;</code>
     *
     * @return The bbox.
     */
    Osmformat.HeaderBBox getBbox();

    /** <code>optional .HeaderBBox bbox = 1;</code> */
    Osmformat.HeaderBBoxOrBuilder getBboxOrBuilder();

    /**
     *
     *
     * <pre>
     * Additional tags to aid in parsing this dataset
     * </pre>
     *
     * <code>repeated string required_features = 4;</code>
     *
     * @return A list containing the requiredFeatures.
     */
    java.util.List<String> getRequiredFeaturesList();

    /**
     *
     *
     * <pre>
     * Additional tags to aid in parsing this dataset
     * </pre>
     *
     * <code>repeated string required_features = 4;</code>
     *
     * @return The count of requiredFeatures.
     */
    int getRequiredFeaturesCount();

    /**
     *
     *
     * <pre>
     * Additional tags to aid in parsing this dataset
     * </pre>
     *
     * <code>repeated string required_features = 4;</code>
     *
     * @param index The index of the element to return.
     * @return The requiredFeatures at the given index.
     */
    String getRequiredFeatures(int index);

    /**
     *
     *
     * <pre>
     * Additional tags to aid in parsing this dataset
     * </pre>
     *
     * <code>repeated string required_features = 4;</code>
     *
     * @param index The index of the value to return.
     * @return The bytes of the requiredFeatures at the given index.
     */
    proto4.ByteString getRequiredFeaturesBytes(int index);

    /**
     * <code>repeated string optional_features = 5;</code>
     *
     * @return A list containing the optionalFeatures.
     */
    java.util.List<String> getOptionalFeaturesList();

    /**
     * <code>repeated string optional_features = 5;</code>
     *
     * @return The count of optionalFeatures.
     */
    int getOptionalFeaturesCount();

    /**
     * <code>repeated string optional_features = 5;</code>
     *
     * @param index The index of the element to return.
     * @return The optionalFeatures at the given index.
     */
    String getOptionalFeatures(int index);

    /**
     * <code>repeated string optional_features = 5;</code>
     *
     * @param index The index of the value to return.
     * @return The bytes of the optionalFeatures at the given index.
     */
    proto4.ByteString getOptionalFeaturesBytes(int index);

    /**
     * <code>optional string writingprogram = 16;</code>
     *
     * @return Whether the writingprogram field is set.
     */
    boolean hasWritingprogram();

    /**
     * <code>optional string writingprogram = 16;</code>
     *
     * @return The writingprogram.
     */
    String getWritingprogram();

    /**
     * <code>optional string writingprogram = 16;</code>
     *
     * @return The bytes for writingprogram.
     */
    proto4.ByteString getWritingprogramBytes();

    /**
     *
     *
     * <pre>
     * From the bbox field.
     * </pre>
     *
     * <code>optional string source = 17;</code>
     *
     * @return Whether the source field is set.
     */
    boolean hasSource();

    /**
     *
     *
     * <pre>
     * From the bbox field.
     * </pre>
     *
     * <code>optional string source = 17;</code>
     *
     * @return The source.
     */
    String getSource();

    /**
     *
     *
     * <pre>
     * From the bbox field.
     * </pre>
     *
     * <code>optional string source = 17;</code>
     *
     * @return The bytes for source.
     */
    proto4.ByteString getSourceBytes();

    /**
     *
     *
     * <pre>
     * replication timestamp, expressed in seconds since the epoch,
     * otherwise the same value as in the "timestamp=..." field
     * in the state.txt file used by Osmosis
     * </pre>
     *
     * <code>optional int64 osmosis_replication_timestamp = 32;</code>
     *
     * @return Whether the osmosisReplicationTimestamp field is set.
     */
    boolean hasOsmosisReplicationTimestamp();

    /**
     *
     *
     * <pre>
     * replication timestamp, expressed in seconds since the epoch,
     * otherwise the same value as in the "timestamp=..." field
     * in the state.txt file used by Osmosis
     * </pre>
     *
     * <code>optional int64 osmosis_replication_timestamp = 32;</code>
     *
     * @return The osmosisReplicationTimestamp.
     */
    long getOsmosisReplicationTimestamp();

    /**
     *
     *
     * <pre>
     * replication sequence number (sequenceNumber in state.txt)
     * </pre>
     *
     * <code>optional int64 osmosis_replication_sequence_number = 33;</code>
     *
     * @return Whether the osmosisReplicationSequenceNumber field is set.
     */
    boolean hasOsmosisReplicationSequenceNumber();

    /**
     *
     *
     * <pre>
     * replication sequence number (sequenceNumber in state.txt)
     * </pre>
     *
     * <code>optional int64 osmosis_replication_sequence_number = 33;</code>
     *
     * @return The osmosisReplicationSequenceNumber.
     */
    long getOsmosisReplicationSequenceNumber();

    /**
     *
     *
     * <pre>
     * replication base URL (from Osmosis' configuration.txt file)
     * </pre>
     *
     * <code>optional string osmosis_replication_base_url = 34;</code>
     *
     * @return Whether the osmosisReplicationBaseUrl field is set.
     */
    boolean hasOsmosisReplicationBaseUrl();

    /**
     *
     *
     * <pre>
     * replication base URL (from Osmosis' configuration.txt file)
     * </pre>
     *
     * <code>optional string osmosis_replication_base_url = 34;</code>
     *
     * @return The osmosisReplicationBaseUrl.
     */
    String getOsmosisReplicationBaseUrl();

    /**
     *
     *
     * <pre>
     * replication base URL (from Osmosis' configuration.txt file)
     * </pre>
     *
     * <code>optional string osmosis_replication_base_url = 34;</code>
     *
     * @return The bytes for osmosisReplicationBaseUrl.
     */
    proto4.ByteString getOsmosisReplicationBaseUrlBytes();
  }

  /** Protobuf type {@code HeaderBlock} */
  public static final class HeaderBlock extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:HeaderBlock)
      HeaderBlockOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          HeaderBlock.class.getName());
    }

    // Use HeaderBlock.newBuilder() to construct.
    private HeaderBlock(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private HeaderBlock() {
      requiredFeatures_ = proto4.LazyStringArrayList.emptyList();
      optionalFeatures_ = proto4.LazyStringArrayList.emptyList();
      writingprogram_ = "";
      source_ = "";
      osmosisReplicationBaseUrl_ = "";
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBlock_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBlock_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.HeaderBlock.class, Osmformat.HeaderBlock.Builder.class);
    }

    private int bitField0_;
    public static final int BBOX_FIELD_NUMBER = 1;
    private Osmformat.HeaderBBox bbox_;

    /**
     * <code>optional .HeaderBBox bbox = 1;</code>
     *
     * @return Whether the bbox field is set.
     */
    @Override
    public boolean hasBbox() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     * <code>optional .HeaderBBox bbox = 1;</code>
     *
     * @return The bbox.
     */
    @Override
    public Osmformat.HeaderBBox getBbox() {
      return bbox_ == null ? Osmformat.HeaderBBox.getDefaultInstance() : bbox_;
    }

    /** <code>optional .HeaderBBox bbox = 1;</code> */
    @Override
    public Osmformat.HeaderBBoxOrBuilder getBboxOrBuilder() {
      return bbox_ == null ? Osmformat.HeaderBBox.getDefaultInstance() : bbox_;
    }

    public static final int REQUIRED_FEATURES_FIELD_NUMBER = 4;

    @SuppressWarnings("serial")
    private proto4.LazyStringArrayList requiredFeatures_ = proto4.LazyStringArrayList.emptyList();

    /**
     *
     *
     * <pre>
     * Additional tags to aid in parsing this dataset
     * </pre>
     *
     * <code>repeated string required_features = 4;</code>
     *
     * @return A list containing the requiredFeatures.
     */
    public proto4.ProtocolStringList getRequiredFeaturesList() {
      return requiredFeatures_;
    }

    /**
     *
     *
     * <pre>
     * Additional tags to aid in parsing this dataset
     * </pre>
     *
     * <code>repeated string required_features = 4;</code>
     *
     * @return The count of requiredFeatures.
     */
    public int getRequiredFeaturesCount() {
      return requiredFeatures_.size();
    }

    /**
     *
     *
     * <pre>
     * Additional tags to aid in parsing this dataset
     * </pre>
     *
     * <code>repeated string required_features = 4;</code>
     *
     * @param index The index of the element to return.
     * @return The requiredFeatures at the given index.
     */
    public String getRequiredFeatures(int index) {
      return requiredFeatures_.get(index);
    }

    /**
     *
     *
     * <pre>
     * Additional tags to aid in parsing this dataset
     * </pre>
     *
     * <code>repeated string required_features = 4;</code>
     *
     * @param index The index of the value to return.
     * @return The bytes of the requiredFeatures at the given index.
     */
    public proto4.ByteString getRequiredFeaturesBytes(int index) {
      return requiredFeatures_.getByteString(index);
    }

    public static final int OPTIONAL_FEATURES_FIELD_NUMBER = 5;

    @SuppressWarnings("serial")
    private proto4.LazyStringArrayList optionalFeatures_ = proto4.LazyStringArrayList.emptyList();

    /**
     * <code>repeated string optional_features = 5;</code>
     *
     * @return A list containing the optionalFeatures.
     */
    public proto4.ProtocolStringList getOptionalFeaturesList() {
      return optionalFeatures_;
    }

    /**
     * <code>repeated string optional_features = 5;</code>
     *
     * @return The count of optionalFeatures.
     */
    public int getOptionalFeaturesCount() {
      return optionalFeatures_.size();
    }

    /**
     * <code>repeated string optional_features = 5;</code>
     *
     * @param index The index of the element to return.
     * @return The optionalFeatures at the given index.
     */
    public String getOptionalFeatures(int index) {
      return optionalFeatures_.get(index);
    }

    /**
     * <code>repeated string optional_features = 5;</code>
     *
     * @param index The index of the value to return.
     * @return The bytes of the optionalFeatures at the given index.
     */
    public proto4.ByteString getOptionalFeaturesBytes(int index) {
      return optionalFeatures_.getByteString(index);
    }

    public static final int WRITINGPROGRAM_FIELD_NUMBER = 16;

    @SuppressWarnings("serial")
    private volatile Object writingprogram_ = "";

    /**
     * <code>optional string writingprogram = 16;</code>
     *
     * @return Whether the writingprogram field is set.
     */
    @Override
    public boolean hasWritingprogram() {
      return ((bitField0_ & 0x00000002) != 0);
    }

    /**
     * <code>optional string writingprogram = 16;</code>
     *
     * @return The writingprogram.
     */
    @Override
    public String getWritingprogram() {
      Object ref = writingprogram_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        proto4.ByteString bs = (proto4.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          writingprogram_ = s;
        }
        return s;
      }
    }

    /**
     * <code>optional string writingprogram = 16;</code>
     *
     * @return The bytes for writingprogram.
     */
    @Override
    public proto4.ByteString getWritingprogramBytes() {
      Object ref = writingprogram_;
      if (ref instanceof String) {
        proto4.ByteString b = proto4.ByteString.copyFromUtf8((String) ref);
        writingprogram_ = b;
        return b;
      } else {
        return (proto4.ByteString) ref;
      }
    }

    public static final int SOURCE_FIELD_NUMBER = 17;

    @SuppressWarnings("serial")
    private volatile Object source_ = "";

    /**
     *
     *
     * <pre>
     * From the bbox field.
     * </pre>
     *
     * <code>optional string source = 17;</code>
     *
     * @return Whether the source field is set.
     */
    @Override
    public boolean hasSource() {
      return ((bitField0_ & 0x00000004) != 0);
    }

    /**
     *
     *
     * <pre>
     * From the bbox field.
     * </pre>
     *
     * <code>optional string source = 17;</code>
     *
     * @return The source.
     */
    @Override
    public String getSource() {
      Object ref = source_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        proto4.ByteString bs = (proto4.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          source_ = s;
        }
        return s;
      }
    }

    /**
     *
     *
     * <pre>
     * From the bbox field.
     * </pre>
     *
     * <code>optional string source = 17;</code>
     *
     * @return The bytes for source.
     */
    @Override
    public proto4.ByteString getSourceBytes() {
      Object ref = source_;
      if (ref instanceof String) {
        proto4.ByteString b = proto4.ByteString.copyFromUtf8((String) ref);
        source_ = b;
        return b;
      } else {
        return (proto4.ByteString) ref;
      }
    }

    public static final int OSMOSIS_REPLICATION_TIMESTAMP_FIELD_NUMBER = 32;
    private long osmosisReplicationTimestamp_ = 0L;

    /**
     *
     *
     * <pre>
     * replication timestamp, expressed in seconds since the epoch,
     * otherwise the same value as in the "timestamp=..." field
     * in the state.txt file used by Osmosis
     * </pre>
     *
     * <code>optional int64 osmosis_replication_timestamp = 32;</code>
     *
     * @return Whether the osmosisReplicationTimestamp field is set.
     */
    @Override
    public boolean hasOsmosisReplicationTimestamp() {
      return ((bitField0_ & 0x00000008) != 0);
    }

    /**
     *
     *
     * <pre>
     * replication timestamp, expressed in seconds since the epoch,
     * otherwise the same value as in the "timestamp=..." field
     * in the state.txt file used by Osmosis
     * </pre>
     *
     * <code>optional int64 osmosis_replication_timestamp = 32;</code>
     *
     * @return The osmosisReplicationTimestamp.
     */
    @Override
    public long getOsmosisReplicationTimestamp() {
      return osmosisReplicationTimestamp_;
    }

    public static final int OSMOSIS_REPLICATION_SEQUENCE_NUMBER_FIELD_NUMBER = 33;
    private long osmosisReplicationSequenceNumber_ = 0L;

    /**
     *
     *
     * <pre>
     * replication sequence number (sequenceNumber in state.txt)
     * </pre>
     *
     * <code>optional int64 osmosis_replication_sequence_number = 33;</code>
     *
     * @return Whether the osmosisReplicationSequenceNumber field is set.
     */
    @Override
    public boolean hasOsmosisReplicationSequenceNumber() {
      return ((bitField0_ & 0x00000010) != 0);
    }

    /**
     *
     *
     * <pre>
     * replication sequence number (sequenceNumber in state.txt)
     * </pre>
     *
     * <code>optional int64 osmosis_replication_sequence_number = 33;</code>
     *
     * @return The osmosisReplicationSequenceNumber.
     */
    @Override
    public long getOsmosisReplicationSequenceNumber() {
      return osmosisReplicationSequenceNumber_;
    }

    public static final int OSMOSIS_REPLICATION_BASE_URL_FIELD_NUMBER = 34;

    @SuppressWarnings("serial")
    private volatile Object osmosisReplicationBaseUrl_ = "";

    /**
     *
     *
     * <pre>
     * replication base URL (from Osmosis' configuration.txt file)
     * </pre>
     *
     * <code>optional string osmosis_replication_base_url = 34;</code>
     *
     * @return Whether the osmosisReplicationBaseUrl field is set.
     */
    @Override
    public boolean hasOsmosisReplicationBaseUrl() {
      return ((bitField0_ & 0x00000020) != 0);
    }

    /**
     *
     *
     * <pre>
     * replication base URL (from Osmosis' configuration.txt file)
     * </pre>
     *
     * <code>optional string osmosis_replication_base_url = 34;</code>
     *
     * @return The osmosisReplicationBaseUrl.
     */
    @Override
    public String getOsmosisReplicationBaseUrl() {
      Object ref = osmosisReplicationBaseUrl_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        proto4.ByteString bs = (proto4.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          osmosisReplicationBaseUrl_ = s;
        }
        return s;
      }
    }

    /**
     *
     *
     * <pre>
     * replication base URL (from Osmosis' configuration.txt file)
     * </pre>
     *
     * <code>optional string osmosis_replication_base_url = 34;</code>
     *
     * @return The bytes for osmosisReplicationBaseUrl.
     */
    @Override
    public proto4.ByteString getOsmosisReplicationBaseUrlBytes() {
      Object ref = osmosisReplicationBaseUrl_;
      if (ref instanceof String) {
        proto4.ByteString b = proto4.ByteString.copyFromUtf8((String) ref);
        osmosisReplicationBaseUrl_ = b;
        return b;
      } else {
        return (proto4.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (hasBbox()) {
        if (!getBbox().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getBbox());
      }
      for (int i = 0; i < requiredFeatures_.size(); i++) {
        proto4.GeneratedMessage.writeString(output, 4, requiredFeatures_.getRaw(i));
      }
      for (int i = 0; i < optionalFeatures_.size(); i++) {
        proto4.GeneratedMessage.writeString(output, 5, optionalFeatures_.getRaw(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        proto4.GeneratedMessage.writeString(output, 16, writingprogram_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        proto4.GeneratedMessage.writeString(output, 17, source_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeInt64(32, osmosisReplicationTimestamp_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeInt64(33, osmosisReplicationSequenceNumber_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        proto4.GeneratedMessage.writeString(output, 34, osmosisReplicationBaseUrl_);
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeMessageSize(1, getBbox());
      }
      {
        int dataSize = 0;
        for (int i = 0; i < requiredFeatures_.size(); i++) {
          dataSize += computeStringSizeNoTag(requiredFeatures_.getRaw(i));
        }
        size += dataSize;
        size += 1 * getRequiredFeaturesList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < optionalFeatures_.size(); i++) {
          dataSize += computeStringSizeNoTag(optionalFeatures_.getRaw(i));
        }
        size += dataSize;
        size += 1 * getOptionalFeaturesList().size();
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += proto4.GeneratedMessage.computeStringSize(16, writingprogram_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += proto4.GeneratedMessage.computeStringSize(17, source_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(32, osmosisReplicationTimestamp_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(33, osmosisReplicationSequenceNumber_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        size += proto4.GeneratedMessage.computeStringSize(34, osmosisReplicationBaseUrl_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.HeaderBlock)) {
        return super.equals(obj);
      }
      Osmformat.HeaderBlock other = (Osmformat.HeaderBlock) obj;

      if (hasBbox() != other.hasBbox()) return false;
      if (hasBbox()) {
        if (!getBbox().equals(other.getBbox())) return false;
      }
      if (!getRequiredFeaturesList().equals(other.getRequiredFeaturesList())) return false;
      if (!getOptionalFeaturesList().equals(other.getOptionalFeaturesList())) return false;
      if (hasWritingprogram() != other.hasWritingprogram()) return false;
      if (hasWritingprogram()) {
        if (!getWritingprogram().equals(other.getWritingprogram())) return false;
      }
      if (hasSource() != other.hasSource()) return false;
      if (hasSource()) {
        if (!getSource().equals(other.getSource())) return false;
      }
      if (hasOsmosisReplicationTimestamp() != other.hasOsmosisReplicationTimestamp()) return false;
      if (hasOsmosisReplicationTimestamp()) {
        if (getOsmosisReplicationTimestamp() != other.getOsmosisReplicationTimestamp())
          return false;
      }
      if (hasOsmosisReplicationSequenceNumber() != other.hasOsmosisReplicationSequenceNumber())
        return false;
      if (hasOsmosisReplicationSequenceNumber()) {
        if (getOsmosisReplicationSequenceNumber() != other.getOsmosisReplicationSequenceNumber())
          return false;
      }
      if (hasOsmosisReplicationBaseUrl() != other.hasOsmosisReplicationBaseUrl()) return false;
      if (hasOsmosisReplicationBaseUrl()) {
        if (!getOsmosisReplicationBaseUrl().equals(other.getOsmosisReplicationBaseUrl()))
          return false;
      }
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasBbox()) {
        hash = (37 * hash) + BBOX_FIELD_NUMBER;
        hash = (53 * hash) + getBbox().hashCode();
      }
      if (getRequiredFeaturesCount() > 0) {
        hash = (37 * hash) + REQUIRED_FEATURES_FIELD_NUMBER;
        hash = (53 * hash) + getRequiredFeaturesList().hashCode();
      }
      if (getOptionalFeaturesCount() > 0) {
        hash = (37 * hash) + OPTIONAL_FEATURES_FIELD_NUMBER;
        hash = (53 * hash) + getOptionalFeaturesList().hashCode();
      }
      if (hasWritingprogram()) {
        hash = (37 * hash) + WRITINGPROGRAM_FIELD_NUMBER;
        hash = (53 * hash) + getWritingprogram().hashCode();
      }
      if (hasSource()) {
        hash = (37 * hash) + SOURCE_FIELD_NUMBER;
        hash = (53 * hash) + getSource().hashCode();
      }
      if (hasOsmosisReplicationTimestamp()) {
        hash = (37 * hash) + OSMOSIS_REPLICATION_TIMESTAMP_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getOsmosisReplicationTimestamp());
      }
      if (hasOsmosisReplicationSequenceNumber()) {
        hash = (37 * hash) + OSMOSIS_REPLICATION_SEQUENCE_NUMBER_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getOsmosisReplicationSequenceNumber());
      }
      if (hasOsmosisReplicationBaseUrl()) {
        hash = (37 * hash) + OSMOSIS_REPLICATION_BASE_URL_FIELD_NUMBER;
        hash = (53 * hash) + getOsmosisReplicationBaseUrl().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.HeaderBlock parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.HeaderBlock parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.HeaderBlock parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.HeaderBlock parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.HeaderBlock parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.HeaderBlock parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.HeaderBlock parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.HeaderBlock parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.HeaderBlock parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.HeaderBlock parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.HeaderBlock parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.HeaderBlock parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.HeaderBlock prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /** Protobuf type {@code HeaderBlock} */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:HeaderBlock)
        Osmformat.HeaderBlockOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBlock_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBlock_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.HeaderBlock.class, Osmformat.HeaderBlock.Builder.class);
      }

      // Construct using Osmformat.HeaderBlock.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }

      private void maybeForceBuilderInitialization() {
        if (proto4.GeneratedMessage.alwaysUseFieldBuilders) {
          getBboxFieldBuilder();
        }
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        bbox_ = null;
        if (bboxBuilder_ != null) {
          bboxBuilder_.dispose();
          bboxBuilder_ = null;
        }
        requiredFeatures_ = proto4.LazyStringArrayList.emptyList();
        optionalFeatures_ = proto4.LazyStringArrayList.emptyList();
        writingprogram_ = "";
        source_ = "";
        osmosisReplicationTimestamp_ = 0L;
        osmosisReplicationSequenceNumber_ = 0L;
        osmosisReplicationBaseUrl_ = "";
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBlock_descriptor;
      }

      @Override
      public Osmformat.HeaderBlock getDefaultInstanceForType() {
        return Osmformat.HeaderBlock.getDefaultInstance();
      }

      @Override
      public Osmformat.HeaderBlock build() {
        Osmformat.HeaderBlock result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.HeaderBlock buildPartial() {
        Osmformat.HeaderBlock result = new Osmformat.HeaderBlock(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.HeaderBlock result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.bbox_ = bboxBuilder_ == null ? bbox_ : bboxBuilder_.build();
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          requiredFeatures_.makeImmutable();
          result.requiredFeatures_ = requiredFeatures_;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          optionalFeatures_.makeImmutable();
          result.optionalFeatures_ = optionalFeatures_;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.writingprogram_ = writingprogram_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.source_ = source_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          result.osmosisReplicationTimestamp_ = osmosisReplicationTimestamp_;
          to_bitField0_ |= 0x00000008;
        }
        if (((from_bitField0_ & 0x00000040) != 0)) {
          result.osmosisReplicationSequenceNumber_ = osmosisReplicationSequenceNumber_;
          to_bitField0_ |= 0x00000010;
        }
        if (((from_bitField0_ & 0x00000080) != 0)) {
          result.osmosisReplicationBaseUrl_ = osmosisReplicationBaseUrl_;
          to_bitField0_ |= 0x00000020;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.HeaderBlock) {
          return mergeFrom((Osmformat.HeaderBlock) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.HeaderBlock other) {
        if (other == Osmformat.HeaderBlock.getDefaultInstance()) return this;
        if (other.hasBbox()) {
          mergeBbox(other.getBbox());
        }
        if (!other.requiredFeatures_.isEmpty()) {
          if (requiredFeatures_.isEmpty()) {
            requiredFeatures_ = other.requiredFeatures_;
            bitField0_ |= 0x00000002;
          } else {
            ensureRequiredFeaturesIsMutable();
            requiredFeatures_.addAll(other.requiredFeatures_);
          }
          onChanged();
        }
        if (!other.optionalFeatures_.isEmpty()) {
          if (optionalFeatures_.isEmpty()) {
            optionalFeatures_ = other.optionalFeatures_;
            bitField0_ |= 0x00000004;
          } else {
            ensureOptionalFeaturesIsMutable();
            optionalFeatures_.addAll(other.optionalFeatures_);
          }
          onChanged();
        }
        if (other.hasWritingprogram()) {
          writingprogram_ = other.writingprogram_;
          bitField0_ |= 0x00000008;
          onChanged();
        }
        if (other.hasSource()) {
          source_ = other.source_;
          bitField0_ |= 0x00000010;
          onChanged();
        }
        if (other.hasOsmosisReplicationTimestamp()) {
          setOsmosisReplicationTimestamp(other.getOsmosisReplicationTimestamp());
        }
        if (other.hasOsmosisReplicationSequenceNumber()) {
          setOsmosisReplicationSequenceNumber(other.getOsmosisReplicationSequenceNumber());
        }
        if (other.hasOsmosisReplicationBaseUrl()) {
          osmosisReplicationBaseUrl_ = other.osmosisReplicationBaseUrl_;
          bitField0_ |= 0x00000080;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        if (hasBbox()) {
          if (!getBbox().isInitialized()) {
            return false;
          }
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 10:
                {
                  input.readMessage(getBboxFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
              case 34:
                {
                  proto4.ByteString bs = input.readBytes();
                  ensureRequiredFeaturesIsMutable();
                  requiredFeatures_.add(bs);
                  break;
                } // case 34
              case 42:
                {
                  proto4.ByteString bs = input.readBytes();
                  ensureOptionalFeaturesIsMutable();
                  optionalFeatures_.add(bs);
                  break;
                } // case 42
              case 130:
                {
                  writingprogram_ = input.readBytes();
                  bitField0_ |= 0x00000008;
                  break;
                } // case 130
              case 138:
                {
                  source_ = input.readBytes();
                  bitField0_ |= 0x00000010;
                  break;
                } // case 138
              case 256:
                {
                  osmosisReplicationTimestamp_ = input.readInt64();
                  bitField0_ |= 0x00000020;
                  break;
                } // case 256
              case 264:
                {
                  osmosisReplicationSequenceNumber_ = input.readInt64();
                  bitField0_ |= 0x00000040;
                  break;
                } // case 264
              case 274:
                {
                  osmosisReplicationBaseUrl_ = input.readBytes();
                  bitField0_ |= 0x00000080;
                  break;
                } // case 274
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private Osmformat.HeaderBBox bbox_;
      private proto4.SingleFieldBuilder<
              Osmformat.HeaderBBox, Osmformat.HeaderBBox.Builder, Osmformat.HeaderBBoxOrBuilder>
          bboxBuilder_;

      /**
       * <code>optional .HeaderBBox bbox = 1;</code>
       *
       * @return Whether the bbox field is set.
       */
      public boolean hasBbox() {
        return ((bitField0_ & 0x00000001) != 0);
      }

      /**
       * <code>optional .HeaderBBox bbox = 1;</code>
       *
       * @return The bbox.
       */
      public Osmformat.HeaderBBox getBbox() {
        if (bboxBuilder_ == null) {
          return bbox_ == null ? Osmformat.HeaderBBox.getDefaultInstance() : bbox_;
        } else {
          return bboxBuilder_.getMessage();
        }
      }

      /** <code>optional .HeaderBBox bbox = 1;</code> */
      public Builder setBbox(Osmformat.HeaderBBox value) {
        if (bboxBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          bbox_ = value;
        } else {
          bboxBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /** <code>optional .HeaderBBox bbox = 1;</code> */
      public Builder setBbox(Osmformat.HeaderBBox.Builder builderForValue) {
        if (bboxBuilder_ == null) {
          bbox_ = builderForValue.build();
        } else {
          bboxBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /** <code>optional .HeaderBBox bbox = 1;</code> */
      public Builder mergeBbox(Osmformat.HeaderBBox value) {
        if (bboxBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)
              && bbox_ != null
              && bbox_ != Osmformat.HeaderBBox.getDefaultInstance()) {
            getBboxBuilder().mergeFrom(value);
          } else {
            bbox_ = value;
          }
        } else {
          bboxBuilder_.mergeFrom(value);
        }
        if (bbox_ != null) {
          bitField0_ |= 0x00000001;
          onChanged();
        }
        return this;
      }

      /** <code>optional .HeaderBBox bbox = 1;</code> */
      public Builder clearBbox() {
        bitField0_ = (bitField0_ & ~0x00000001);
        bbox_ = null;
        if (bboxBuilder_ != null) {
          bboxBuilder_.dispose();
          bboxBuilder_ = null;
        }
        onChanged();
        return this;
      }

      /** <code>optional .HeaderBBox bbox = 1;</code> */
      public Osmformat.HeaderBBox.Builder getBboxBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getBboxFieldBuilder().getBuilder();
      }

      /** <code>optional .HeaderBBox bbox = 1;</code> */
      public Osmformat.HeaderBBoxOrBuilder getBboxOrBuilder() {
        if (bboxBuilder_ != null) {
          return bboxBuilder_.getMessageOrBuilder();
        } else {
          return bbox_ == null ? Osmformat.HeaderBBox.getDefaultInstance() : bbox_;
        }
      }

      /** <code>optional .HeaderBBox bbox = 1;</code> */
      private proto4.SingleFieldBuilder<
              Osmformat.HeaderBBox, Osmformat.HeaderBBox.Builder, Osmformat.HeaderBBoxOrBuilder>
          getBboxFieldBuilder() {
        if (bboxBuilder_ == null) {
          bboxBuilder_ =
              new proto4.SingleFieldBuilder<
                  Osmformat.HeaderBBox,
                  Osmformat.HeaderBBox.Builder,
                  Osmformat.HeaderBBoxOrBuilder>(getBbox(), getParentForChildren(), isClean());
          bbox_ = null;
        }
        return bboxBuilder_;
      }

      private proto4.LazyStringArrayList requiredFeatures_ = proto4.LazyStringArrayList.emptyList();

      private void ensureRequiredFeaturesIsMutable() {
        if (!requiredFeatures_.isModifiable()) {
          requiredFeatures_ = new proto4.LazyStringArrayList(requiredFeatures_);
        }
        bitField0_ |= 0x00000002;
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @return A list containing the requiredFeatures.
       */
      public proto4.ProtocolStringList getRequiredFeaturesList() {
        requiredFeatures_.makeImmutable();
        return requiredFeatures_;
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @return The count of requiredFeatures.
       */
      public int getRequiredFeaturesCount() {
        return requiredFeatures_.size();
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @param index The index of the element to return.
       * @return The requiredFeatures at the given index.
       */
      public String getRequiredFeatures(int index) {
        return requiredFeatures_.get(index);
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @param index The index of the value to return.
       * @return The bytes of the requiredFeatures at the given index.
       */
      public proto4.ByteString getRequiredFeaturesBytes(int index) {
        return requiredFeatures_.getByteString(index);
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @param index The index to set the value at.
       * @param value The requiredFeatures to set.
       * @return This builder for chaining.
       */
      public Builder setRequiredFeatures(int index, String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureRequiredFeaturesIsMutable();
        requiredFeatures_.set(index, value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @param value The requiredFeatures to add.
       * @return This builder for chaining.
       */
      public Builder addRequiredFeatures(String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureRequiredFeaturesIsMutable();
        requiredFeatures_.add(value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @param values The requiredFeatures to add.
       * @return This builder for chaining.
       */
      public Builder addAllRequiredFeatures(Iterable<String> values) {
        ensureRequiredFeaturesIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, requiredFeatures_);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearRequiredFeatures() {
        requiredFeatures_ = proto4.LazyStringArrayList.emptyList();
        bitField0_ = (bitField0_ & ~0x00000002);
        ;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Additional tags to aid in parsing this dataset
       * </pre>
       *
       * <code>repeated string required_features = 4;</code>
       *
       * @param value The bytes of the requiredFeatures to add.
       * @return This builder for chaining.
       */
      public Builder addRequiredFeaturesBytes(proto4.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureRequiredFeaturesIsMutable();
        requiredFeatures_.add(value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      private proto4.LazyStringArrayList optionalFeatures_ = proto4.LazyStringArrayList.emptyList();

      private void ensureOptionalFeaturesIsMutable() {
        if (!optionalFeatures_.isModifiable()) {
          optionalFeatures_ = new proto4.LazyStringArrayList(optionalFeatures_);
        }
        bitField0_ |= 0x00000004;
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @return A list containing the optionalFeatures.
       */
      public proto4.ProtocolStringList getOptionalFeaturesList() {
        optionalFeatures_.makeImmutable();
        return optionalFeatures_;
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @return The count of optionalFeatures.
       */
      public int getOptionalFeaturesCount() {
        return optionalFeatures_.size();
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @param index The index of the element to return.
       * @return The optionalFeatures at the given index.
       */
      public String getOptionalFeatures(int index) {
        return optionalFeatures_.get(index);
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @param index The index of the value to return.
       * @return The bytes of the optionalFeatures at the given index.
       */
      public proto4.ByteString getOptionalFeaturesBytes(int index) {
        return optionalFeatures_.getByteString(index);
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @param index The index to set the value at.
       * @param value The optionalFeatures to set.
       * @return This builder for chaining.
       */
      public Builder setOptionalFeatures(int index, String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureOptionalFeaturesIsMutable();
        optionalFeatures_.set(index, value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @param value The optionalFeatures to add.
       * @return This builder for chaining.
       */
      public Builder addOptionalFeatures(String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureOptionalFeaturesIsMutable();
        optionalFeatures_.add(value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @param values The optionalFeatures to add.
       * @return This builder for chaining.
       */
      public Builder addAllOptionalFeatures(Iterable<String> values) {
        ensureOptionalFeaturesIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, optionalFeatures_);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearOptionalFeatures() {
        optionalFeatures_ = proto4.LazyStringArrayList.emptyList();
        bitField0_ = (bitField0_ & ~0x00000004);
        ;
        onChanged();
        return this;
      }

      /**
       * <code>repeated string optional_features = 5;</code>
       *
       * @param value The bytes of the optionalFeatures to add.
       * @return This builder for chaining.
       */
      public Builder addOptionalFeaturesBytes(proto4.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureOptionalFeaturesIsMutable();
        optionalFeatures_.add(value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      private Object writingprogram_ = "";

      /**
       * <code>optional string writingprogram = 16;</code>
       *
       * @return Whether the writingprogram field is set.
       */
      public boolean hasWritingprogram() {
        return ((bitField0_ & 0x00000008) != 0);
      }

      /**
       * <code>optional string writingprogram = 16;</code>
       *
       * @return The writingprogram.
       */
      public String getWritingprogram() {
        Object ref = writingprogram_;
        if (!(ref instanceof String)) {
          proto4.ByteString bs = (proto4.ByteString) ref;
          String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            writingprogram_ = s;
          }
          return s;
        } else {
          return (String) ref;
        }
      }

      /**
       * <code>optional string writingprogram = 16;</code>
       *
       * @return The bytes for writingprogram.
       */
      public proto4.ByteString getWritingprogramBytes() {
        Object ref = writingprogram_;
        if (ref instanceof String) {
          proto4.ByteString b = proto4.ByteString.copyFromUtf8((String) ref);
          writingprogram_ = b;
          return b;
        } else {
          return (proto4.ByteString) ref;
        }
      }

      /**
       * <code>optional string writingprogram = 16;</code>
       *
       * @param value The writingprogram to set.
       * @return This builder for chaining.
       */
      public Builder setWritingprogram(String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        writingprogram_ = value;
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       * <code>optional string writingprogram = 16;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearWritingprogram() {
        writingprogram_ = getDefaultInstance().getWritingprogram();
        bitField0_ = (bitField0_ & ~0x00000008);
        onChanged();
        return this;
      }

      /**
       * <code>optional string writingprogram = 16;</code>
       *
       * @param value The bytes for writingprogram to set.
       * @return This builder for chaining.
       */
      public Builder setWritingprogramBytes(proto4.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        writingprogram_ = value;
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      private Object source_ = "";

      /**
       *
       *
       * <pre>
       * From the bbox field.
       * </pre>
       *
       * <code>optional string source = 17;</code>
       *
       * @return Whether the source field is set.
       */
      public boolean hasSource() {
        return ((bitField0_ & 0x00000010) != 0);
      }

      /**
       *
       *
       * <pre>
       * From the bbox field.
       * </pre>
       *
       * <code>optional string source = 17;</code>
       *
       * @return The source.
       */
      public String getSource() {
        Object ref = source_;
        if (!(ref instanceof String)) {
          proto4.ByteString bs = (proto4.ByteString) ref;
          String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            source_ = s;
          }
          return s;
        } else {
          return (String) ref;
        }
      }

      /**
       *
       *
       * <pre>
       * From the bbox field.
       * </pre>
       *
       * <code>optional string source = 17;</code>
       *
       * @return The bytes for source.
       */
      public proto4.ByteString getSourceBytes() {
        Object ref = source_;
        if (ref instanceof String) {
          proto4.ByteString b = proto4.ByteString.copyFromUtf8((String) ref);
          source_ = b;
          return b;
        } else {
          return (proto4.ByteString) ref;
        }
      }

      /**
       *
       *
       * <pre>
       * From the bbox field.
       * </pre>
       *
       * <code>optional string source = 17;</code>
       *
       * @param value The source to set.
       * @return This builder for chaining.
       */
      public Builder setSource(String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        source_ = value;
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * From the bbox field.
       * </pre>
       *
       * <code>optional string source = 17;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearSource() {
        source_ = getDefaultInstance().getSource();
        bitField0_ = (bitField0_ & ~0x00000010);
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * From the bbox field.
       * </pre>
       *
       * <code>optional string source = 17;</code>
       *
       * @param value The bytes for source to set.
       * @return This builder for chaining.
       */
      public Builder setSourceBytes(proto4.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        source_ = value;
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      private long osmosisReplicationTimestamp_;

      /**
       *
       *
       * <pre>
       * replication timestamp, expressed in seconds since the epoch,
       * otherwise the same value as in the "timestamp=..." field
       * in the state.txt file used by Osmosis
       * </pre>
       *
       * <code>optional int64 osmosis_replication_timestamp = 32;</code>
       *
       * @return Whether the osmosisReplicationTimestamp field is set.
       */
      @Override
      public boolean hasOsmosisReplicationTimestamp() {
        return ((bitField0_ & 0x00000020) != 0);
      }

      /**
       *
       *
       * <pre>
       * replication timestamp, expressed in seconds since the epoch,
       * otherwise the same value as in the "timestamp=..." field
       * in the state.txt file used by Osmosis
       * </pre>
       *
       * <code>optional int64 osmosis_replication_timestamp = 32;</code>
       *
       * @return The osmosisReplicationTimestamp.
       */
      @Override
      public long getOsmosisReplicationTimestamp() {
        return osmosisReplicationTimestamp_;
      }

      /**
       *
       *
       * <pre>
       * replication timestamp, expressed in seconds since the epoch,
       * otherwise the same value as in the "timestamp=..." field
       * in the state.txt file used by Osmosis
       * </pre>
       *
       * <code>optional int64 osmosis_replication_timestamp = 32;</code>
       *
       * @param value The osmosisReplicationTimestamp to set.
       * @return This builder for chaining.
       */
      public Builder setOsmosisReplicationTimestamp(long value) {

        osmosisReplicationTimestamp_ = value;
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * replication timestamp, expressed in seconds since the epoch,
       * otherwise the same value as in the "timestamp=..." field
       * in the state.txt file used by Osmosis
       * </pre>
       *
       * <code>optional int64 osmosis_replication_timestamp = 32;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearOsmosisReplicationTimestamp() {
        bitField0_ = (bitField0_ & ~0x00000020);
        osmosisReplicationTimestamp_ = 0L;
        onChanged();
        return this;
      }

      private long osmosisReplicationSequenceNumber_;

      /**
       *
       *
       * <pre>
       * replication sequence number (sequenceNumber in state.txt)
       * </pre>
       *
       * <code>optional int64 osmosis_replication_sequence_number = 33;</code>
       *
       * @return Whether the osmosisReplicationSequenceNumber field is set.
       */
      @Override
      public boolean hasOsmosisReplicationSequenceNumber() {
        return ((bitField0_ & 0x00000040) != 0);
      }

      /**
       *
       *
       * <pre>
       * replication sequence number (sequenceNumber in state.txt)
       * </pre>
       *
       * <code>optional int64 osmosis_replication_sequence_number = 33;</code>
       *
       * @return The osmosisReplicationSequenceNumber.
       */
      @Override
      public long getOsmosisReplicationSequenceNumber() {
        return osmosisReplicationSequenceNumber_;
      }

      /**
       *
       *
       * <pre>
       * replication sequence number (sequenceNumber in state.txt)
       * </pre>
       *
       * <code>optional int64 osmosis_replication_sequence_number = 33;</code>
       *
       * @param value The osmosisReplicationSequenceNumber to set.
       * @return This builder for chaining.
       */
      public Builder setOsmosisReplicationSequenceNumber(long value) {

        osmosisReplicationSequenceNumber_ = value;
        bitField0_ |= 0x00000040;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * replication sequence number (sequenceNumber in state.txt)
       * </pre>
       *
       * <code>optional int64 osmosis_replication_sequence_number = 33;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearOsmosisReplicationSequenceNumber() {
        bitField0_ = (bitField0_ & ~0x00000040);
        osmosisReplicationSequenceNumber_ = 0L;
        onChanged();
        return this;
      }

      private Object osmosisReplicationBaseUrl_ = "";

      /**
       *
       *
       * <pre>
       * replication base URL (from Osmosis' configuration.txt file)
       * </pre>
       *
       * <code>optional string osmosis_replication_base_url = 34;</code>
       *
       * @return Whether the osmosisReplicationBaseUrl field is set.
       */
      public boolean hasOsmosisReplicationBaseUrl() {
        return ((bitField0_ & 0x00000080) != 0);
      }

      /**
       *
       *
       * <pre>
       * replication base URL (from Osmosis' configuration.txt file)
       * </pre>
       *
       * <code>optional string osmosis_replication_base_url = 34;</code>
       *
       * @return The osmosisReplicationBaseUrl.
       */
      public String getOsmosisReplicationBaseUrl() {
        Object ref = osmosisReplicationBaseUrl_;
        if (!(ref instanceof String)) {
          proto4.ByteString bs = (proto4.ByteString) ref;
          String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            osmosisReplicationBaseUrl_ = s;
          }
          return s;
        } else {
          return (String) ref;
        }
      }

      /**
       *
       *
       * <pre>
       * replication base URL (from Osmosis' configuration.txt file)
       * </pre>
       *
       * <code>optional string osmosis_replication_base_url = 34;</code>
       *
       * @return The bytes for osmosisReplicationBaseUrl.
       */
      public proto4.ByteString getOsmosisReplicationBaseUrlBytes() {
        Object ref = osmosisReplicationBaseUrl_;
        if (ref instanceof String) {
          proto4.ByteString b = proto4.ByteString.copyFromUtf8((String) ref);
          osmosisReplicationBaseUrl_ = b;
          return b;
        } else {
          return (proto4.ByteString) ref;
        }
      }

      /**
       *
       *
       * <pre>
       * replication base URL (from Osmosis' configuration.txt file)
       * </pre>
       *
       * <code>optional string osmosis_replication_base_url = 34;</code>
       *
       * @param value The osmosisReplicationBaseUrl to set.
       * @return This builder for chaining.
       */
      public Builder setOsmosisReplicationBaseUrl(String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        osmosisReplicationBaseUrl_ = value;
        bitField0_ |= 0x00000080;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * replication base URL (from Osmosis' configuration.txt file)
       * </pre>
       *
       * <code>optional string osmosis_replication_base_url = 34;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearOsmosisReplicationBaseUrl() {
        osmosisReplicationBaseUrl_ = getDefaultInstance().getOsmosisReplicationBaseUrl();
        bitField0_ = (bitField0_ & ~0x00000080);
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * replication base URL (from Osmosis' configuration.txt file)
       * </pre>
       *
       * <code>optional string osmosis_replication_base_url = 34;</code>
       *
       * @param value The bytes for osmosisReplicationBaseUrl to set.
       * @return This builder for chaining.
       */
      public Builder setOsmosisReplicationBaseUrlBytes(proto4.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        osmosisReplicationBaseUrl_ = value;
        bitField0_ |= 0x00000080;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:HeaderBlock)
    }

    // @@protoc_insertion_point(class_scope:HeaderBlock)
    private static final Osmformat.HeaderBlock DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.HeaderBlock();
    }

    public static Osmformat.HeaderBlock getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<HeaderBlock> PARSER =
        new proto4.AbstractParser<HeaderBlock>() {
          @Override
          public HeaderBlock parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<HeaderBlock> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<HeaderBlock> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.HeaderBlock getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface HeaderBBoxOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:HeaderBBox)
      proto4.MessageOrBuilder {

    /**
     * <code>required sint64 left = 1;</code>
     *
     * @return Whether the left field is set.
     */
    boolean hasLeft();

    /**
     * <code>required sint64 left = 1;</code>
     *
     * @return The left.
     */
    long getLeft();

    /**
     * <code>required sint64 right = 2;</code>
     *
     * @return Whether the right field is set.
     */
    boolean hasRight();

    /**
     * <code>required sint64 right = 2;</code>
     *
     * @return The right.
     */
    long getRight();

    /**
     * <code>required sint64 top = 3;</code>
     *
     * @return Whether the top field is set.
     */
    boolean hasTop();

    /**
     * <code>required sint64 top = 3;</code>
     *
     * @return The top.
     */
    long getTop();

    /**
     * <code>required sint64 bottom = 4;</code>
     *
     * @return Whether the bottom field is set.
     */
    boolean hasBottom();

    /**
     * <code>required sint64 bottom = 4;</code>
     *
     * @return The bottom.
     */
    long getBottom();
  }

  /** Protobuf type {@code HeaderBBox} */
  public static final class HeaderBBox extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:HeaderBBox)
      HeaderBBoxOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          HeaderBBox.class.getName());
    }

    // Use HeaderBBox.newBuilder() to construct.
    private HeaderBBox(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private HeaderBBox() {}

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBBox_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBBox_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.HeaderBBox.class, Osmformat.HeaderBBox.Builder.class);
    }

    private int bitField0_;
    public static final int LEFT_FIELD_NUMBER = 1;
    private long left_ = 0L;

    /**
     * <code>required sint64 left = 1;</code>
     *
     * @return Whether the left field is set.
     */
    @Override
    public boolean hasLeft() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     * <code>required sint64 left = 1;</code>
     *
     * @return The left.
     */
    @Override
    public long getLeft() {
      return left_;
    }

    public static final int RIGHT_FIELD_NUMBER = 2;
    private long right_ = 0L;

    /**
     * <code>required sint64 right = 2;</code>
     *
     * @return Whether the right field is set.
     */
    @Override
    public boolean hasRight() {
      return ((bitField0_ & 0x00000002) != 0);
    }

    /**
     * <code>required sint64 right = 2;</code>
     *
     * @return The right.
     */
    @Override
    public long getRight() {
      return right_;
    }

    public static final int TOP_FIELD_NUMBER = 3;
    private long top_ = 0L;

    /**
     * <code>required sint64 top = 3;</code>
     *
     * @return Whether the top field is set.
     */
    @Override
    public boolean hasTop() {
      return ((bitField0_ & 0x00000004) != 0);
    }

    /**
     * <code>required sint64 top = 3;</code>
     *
     * @return The top.
     */
    @Override
    public long getTop() {
      return top_;
    }

    public static final int BOTTOM_FIELD_NUMBER = 4;
    private long bottom_ = 0L;

    /**
     * <code>required sint64 bottom = 4;</code>
     *
     * @return Whether the bottom field is set.
     */
    @Override
    public boolean hasBottom() {
      return ((bitField0_ & 0x00000008) != 0);
    }

    /**
     * <code>required sint64 bottom = 4;</code>
     *
     * @return The bottom.
     */
    @Override
    public long getBottom() {
      return bottom_;
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasLeft()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasRight()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasTop()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasBottom()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeSInt64(1, left_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeSInt64(2, right_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeSInt64(3, top_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeSInt64(4, bottom_);
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeSInt64Size(1, left_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += proto4.CodedOutputStream.computeSInt64Size(2, right_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += proto4.CodedOutputStream.computeSInt64Size(3, top_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += proto4.CodedOutputStream.computeSInt64Size(4, bottom_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.HeaderBBox)) {
        return super.equals(obj);
      }
      Osmformat.HeaderBBox other = (Osmformat.HeaderBBox) obj;

      if (hasLeft() != other.hasLeft()) return false;
      if (hasLeft()) {
        if (getLeft() != other.getLeft()) return false;
      }
      if (hasRight() != other.hasRight()) return false;
      if (hasRight()) {
        if (getRight() != other.getRight()) return false;
      }
      if (hasTop() != other.hasTop()) return false;
      if (hasTop()) {
        if (getTop() != other.getTop()) return false;
      }
      if (hasBottom() != other.hasBottom()) return false;
      if (hasBottom()) {
        if (getBottom() != other.getBottom()) return false;
      }
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasLeft()) {
        hash = (37 * hash) + LEFT_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getLeft());
      }
      if (hasRight()) {
        hash = (37 * hash) + RIGHT_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getRight());
      }
      if (hasTop()) {
        hash = (37 * hash) + TOP_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getTop());
      }
      if (hasBottom()) {
        hash = (37 * hash) + BOTTOM_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getBottom());
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.HeaderBBox parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.HeaderBBox parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.HeaderBBox parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.HeaderBBox parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.HeaderBBox parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.HeaderBBox parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.HeaderBBox parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.HeaderBBox parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.HeaderBBox parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.HeaderBBox parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.HeaderBBox parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.HeaderBBox parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.HeaderBBox prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /** Protobuf type {@code HeaderBBox} */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:HeaderBBox)
        Osmformat.HeaderBBoxOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBBox_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBBox_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.HeaderBBox.class, Osmformat.HeaderBBox.Builder.class);
      }

      // Construct using Osmformat.HeaderBBox.newBuilder()
      private Builder() {}

      private Builder(BuilderParent parent) {
        super(parent);
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        left_ = 0L;
        right_ = 0L;
        top_ = 0L;
        bottom_ = 0L;
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_HeaderBBox_descriptor;
      }

      @Override
      public Osmformat.HeaderBBox getDefaultInstanceForType() {
        return Osmformat.HeaderBBox.getDefaultInstance();
      }

      @Override
      public Osmformat.HeaderBBox build() {
        Osmformat.HeaderBBox result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.HeaderBBox buildPartial() {
        Osmformat.HeaderBBox result = new Osmformat.HeaderBBox(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.HeaderBBox result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.left_ = left_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.right_ = right_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.top_ = top_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.bottom_ = bottom_;
          to_bitField0_ |= 0x00000008;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.HeaderBBox) {
          return mergeFrom((Osmformat.HeaderBBox) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.HeaderBBox other) {
        if (other == Osmformat.HeaderBBox.getDefaultInstance()) return this;
        if (other.hasLeft()) {
          setLeft(other.getLeft());
        }
        if (other.hasRight()) {
          setRight(other.getRight());
        }
        if (other.hasTop()) {
          setTop(other.getTop());
        }
        if (other.hasBottom()) {
          setBottom(other.getBottom());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        if (!hasLeft()) {
          return false;
        }
        if (!hasRight()) {
          return false;
        }
        if (!hasTop()) {
          return false;
        }
        if (!hasBottom()) {
          return false;
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8:
                {
                  left_ = input.readSInt64();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 8
              case 16:
                {
                  right_ = input.readSInt64();
                  bitField0_ |= 0x00000002;
                  break;
                } // case 16
              case 24:
                {
                  top_ = input.readSInt64();
                  bitField0_ |= 0x00000004;
                  break;
                } // case 24
              case 32:
                {
                  bottom_ = input.readSInt64();
                  bitField0_ |= 0x00000008;
                  break;
                } // case 32
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private long left_;

      /**
       * <code>required sint64 left = 1;</code>
       *
       * @return Whether the left field is set.
       */
      @Override
      public boolean hasLeft() {
        return ((bitField0_ & 0x00000001) != 0);
      }

      /**
       * <code>required sint64 left = 1;</code>
       *
       * @return The left.
       */
      @Override
      public long getLeft() {
        return left_;
      }

      /**
       * <code>required sint64 left = 1;</code>
       *
       * @param value The left to set.
       * @return This builder for chaining.
       */
      public Builder setLeft(long value) {

        left_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>required sint64 left = 1;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearLeft() {
        bitField0_ = (bitField0_ & ~0x00000001);
        left_ = 0L;
        onChanged();
        return this;
      }

      private long right_;

      /**
       * <code>required sint64 right = 2;</code>
       *
       * @return Whether the right field is set.
       */
      @Override
      public boolean hasRight() {
        return ((bitField0_ & 0x00000002) != 0);
      }

      /**
       * <code>required sint64 right = 2;</code>
       *
       * @return The right.
       */
      @Override
      public long getRight() {
        return right_;
      }

      /**
       * <code>required sint64 right = 2;</code>
       *
       * @param value The right to set.
       * @return This builder for chaining.
       */
      public Builder setRight(long value) {

        right_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       * <code>required sint64 right = 2;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearRight() {
        bitField0_ = (bitField0_ & ~0x00000002);
        right_ = 0L;
        onChanged();
        return this;
      }

      private long top_;

      /**
       * <code>required sint64 top = 3;</code>
       *
       * @return Whether the top field is set.
       */
      @Override
      public boolean hasTop() {
        return ((bitField0_ & 0x00000004) != 0);
      }

      /**
       * <code>required sint64 top = 3;</code>
       *
       * @return The top.
       */
      @Override
      public long getTop() {
        return top_;
      }

      /**
       * <code>required sint64 top = 3;</code>
       *
       * @param value The top to set.
       * @return This builder for chaining.
       */
      public Builder setTop(long value) {

        top_ = value;
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>required sint64 top = 3;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearTop() {
        bitField0_ = (bitField0_ & ~0x00000004);
        top_ = 0L;
        onChanged();
        return this;
      }

      private long bottom_;

      /**
       * <code>required sint64 bottom = 4;</code>
       *
       * @return Whether the bottom field is set.
       */
      @Override
      public boolean hasBottom() {
        return ((bitField0_ & 0x00000008) != 0);
      }

      /**
       * <code>required sint64 bottom = 4;</code>
       *
       * @return The bottom.
       */
      @Override
      public long getBottom() {
        return bottom_;
      }

      /**
       * <code>required sint64 bottom = 4;</code>
       *
       * @param value The bottom to set.
       * @return This builder for chaining.
       */
      public Builder setBottom(long value) {

        bottom_ = value;
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       * <code>required sint64 bottom = 4;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearBottom() {
        bitField0_ = (bitField0_ & ~0x00000008);
        bottom_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:HeaderBBox)
    }

    // @@protoc_insertion_point(class_scope:HeaderBBox)
    private static final Osmformat.HeaderBBox DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.HeaderBBox();
    }

    public static Osmformat.HeaderBBox getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<HeaderBBox> PARSER =
        new proto4.AbstractParser<HeaderBBox>() {
          @Override
          public HeaderBBox parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<HeaderBBox> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<HeaderBBox> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.HeaderBBox getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface PrimitiveBlockOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:PrimitiveBlock)
      proto4.MessageOrBuilder {

    /**
     * <code>required .StringTable stringtable = 1;</code>
     *
     * @return Whether the stringtable field is set.
     */
    boolean hasStringtable();

    /**
     * <code>required .StringTable stringtable = 1;</code>
     *
     * @return The stringtable.
     */
    Osmformat.StringTable getStringtable();

    /** <code>required .StringTable stringtable = 1;</code> */
    Osmformat.StringTableOrBuilder getStringtableOrBuilder();

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    java.util.List<Osmformat.PrimitiveGroup> getPrimitivegroupList();

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    Osmformat.PrimitiveGroup getPrimitivegroup(int index);

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    int getPrimitivegroupCount();

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    java.util.List<? extends Osmformat.PrimitiveGroupOrBuilder> getPrimitivegroupOrBuilderList();

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    Osmformat.PrimitiveGroupOrBuilder getPrimitivegroupOrBuilder(int index);

    /**
     *
     *
     * <pre>
     * Granularity, units of nanodegrees, used to store coordinates in this block
     * </pre>
     *
     * <code>optional int32 granularity = 17 [default = 100];</code>
     *
     * @return Whether the granularity field is set.
     */
    boolean hasGranularity();

    /**
     *
     *
     * <pre>
     * Granularity, units of nanodegrees, used to store coordinates in this block
     * </pre>
     *
     * <code>optional int32 granularity = 17 [default = 100];</code>
     *
     * @return The granularity.
     */
    int getGranularity();

    /**
     *
     *
     * <pre>
     * Offset value between the output coordinates coordinates and the granularity grid in unites of nanodegrees.
     * </pre>
     *
     * <code>optional int64 lat_offset = 19 [default = 0];</code>
     *
     * @return Whether the latOffset field is set.
     */
    boolean hasLatOffset();

    /**
     *
     *
     * <pre>
     * Offset value between the output coordinates coordinates and the granularity grid in unites of nanodegrees.
     * </pre>
     *
     * <code>optional int64 lat_offset = 19 [default = 0];</code>
     *
     * @return The latOffset.
     */
    long getLatOffset();

    /**
     * <code>optional int64 lon_offset = 20 [default = 0];</code>
     *
     * @return Whether the lonOffset field is set.
     */
    boolean hasLonOffset();

    /**
     * <code>optional int64 lon_offset = 20 [default = 0];</code>
     *
     * @return The lonOffset.
     */
    long getLonOffset();

    /**
     *
     *
     * <pre>
     * Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
     * </pre>
     *
     * <code>optional int32 date_granularity = 18 [default = 1000];</code>
     *
     * @return Whether the dateGranularity field is set.
     */
    boolean hasDateGranularity();

    /**
     *
     *
     * <pre>
     * Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
     * </pre>
     *
     * <code>optional int32 date_granularity = 18 [default = 1000];</code>
     *
     * @return The dateGranularity.
     */
    int getDateGranularity();
  }

  /** Protobuf type {@code PrimitiveBlock} */
  public static final class PrimitiveBlock extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:PrimitiveBlock)
      PrimitiveBlockOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          PrimitiveBlock.class.getName());
    }

    // Use PrimitiveBlock.newBuilder() to construct.
    private PrimitiveBlock(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private PrimitiveBlock() {
      primitivegroup_ = java.util.Collections.emptyList();
      granularity_ = 100;
      dateGranularity_ = 1000;
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_PrimitiveBlock_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_PrimitiveBlock_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.PrimitiveBlock.class, Osmformat.PrimitiveBlock.Builder.class);
    }

    private int bitField0_;
    public static final int STRINGTABLE_FIELD_NUMBER = 1;
    private Osmformat.StringTable stringtable_;

    /**
     * <code>required .StringTable stringtable = 1;</code>
     *
     * @return Whether the stringtable field is set.
     */
    @Override
    public boolean hasStringtable() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     * <code>required .StringTable stringtable = 1;</code>
     *
     * @return The stringtable.
     */
    @Override
    public Osmformat.StringTable getStringtable() {
      return stringtable_ == null ? Osmformat.StringTable.getDefaultInstance() : stringtable_;
    }

    /** <code>required .StringTable stringtable = 1;</code> */
    @Override
    public Osmformat.StringTableOrBuilder getStringtableOrBuilder() {
      return stringtable_ == null ? Osmformat.StringTable.getDefaultInstance() : stringtable_;
    }

    public static final int PRIMITIVEGROUP_FIELD_NUMBER = 2;

    @SuppressWarnings("serial")
    private java.util.List<Osmformat.PrimitiveGroup> primitivegroup_;

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    @Override
    public java.util.List<Osmformat.PrimitiveGroup> getPrimitivegroupList() {
      return primitivegroup_;
    }

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    @Override
    public java.util.List<? extends Osmformat.PrimitiveGroupOrBuilder>
        getPrimitivegroupOrBuilderList() {
      return primitivegroup_;
    }

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    @Override
    public int getPrimitivegroupCount() {
      return primitivegroup_.size();
    }

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    @Override
    public Osmformat.PrimitiveGroup getPrimitivegroup(int index) {
      return primitivegroup_.get(index);
    }

    /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
    @Override
    public Osmformat.PrimitiveGroupOrBuilder getPrimitivegroupOrBuilder(int index) {
      return primitivegroup_.get(index);
    }

    public static final int GRANULARITY_FIELD_NUMBER = 17;
    private int granularity_ = 100;

    /**
     *
     *
     * <pre>
     * Granularity, units of nanodegrees, used to store coordinates in this block
     * </pre>
     *
     * <code>optional int32 granularity = 17 [default = 100];</code>
     *
     * @return Whether the granularity field is set.
     */
    @Override
    public boolean hasGranularity() {
      return ((bitField0_ & 0x00000002) != 0);
    }

    /**
     *
     *
     * <pre>
     * Granularity, units of nanodegrees, used to store coordinates in this block
     * </pre>
     *
     * <code>optional int32 granularity = 17 [default = 100];</code>
     *
     * @return The granularity.
     */
    @Override
    public int getGranularity() {
      return granularity_;
    }

    public static final int LAT_OFFSET_FIELD_NUMBER = 19;
    private long latOffset_ = 0L;

    /**
     *
     *
     * <pre>
     * Offset value between the output coordinates coordinates and the granularity grid in unites of nanodegrees.
     * </pre>
     *
     * <code>optional int64 lat_offset = 19 [default = 0];</code>
     *
     * @return Whether the latOffset field is set.
     */
    @Override
    public boolean hasLatOffset() {
      return ((bitField0_ & 0x00000004) != 0);
    }

    /**
     *
     *
     * <pre>
     * Offset value between the output coordinates coordinates and the granularity grid in unites of nanodegrees.
     * </pre>
     *
     * <code>optional int64 lat_offset = 19 [default = 0];</code>
     *
     * @return The latOffset.
     */
    @Override
    public long getLatOffset() {
      return latOffset_;
    }

    public static final int LON_OFFSET_FIELD_NUMBER = 20;
    private long lonOffset_ = 0L;

    /**
     * <code>optional int64 lon_offset = 20 [default = 0];</code>
     *
     * @return Whether the lonOffset field is set.
     */
    @Override
    public boolean hasLonOffset() {
      return ((bitField0_ & 0x00000008) != 0);
    }

    /**
     * <code>optional int64 lon_offset = 20 [default = 0];</code>
     *
     * @return The lonOffset.
     */
    @Override
    public long getLonOffset() {
      return lonOffset_;
    }

    public static final int DATE_GRANULARITY_FIELD_NUMBER = 18;
    private int dateGranularity_ = 1000;

    /**
     *
     *
     * <pre>
     * Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
     * </pre>
     *
     * <code>optional int32 date_granularity = 18 [default = 1000];</code>
     *
     * @return Whether the dateGranularity field is set.
     */
    @Override
    public boolean hasDateGranularity() {
      return ((bitField0_ & 0x00000010) != 0);
    }

    /**
     *
     *
     * <pre>
     * Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
     * </pre>
     *
     * <code>optional int32 date_granularity = 18 [default = 1000];</code>
     *
     * @return The dateGranularity.
     */
    @Override
    public int getDateGranularity() {
      return dateGranularity_;
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasStringtable()) {
        memoizedIsInitialized = 0;
        return false;
      }
      for (int i = 0; i < getPrimitivegroupCount(); i++) {
        if (!getPrimitivegroup(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getStringtable());
      }
      for (int i = 0; i < primitivegroup_.size(); i++) {
        output.writeMessage(2, primitivegroup_.get(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeInt32(17, granularity_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeInt32(18, dateGranularity_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeInt64(19, latOffset_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeInt64(20, lonOffset_);
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeMessageSize(1, getStringtable());
      }
      for (int i = 0; i < primitivegroup_.size(); i++) {
        size += proto4.CodedOutputStream.computeMessageSize(2, primitivegroup_.get(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += proto4.CodedOutputStream.computeInt32Size(17, granularity_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += proto4.CodedOutputStream.computeInt32Size(18, dateGranularity_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(19, latOffset_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(20, lonOffset_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.PrimitiveBlock)) {
        return super.equals(obj);
      }
      Osmformat.PrimitiveBlock other = (Osmformat.PrimitiveBlock) obj;

      if (hasStringtable() != other.hasStringtable()) return false;
      if (hasStringtable()) {
        if (!getStringtable().equals(other.getStringtable())) return false;
      }
      if (!getPrimitivegroupList().equals(other.getPrimitivegroupList())) return false;
      if (hasGranularity() != other.hasGranularity()) return false;
      if (hasGranularity()) {
        if (getGranularity() != other.getGranularity()) return false;
      }
      if (hasLatOffset() != other.hasLatOffset()) return false;
      if (hasLatOffset()) {
        if (getLatOffset() != other.getLatOffset()) return false;
      }
      if (hasLonOffset() != other.hasLonOffset()) return false;
      if (hasLonOffset()) {
        if (getLonOffset() != other.getLonOffset()) return false;
      }
      if (hasDateGranularity() != other.hasDateGranularity()) return false;
      if (hasDateGranularity()) {
        if (getDateGranularity() != other.getDateGranularity()) return false;
      }
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasStringtable()) {
        hash = (37 * hash) + STRINGTABLE_FIELD_NUMBER;
        hash = (53 * hash) + getStringtable().hashCode();
      }
      if (getPrimitivegroupCount() > 0) {
        hash = (37 * hash) + PRIMITIVEGROUP_FIELD_NUMBER;
        hash = (53 * hash) + getPrimitivegroupList().hashCode();
      }
      if (hasGranularity()) {
        hash = (37 * hash) + GRANULARITY_FIELD_NUMBER;
        hash = (53 * hash) + getGranularity();
      }
      if (hasLatOffset()) {
        hash = (37 * hash) + LAT_OFFSET_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getLatOffset());
      }
      if (hasLonOffset()) {
        hash = (37 * hash) + LON_OFFSET_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getLonOffset());
      }
      if (hasDateGranularity()) {
        hash = (37 * hash) + DATE_GRANULARITY_FIELD_NUMBER;
        hash = (53 * hash) + getDateGranularity();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.PrimitiveBlock parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.PrimitiveBlock parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.PrimitiveBlock parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.PrimitiveBlock parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.PrimitiveBlock parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.PrimitiveBlock parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.PrimitiveBlock parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.PrimitiveBlock parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.PrimitiveBlock parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.PrimitiveBlock parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.PrimitiveBlock parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.PrimitiveBlock parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.PrimitiveBlock prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /** Protobuf type {@code PrimitiveBlock} */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:PrimitiveBlock)
        Osmformat.PrimitiveBlockOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_PrimitiveBlock_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat
            .internal_static_org_apache_sedona_osm_build_PrimitiveBlock_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.PrimitiveBlock.class, Osmformat.PrimitiveBlock.Builder.class);
      }

      // Construct using Osmformat.PrimitiveBlock.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }

      private void maybeForceBuilderInitialization() {
        if (proto4.GeneratedMessage.alwaysUseFieldBuilders) {
          getStringtableFieldBuilder();
          getPrimitivegroupFieldBuilder();
        }
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        stringtable_ = null;
        if (stringtableBuilder_ != null) {
          stringtableBuilder_.dispose();
          stringtableBuilder_ = null;
        }
        if (primitivegroupBuilder_ == null) {
          primitivegroup_ = java.util.Collections.emptyList();
        } else {
          primitivegroup_ = null;
          primitivegroupBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        granularity_ = 100;
        latOffset_ = 0L;
        lonOffset_ = 0L;
        dateGranularity_ = 1000;
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_PrimitiveBlock_descriptor;
      }

      @Override
      public Osmformat.PrimitiveBlock getDefaultInstanceForType() {
        return Osmformat.PrimitiveBlock.getDefaultInstance();
      }

      @Override
      public Osmformat.PrimitiveBlock build() {
        Osmformat.PrimitiveBlock result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.PrimitiveBlock buildPartial() {
        Osmformat.PrimitiveBlock result = new Osmformat.PrimitiveBlock(this);
        buildPartialRepeatedFields(result);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartialRepeatedFields(Osmformat.PrimitiveBlock result) {
        if (primitivegroupBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0)) {
            primitivegroup_ = java.util.Collections.unmodifiableList(primitivegroup_);
            bitField0_ = (bitField0_ & ~0x00000002);
          }
          result.primitivegroup_ = primitivegroup_;
        } else {
          result.primitivegroup_ = primitivegroupBuilder_.build();
        }
      }

      private void buildPartial0(Osmformat.PrimitiveBlock result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.stringtable_ =
              stringtableBuilder_ == null ? stringtable_ : stringtableBuilder_.build();
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.granularity_ = granularity_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.latOffset_ = latOffset_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.lonOffset_ = lonOffset_;
          to_bitField0_ |= 0x00000008;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          result.dateGranularity_ = dateGranularity_;
          to_bitField0_ |= 0x00000010;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.PrimitiveBlock) {
          return mergeFrom((Osmformat.PrimitiveBlock) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.PrimitiveBlock other) {
        if (other == Osmformat.PrimitiveBlock.getDefaultInstance()) return this;
        if (other.hasStringtable()) {
          mergeStringtable(other.getStringtable());
        }
        if (primitivegroupBuilder_ == null) {
          if (!other.primitivegroup_.isEmpty()) {
            if (primitivegroup_.isEmpty()) {
              primitivegroup_ = other.primitivegroup_;
              bitField0_ = (bitField0_ & ~0x00000002);
            } else {
              ensurePrimitivegroupIsMutable();
              primitivegroup_.addAll(other.primitivegroup_);
            }
            onChanged();
          }
        } else {
          if (!other.primitivegroup_.isEmpty()) {
            if (primitivegroupBuilder_.isEmpty()) {
              primitivegroupBuilder_.dispose();
              primitivegroupBuilder_ = null;
              primitivegroup_ = other.primitivegroup_;
              bitField0_ = (bitField0_ & ~0x00000002);
              primitivegroupBuilder_ =
                  proto4.GeneratedMessage.alwaysUseFieldBuilders
                      ? getPrimitivegroupFieldBuilder()
                      : null;
            } else {
              primitivegroupBuilder_.addAllMessages(other.primitivegroup_);
            }
          }
        }
        if (other.hasGranularity()) {
          setGranularity(other.getGranularity());
        }
        if (other.hasLatOffset()) {
          setLatOffset(other.getLatOffset());
        }
        if (other.hasLonOffset()) {
          setLonOffset(other.getLonOffset());
        }
        if (other.hasDateGranularity()) {
          setDateGranularity(other.getDateGranularity());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        if (!hasStringtable()) {
          return false;
        }
        for (int i = 0; i < getPrimitivegroupCount(); i++) {
          if (!getPrimitivegroup(i).isInitialized()) {
            return false;
          }
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 10:
                {
                  input.readMessage(getStringtableFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
              case 18:
                {
                  Osmformat.PrimitiveGroup m =
                      input.readMessage(Osmformat.PrimitiveGroup.parser(), extensionRegistry);
                  if (primitivegroupBuilder_ == null) {
                    ensurePrimitivegroupIsMutable();
                    primitivegroup_.add(m);
                  } else {
                    primitivegroupBuilder_.addMessage(m);
                  }
                  break;
                } // case 18
              case 136:
                {
                  granularity_ = input.readInt32();
                  bitField0_ |= 0x00000004;
                  break;
                } // case 136
              case 144:
                {
                  dateGranularity_ = input.readInt32();
                  bitField0_ |= 0x00000020;
                  break;
                } // case 144
              case 152:
                {
                  latOffset_ = input.readInt64();
                  bitField0_ |= 0x00000008;
                  break;
                } // case 152
              case 160:
                {
                  lonOffset_ = input.readInt64();
                  bitField0_ |= 0x00000010;
                  break;
                } // case 160
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private Osmformat.StringTable stringtable_;
      private proto4.SingleFieldBuilder<
              Osmformat.StringTable, Osmformat.StringTable.Builder, Osmformat.StringTableOrBuilder>
          stringtableBuilder_;

      /**
       * <code>required .StringTable stringtable = 1;</code>
       *
       * @return Whether the stringtable field is set.
       */
      public boolean hasStringtable() {
        return ((bitField0_ & 0x00000001) != 0);
      }

      /**
       * <code>required .StringTable stringtable = 1;</code>
       *
       * @return The stringtable.
       */
      public Osmformat.StringTable getStringtable() {
        if (stringtableBuilder_ == null) {
          return stringtable_ == null ? Osmformat.StringTable.getDefaultInstance() : stringtable_;
        } else {
          return stringtableBuilder_.getMessage();
        }
      }

      /** <code>required .StringTable stringtable = 1;</code> */
      public Builder setStringtable(Osmformat.StringTable value) {
        if (stringtableBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          stringtable_ = value;
        } else {
          stringtableBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /** <code>required .StringTable stringtable = 1;</code> */
      public Builder setStringtable(Osmformat.StringTable.Builder builderForValue) {
        if (stringtableBuilder_ == null) {
          stringtable_ = builderForValue.build();
        } else {
          stringtableBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /** <code>required .StringTable stringtable = 1;</code> */
      public Builder mergeStringtable(Osmformat.StringTable value) {
        if (stringtableBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)
              && stringtable_ != null
              && stringtable_ != Osmformat.StringTable.getDefaultInstance()) {
            getStringtableBuilder().mergeFrom(value);
          } else {
            stringtable_ = value;
          }
        } else {
          stringtableBuilder_.mergeFrom(value);
        }
        if (stringtable_ != null) {
          bitField0_ |= 0x00000001;
          onChanged();
        }
        return this;
      }

      /** <code>required .StringTable stringtable = 1;</code> */
      public Builder clearStringtable() {
        bitField0_ = (bitField0_ & ~0x00000001);
        stringtable_ = null;
        if (stringtableBuilder_ != null) {
          stringtableBuilder_.dispose();
          stringtableBuilder_ = null;
        }
        onChanged();
        return this;
      }

      /** <code>required .StringTable stringtable = 1;</code> */
      public Osmformat.StringTable.Builder getStringtableBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getStringtableFieldBuilder().getBuilder();
      }

      /** <code>required .StringTable stringtable = 1;</code> */
      public Osmformat.StringTableOrBuilder getStringtableOrBuilder() {
        if (stringtableBuilder_ != null) {
          return stringtableBuilder_.getMessageOrBuilder();
        } else {
          return stringtable_ == null ? Osmformat.StringTable.getDefaultInstance() : stringtable_;
        }
      }

      /** <code>required .StringTable stringtable = 1;</code> */
      private proto4.SingleFieldBuilder<
              Osmformat.StringTable, Osmformat.StringTable.Builder, Osmformat.StringTableOrBuilder>
          getStringtableFieldBuilder() {
        if (stringtableBuilder_ == null) {
          stringtableBuilder_ =
              new proto4.SingleFieldBuilder<
                  Osmformat.StringTable,
                  Osmformat.StringTable.Builder,
                  Osmformat.StringTableOrBuilder>(
                  getStringtable(), getParentForChildren(), isClean());
          stringtable_ = null;
        }
        return stringtableBuilder_;
      }

      private java.util.List<Osmformat.PrimitiveGroup> primitivegroup_ =
          java.util.Collections.emptyList();

      private void ensurePrimitivegroupIsMutable() {
        if (!((bitField0_ & 0x00000002) != 0)) {
          primitivegroup_ = new java.util.ArrayList<Osmformat.PrimitiveGroup>(primitivegroup_);
          bitField0_ |= 0x00000002;
        }
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.PrimitiveGroup,
              Osmformat.PrimitiveGroup.Builder,
              Osmformat.PrimitiveGroupOrBuilder>
          primitivegroupBuilder_;

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public java.util.List<Osmformat.PrimitiveGroup> getPrimitivegroupList() {
        if (primitivegroupBuilder_ == null) {
          return java.util.Collections.unmodifiableList(primitivegroup_);
        } else {
          return primitivegroupBuilder_.getMessageList();
        }
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public int getPrimitivegroupCount() {
        if (primitivegroupBuilder_ == null) {
          return primitivegroup_.size();
        } else {
          return primitivegroupBuilder_.getCount();
        }
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Osmformat.PrimitiveGroup getPrimitivegroup(int index) {
        if (primitivegroupBuilder_ == null) {
          return primitivegroup_.get(index);
        } else {
          return primitivegroupBuilder_.getMessage(index);
        }
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder setPrimitivegroup(int index, Osmformat.PrimitiveGroup value) {
        if (primitivegroupBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensurePrimitivegroupIsMutable();
          primitivegroup_.set(index, value);
          onChanged();
        } else {
          primitivegroupBuilder_.setMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder setPrimitivegroup(
          int index, Osmformat.PrimitiveGroup.Builder builderForValue) {
        if (primitivegroupBuilder_ == null) {
          ensurePrimitivegroupIsMutable();
          primitivegroup_.set(index, builderForValue.build());
          onChanged();
        } else {
          primitivegroupBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder addPrimitivegroup(Osmformat.PrimitiveGroup value) {
        if (primitivegroupBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensurePrimitivegroupIsMutable();
          primitivegroup_.add(value);
          onChanged();
        } else {
          primitivegroupBuilder_.addMessage(value);
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder addPrimitivegroup(int index, Osmformat.PrimitiveGroup value) {
        if (primitivegroupBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensurePrimitivegroupIsMutable();
          primitivegroup_.add(index, value);
          onChanged();
        } else {
          primitivegroupBuilder_.addMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder addPrimitivegroup(Osmformat.PrimitiveGroup.Builder builderForValue) {
        if (primitivegroupBuilder_ == null) {
          ensurePrimitivegroupIsMutable();
          primitivegroup_.add(builderForValue.build());
          onChanged();
        } else {
          primitivegroupBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder addPrimitivegroup(
          int index, Osmformat.PrimitiveGroup.Builder builderForValue) {
        if (primitivegroupBuilder_ == null) {
          ensurePrimitivegroupIsMutable();
          primitivegroup_.add(index, builderForValue.build());
          onChanged();
        } else {
          primitivegroupBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder addAllPrimitivegroup(Iterable<? extends Osmformat.PrimitiveGroup> values) {
        if (primitivegroupBuilder_ == null) {
          ensurePrimitivegroupIsMutable();
          proto4.AbstractMessageLite.Builder.addAll(values, primitivegroup_);
          onChanged();
        } else {
          primitivegroupBuilder_.addAllMessages(values);
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder clearPrimitivegroup() {
        if (primitivegroupBuilder_ == null) {
          primitivegroup_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
          onChanged();
        } else {
          primitivegroupBuilder_.clear();
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Builder removePrimitivegroup(int index) {
        if (primitivegroupBuilder_ == null) {
          ensurePrimitivegroupIsMutable();
          primitivegroup_.remove(index);
          onChanged();
        } else {
          primitivegroupBuilder_.remove(index);
        }
        return this;
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Osmformat.PrimitiveGroup.Builder getPrimitivegroupBuilder(int index) {
        return getPrimitivegroupFieldBuilder().getBuilder(index);
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Osmformat.PrimitiveGroupOrBuilder getPrimitivegroupOrBuilder(int index) {
        if (primitivegroupBuilder_ == null) {
          return primitivegroup_.get(index);
        } else {
          return primitivegroupBuilder_.getMessageOrBuilder(index);
        }
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public java.util.List<? extends Osmformat.PrimitiveGroupOrBuilder>
          getPrimitivegroupOrBuilderList() {
        if (primitivegroupBuilder_ != null) {
          return primitivegroupBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(primitivegroup_);
        }
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Osmformat.PrimitiveGroup.Builder addPrimitivegroupBuilder() {
        return getPrimitivegroupFieldBuilder()
            .addBuilder(Osmformat.PrimitiveGroup.getDefaultInstance());
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public Osmformat.PrimitiveGroup.Builder addPrimitivegroupBuilder(int index) {
        return getPrimitivegroupFieldBuilder()
            .addBuilder(index, Osmformat.PrimitiveGroup.getDefaultInstance());
      }

      /** <code>repeated .PrimitiveGroup primitivegroup = 2;</code> */
      public java.util.List<Osmformat.PrimitiveGroup.Builder> getPrimitivegroupBuilderList() {
        return getPrimitivegroupFieldBuilder().getBuilderList();
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.PrimitiveGroup,
              Osmformat.PrimitiveGroup.Builder,
              Osmformat.PrimitiveGroupOrBuilder>
          getPrimitivegroupFieldBuilder() {
        if (primitivegroupBuilder_ == null) {
          primitivegroupBuilder_ =
              new proto4.RepeatedFieldBuilder<
                  Osmformat.PrimitiveGroup,
                  Osmformat.PrimitiveGroup.Builder,
                  Osmformat.PrimitiveGroupOrBuilder>(
                  primitivegroup_,
                  ((bitField0_ & 0x00000002) != 0),
                  getParentForChildren(),
                  isClean());
          primitivegroup_ = null;
        }
        return primitivegroupBuilder_;
      }

      private int granularity_ = 100;

      /**
       *
       *
       * <pre>
       * Granularity, units of nanodegrees, used to store coordinates in this block
       * </pre>
       *
       * <code>optional int32 granularity = 17 [default = 100];</code>
       *
       * @return Whether the granularity field is set.
       */
      @Override
      public boolean hasGranularity() {
        return ((bitField0_ & 0x00000004) != 0);
      }

      /**
       *
       *
       * <pre>
       * Granularity, units of nanodegrees, used to store coordinates in this block
       * </pre>
       *
       * <code>optional int32 granularity = 17 [default = 100];</code>
       *
       * @return The granularity.
       */
      @Override
      public int getGranularity() {
        return granularity_;
      }

      /**
       *
       *
       * <pre>
       * Granularity, units of nanodegrees, used to store coordinates in this block
       * </pre>
       *
       * <code>optional int32 granularity = 17 [default = 100];</code>
       *
       * @param value The granularity to set.
       * @return This builder for chaining.
       */
      public Builder setGranularity(int value) {

        granularity_ = value;
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Granularity, units of nanodegrees, used to store coordinates in this block
       * </pre>
       *
       * <code>optional int32 granularity = 17 [default = 100];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearGranularity() {
        bitField0_ = (bitField0_ & ~0x00000004);
        granularity_ = 100;
        onChanged();
        return this;
      }

      private long latOffset_;

      /**
       *
       *
       * <pre>
       * Offset value between the output coordinates coordinates and the granularity grid in unites of nanodegrees.
       * </pre>
       *
       * <code>optional int64 lat_offset = 19 [default = 0];</code>
       *
       * @return Whether the latOffset field is set.
       */
      @Override
      public boolean hasLatOffset() {
        return ((bitField0_ & 0x00000008) != 0);
      }

      /**
       *
       *
       * <pre>
       * Offset value between the output coordinates coordinates and the granularity grid in unites of nanodegrees.
       * </pre>
       *
       * <code>optional int64 lat_offset = 19 [default = 0];</code>
       *
       * @return The latOffset.
       */
      @Override
      public long getLatOffset() {
        return latOffset_;
      }

      /**
       *
       *
       * <pre>
       * Offset value between the output coordinates coordinates and the granularity grid in unites of nanodegrees.
       * </pre>
       *
       * <code>optional int64 lat_offset = 19 [default = 0];</code>
       *
       * @param value The latOffset to set.
       * @return This builder for chaining.
       */
      public Builder setLatOffset(long value) {

        latOffset_ = value;
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Offset value between the output coordinates coordinates and the granularity grid in unites of nanodegrees.
       * </pre>
       *
       * <code>optional int64 lat_offset = 19 [default = 0];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearLatOffset() {
        bitField0_ = (bitField0_ & ~0x00000008);
        latOffset_ = 0L;
        onChanged();
        return this;
      }

      private long lonOffset_;

      /**
       * <code>optional int64 lon_offset = 20 [default = 0];</code>
       *
       * @return Whether the lonOffset field is set.
       */
      @Override
      public boolean hasLonOffset() {
        return ((bitField0_ & 0x00000010) != 0);
      }

      /**
       * <code>optional int64 lon_offset = 20 [default = 0];</code>
       *
       * @return The lonOffset.
       */
      @Override
      public long getLonOffset() {
        return lonOffset_;
      }

      /**
       * <code>optional int64 lon_offset = 20 [default = 0];</code>
       *
       * @param value The lonOffset to set.
       * @return This builder for chaining.
       */
      public Builder setLonOffset(long value) {

        lonOffset_ = value;
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       * <code>optional int64 lon_offset = 20 [default = 0];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearLonOffset() {
        bitField0_ = (bitField0_ & ~0x00000010);
        lonOffset_ = 0L;
        onChanged();
        return this;
      }

      private int dateGranularity_ = 1000;

      /**
       *
       *
       * <pre>
       * Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
       * </pre>
       *
       * <code>optional int32 date_granularity = 18 [default = 1000];</code>
       *
       * @return Whether the dateGranularity field is set.
       */
      @Override
      public boolean hasDateGranularity() {
        return ((bitField0_ & 0x00000020) != 0);
      }

      /**
       *
       *
       * <pre>
       * Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
       * </pre>
       *
       * <code>optional int32 date_granularity = 18 [default = 1000];</code>
       *
       * @return The dateGranularity.
       */
      @Override
      public int getDateGranularity() {
        return dateGranularity_;
      }

      /**
       *
       *
       * <pre>
       * Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
       * </pre>
       *
       * <code>optional int32 date_granularity = 18 [default = 1000];</code>
       *
       * @param value The dateGranularity to set.
       * @return This builder for chaining.
       */
      public Builder setDateGranularity(int value) {

        dateGranularity_ = value;
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Granularity of dates, normally represented in units of milliseconds since the 1970 epoch.
       * </pre>
       *
       * <code>optional int32 date_granularity = 18 [default = 1000];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearDateGranularity() {
        bitField0_ = (bitField0_ & ~0x00000020);
        dateGranularity_ = 1000;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:PrimitiveBlock)
    }

    // @@protoc_insertion_point(class_scope:PrimitiveBlock)
    private static final Osmformat.PrimitiveBlock DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.PrimitiveBlock();
    }

    public static Osmformat.PrimitiveBlock getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<PrimitiveBlock> PARSER =
        new proto4.AbstractParser<PrimitiveBlock>() {
          @Override
          public PrimitiveBlock parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<PrimitiveBlock> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<PrimitiveBlock> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.PrimitiveBlock getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface PrimitiveGroupOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:PrimitiveGroup)
      proto4.MessageOrBuilder {

    /** <code>repeated .Node nodes = 1;</code> */
    java.util.List<Osmformat.Node> getNodesList();

    /** <code>repeated .Node nodes = 1;</code> */
    Osmformat.Node getNodes(int index);

    /** <code>repeated .Node nodes = 1;</code> */
    int getNodesCount();

    /** <code>repeated .Node nodes = 1;</code> */
    java.util.List<? extends Osmformat.NodeOrBuilder> getNodesOrBuilderList();

    /** <code>repeated .Node nodes = 1;</code> */
    Osmformat.NodeOrBuilder getNodesOrBuilder(int index);

    /**
     * <code>optional .DenseNodes dense = 2;</code>
     *
     * @return Whether the dense field is set.
     */
    boolean hasDense();

    /**
     * <code>optional .DenseNodes dense = 2;</code>
     *
     * @return The dense.
     */
    Osmformat.DenseNodes getDense();

    /** <code>optional .DenseNodes dense = 2;</code> */
    Osmformat.DenseNodesOrBuilder getDenseOrBuilder();

    /** <code>repeated .Way ways = 3;</code> */
    java.util.List<Osmformat.Way> getWaysList();

    /** <code>repeated .Way ways = 3;</code> */
    Osmformat.Way getWays(int index);

    /** <code>repeated .Way ways = 3;</code> */
    int getWaysCount();

    /** <code>repeated .Way ways = 3;</code> */
    java.util.List<? extends Osmformat.WayOrBuilder> getWaysOrBuilderList();

    /** <code>repeated .Way ways = 3;</code> */
    Osmformat.WayOrBuilder getWaysOrBuilder(int index);

    /** <code>repeated .Relation relations = 4;</code> */
    java.util.List<Osmformat.Relation> getRelationsList();

    /** <code>repeated .Relation relations = 4;</code> */
    Osmformat.Relation getRelations(int index);

    /** <code>repeated .Relation relations = 4;</code> */
    int getRelationsCount();

    /** <code>repeated .Relation relations = 4;</code> */
    java.util.List<? extends Osmformat.RelationOrBuilder> getRelationsOrBuilderList();

    /** <code>repeated .Relation relations = 4;</code> */
    Osmformat.RelationOrBuilder getRelationsOrBuilder(int index);

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    java.util.List<Osmformat.ChangeSet> getChangesetsList();

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    Osmformat.ChangeSet getChangesets(int index);

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    int getChangesetsCount();

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    java.util.List<? extends Osmformat.ChangeSetOrBuilder> getChangesetsOrBuilderList();

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    Osmformat.ChangeSetOrBuilder getChangesetsOrBuilder(int index);
  }

  /**
   *
   *
   * <pre>
   * Group of OSMPrimitives. All primitives in a group must be the same type.
   * </pre>
   *
   * <p>Protobuf type {@code PrimitiveGroup}
   */
  public static final class PrimitiveGroup extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:PrimitiveGroup)
      PrimitiveGroupOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          PrimitiveGroup.class.getName());
    }

    // Use PrimitiveGroup.newBuilder() to construct.
    private PrimitiveGroup(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private PrimitiveGroup() {
      nodes_ = java.util.Collections.emptyList();
      ways_ = java.util.Collections.emptyList();
      relations_ = java.util.Collections.emptyList();
      changesets_ = java.util.Collections.emptyList();
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_PrimitiveGroup_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_PrimitiveGroup_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.PrimitiveGroup.class, Osmformat.PrimitiveGroup.Builder.class);
    }

    private int bitField0_;
    public static final int NODES_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private java.util.List<Osmformat.Node> nodes_;

    /** <code>repeated .Node nodes = 1;</code> */
    @Override
    public java.util.List<Osmformat.Node> getNodesList() {
      return nodes_;
    }

    /** <code>repeated .Node nodes = 1;</code> */
    @Override
    public java.util.List<? extends Osmformat.NodeOrBuilder> getNodesOrBuilderList() {
      return nodes_;
    }

    /** <code>repeated .Node nodes = 1;</code> */
    @Override
    public int getNodesCount() {
      return nodes_.size();
    }

    /** <code>repeated .Node nodes = 1;</code> */
    @Override
    public Osmformat.Node getNodes(int index) {
      return nodes_.get(index);
    }

    /** <code>repeated .Node nodes = 1;</code> */
    @Override
    public Osmformat.NodeOrBuilder getNodesOrBuilder(int index) {
      return nodes_.get(index);
    }

    public static final int DENSE_FIELD_NUMBER = 2;
    private Osmformat.DenseNodes dense_;

    /**
     * <code>optional .DenseNodes dense = 2;</code>
     *
     * @return Whether the dense field is set.
     */
    @Override
    public boolean hasDense() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     * <code>optional .DenseNodes dense = 2;</code>
     *
     * @return The dense.
     */
    @Override
    public Osmformat.DenseNodes getDense() {
      return dense_ == null ? Osmformat.DenseNodes.getDefaultInstance() : dense_;
    }

    /** <code>optional .DenseNodes dense = 2;</code> */
    @Override
    public Osmformat.DenseNodesOrBuilder getDenseOrBuilder() {
      return dense_ == null ? Osmformat.DenseNodes.getDefaultInstance() : dense_;
    }

    public static final int WAYS_FIELD_NUMBER = 3;

    @SuppressWarnings("serial")
    private java.util.List<Osmformat.Way> ways_;

    /** <code>repeated .Way ways = 3;</code> */
    @Override
    public java.util.List<Osmformat.Way> getWaysList() {
      return ways_;
    }

    /** <code>repeated .Way ways = 3;</code> */
    @Override
    public java.util.List<? extends Osmformat.WayOrBuilder> getWaysOrBuilderList() {
      return ways_;
    }

    /** <code>repeated .Way ways = 3;</code> */
    @Override
    public int getWaysCount() {
      return ways_.size();
    }

    /** <code>repeated .Way ways = 3;</code> */
    @Override
    public Osmformat.Way getWays(int index) {
      return ways_.get(index);
    }

    /** <code>repeated .Way ways = 3;</code> */
    @Override
    public Osmformat.WayOrBuilder getWaysOrBuilder(int index) {
      return ways_.get(index);
    }

    public static final int RELATIONS_FIELD_NUMBER = 4;

    @SuppressWarnings("serial")
    private java.util.List<Osmformat.Relation> relations_;

    /** <code>repeated .Relation relations = 4;</code> */
    @Override
    public java.util.List<Osmformat.Relation> getRelationsList() {
      return relations_;
    }

    /** <code>repeated .Relation relations = 4;</code> */
    @Override
    public java.util.List<? extends Osmformat.RelationOrBuilder> getRelationsOrBuilderList() {
      return relations_;
    }

    /** <code>repeated .Relation relations = 4;</code> */
    @Override
    public int getRelationsCount() {
      return relations_.size();
    }

    /** <code>repeated .Relation relations = 4;</code> */
    @Override
    public Osmformat.Relation getRelations(int index) {
      return relations_.get(index);
    }

    /** <code>repeated .Relation relations = 4;</code> */
    @Override
    public Osmformat.RelationOrBuilder getRelationsOrBuilder(int index) {
      return relations_.get(index);
    }

    public static final int CHANGESETS_FIELD_NUMBER = 5;

    @SuppressWarnings("serial")
    private java.util.List<Osmformat.ChangeSet> changesets_;

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    @Override
    public java.util.List<Osmformat.ChangeSet> getChangesetsList() {
      return changesets_;
    }

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    @Override
    public java.util.List<? extends Osmformat.ChangeSetOrBuilder> getChangesetsOrBuilderList() {
      return changesets_;
    }

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    @Override
    public int getChangesetsCount() {
      return changesets_.size();
    }

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    @Override
    public Osmformat.ChangeSet getChangesets(int index) {
      return changesets_.get(index);
    }

    /** <code>repeated .ChangeSet changesets = 5;</code> */
    @Override
    public Osmformat.ChangeSetOrBuilder getChangesetsOrBuilder(int index) {
      return changesets_.get(index);
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      for (int i = 0; i < getNodesCount(); i++) {
        if (!getNodes(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      for (int i = 0; i < getWaysCount(); i++) {
        if (!getWays(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      for (int i = 0; i < getRelationsCount(); i++) {
        if (!getRelations(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      for (int i = 0; i < getChangesetsCount(); i++) {
        if (!getChangesets(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      for (int i = 0; i < nodes_.size(); i++) {
        output.writeMessage(1, nodes_.get(i));
      }
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(2, getDense());
      }
      for (int i = 0; i < ways_.size(); i++) {
        output.writeMessage(3, ways_.get(i));
      }
      for (int i = 0; i < relations_.size(); i++) {
        output.writeMessage(4, relations_.get(i));
      }
      for (int i = 0; i < changesets_.size(); i++) {
        output.writeMessage(5, changesets_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      for (int i = 0; i < nodes_.size(); i++) {
        size += proto4.CodedOutputStream.computeMessageSize(1, nodes_.get(i));
      }
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeMessageSize(2, getDense());
      }
      for (int i = 0; i < ways_.size(); i++) {
        size += proto4.CodedOutputStream.computeMessageSize(3, ways_.get(i));
      }
      for (int i = 0; i < relations_.size(); i++) {
        size += proto4.CodedOutputStream.computeMessageSize(4, relations_.get(i));
      }
      for (int i = 0; i < changesets_.size(); i++) {
        size += proto4.CodedOutputStream.computeMessageSize(5, changesets_.get(i));
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.PrimitiveGroup)) {
        return super.equals(obj);
      }
      Osmformat.PrimitiveGroup other = (Osmformat.PrimitiveGroup) obj;

      if (!getNodesList().equals(other.getNodesList())) return false;
      if (hasDense() != other.hasDense()) return false;
      if (hasDense()) {
        if (!getDense().equals(other.getDense())) return false;
      }
      if (!getWaysList().equals(other.getWaysList())) return false;
      if (!getRelationsList().equals(other.getRelationsList())) return false;
      if (!getChangesetsList().equals(other.getChangesetsList())) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (getNodesCount() > 0) {
        hash = (37 * hash) + NODES_FIELD_NUMBER;
        hash = (53 * hash) + getNodesList().hashCode();
      }
      if (hasDense()) {
        hash = (37 * hash) + DENSE_FIELD_NUMBER;
        hash = (53 * hash) + getDense().hashCode();
      }
      if (getWaysCount() > 0) {
        hash = (37 * hash) + WAYS_FIELD_NUMBER;
        hash = (53 * hash) + getWaysList().hashCode();
      }
      if (getRelationsCount() > 0) {
        hash = (37 * hash) + RELATIONS_FIELD_NUMBER;
        hash = (53 * hash) + getRelationsList().hashCode();
      }
      if (getChangesetsCount() > 0) {
        hash = (37 * hash) + CHANGESETS_FIELD_NUMBER;
        hash = (53 * hash) + getChangesetsList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.PrimitiveGroup parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.PrimitiveGroup parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.PrimitiveGroup parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.PrimitiveGroup parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.PrimitiveGroup parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.PrimitiveGroup parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.PrimitiveGroup parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.PrimitiveGroup parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.PrimitiveGroup parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.PrimitiveGroup parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.PrimitiveGroup parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.PrimitiveGroup parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.PrimitiveGroup prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /**
     *
     *
     * <pre>
     * Group of OSMPrimitives. All primitives in a group must be the same type.
     * </pre>
     *
     * <p>Protobuf type {@code PrimitiveGroup}
     */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:PrimitiveGroup)
        Osmformat.PrimitiveGroupOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_PrimitiveGroup_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat
            .internal_static_org_apache_sedona_osm_build_PrimitiveGroup_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.PrimitiveGroup.class, Osmformat.PrimitiveGroup.Builder.class);
      }

      // Construct using Osmformat.PrimitiveGroup.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }

      private void maybeForceBuilderInitialization() {
        if (proto4.GeneratedMessage.alwaysUseFieldBuilders) {
          getNodesFieldBuilder();
          getDenseFieldBuilder();
          getWaysFieldBuilder();
          getRelationsFieldBuilder();
          getChangesetsFieldBuilder();
        }
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        if (nodesBuilder_ == null) {
          nodes_ = java.util.Collections.emptyList();
        } else {
          nodes_ = null;
          nodesBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        dense_ = null;
        if (denseBuilder_ != null) {
          denseBuilder_.dispose();
          denseBuilder_ = null;
        }
        if (waysBuilder_ == null) {
          ways_ = java.util.Collections.emptyList();
        } else {
          ways_ = null;
          waysBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        if (relationsBuilder_ == null) {
          relations_ = java.util.Collections.emptyList();
        } else {
          relations_ = null;
          relationsBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000008);
        if (changesetsBuilder_ == null) {
          changesets_ = java.util.Collections.emptyList();
        } else {
          changesets_ = null;
          changesetsBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000010);
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_PrimitiveGroup_descriptor;
      }

      @Override
      public Osmformat.PrimitiveGroup getDefaultInstanceForType() {
        return Osmformat.PrimitiveGroup.getDefaultInstance();
      }

      @Override
      public Osmformat.PrimitiveGroup build() {
        Osmformat.PrimitiveGroup result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.PrimitiveGroup buildPartial() {
        Osmformat.PrimitiveGroup result = new Osmformat.PrimitiveGroup(this);
        buildPartialRepeatedFields(result);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartialRepeatedFields(Osmformat.PrimitiveGroup result) {
        if (nodesBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)) {
            nodes_ = java.util.Collections.unmodifiableList(nodes_);
            bitField0_ = (bitField0_ & ~0x00000001);
          }
          result.nodes_ = nodes_;
        } else {
          result.nodes_ = nodesBuilder_.build();
        }
        if (waysBuilder_ == null) {
          if (((bitField0_ & 0x00000004) != 0)) {
            ways_ = java.util.Collections.unmodifiableList(ways_);
            bitField0_ = (bitField0_ & ~0x00000004);
          }
          result.ways_ = ways_;
        } else {
          result.ways_ = waysBuilder_.build();
        }
        if (relationsBuilder_ == null) {
          if (((bitField0_ & 0x00000008) != 0)) {
            relations_ = java.util.Collections.unmodifiableList(relations_);
            bitField0_ = (bitField0_ & ~0x00000008);
          }
          result.relations_ = relations_;
        } else {
          result.relations_ = relationsBuilder_.build();
        }
        if (changesetsBuilder_ == null) {
          if (((bitField0_ & 0x00000010) != 0)) {
            changesets_ = java.util.Collections.unmodifiableList(changesets_);
            bitField0_ = (bitField0_ & ~0x00000010);
          }
          result.changesets_ = changesets_;
        } else {
          result.changesets_ = changesetsBuilder_.build();
        }
      }

      private void buildPartial0(Osmformat.PrimitiveGroup result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.dense_ = denseBuilder_ == null ? dense_ : denseBuilder_.build();
          to_bitField0_ |= 0x00000001;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.PrimitiveGroup) {
          return mergeFrom((Osmformat.PrimitiveGroup) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.PrimitiveGroup other) {
        if (other == Osmformat.PrimitiveGroup.getDefaultInstance()) return this;
        if (nodesBuilder_ == null) {
          if (!other.nodes_.isEmpty()) {
            if (nodes_.isEmpty()) {
              nodes_ = other.nodes_;
              bitField0_ = (bitField0_ & ~0x00000001);
            } else {
              ensureNodesIsMutable();
              nodes_.addAll(other.nodes_);
            }
            onChanged();
          }
        } else {
          if (!other.nodes_.isEmpty()) {
            if (nodesBuilder_.isEmpty()) {
              nodesBuilder_.dispose();
              nodesBuilder_ = null;
              nodes_ = other.nodes_;
              bitField0_ = (bitField0_ & ~0x00000001);
              nodesBuilder_ =
                  proto4.GeneratedMessage.alwaysUseFieldBuilders ? getNodesFieldBuilder() : null;
            } else {
              nodesBuilder_.addAllMessages(other.nodes_);
            }
          }
        }
        if (other.hasDense()) {
          mergeDense(other.getDense());
        }
        if (waysBuilder_ == null) {
          if (!other.ways_.isEmpty()) {
            if (ways_.isEmpty()) {
              ways_ = other.ways_;
              bitField0_ = (bitField0_ & ~0x00000004);
            } else {
              ensureWaysIsMutable();
              ways_.addAll(other.ways_);
            }
            onChanged();
          }
        } else {
          if (!other.ways_.isEmpty()) {
            if (waysBuilder_.isEmpty()) {
              waysBuilder_.dispose();
              waysBuilder_ = null;
              ways_ = other.ways_;
              bitField0_ = (bitField0_ & ~0x00000004);
              waysBuilder_ =
                  proto4.GeneratedMessage.alwaysUseFieldBuilders ? getWaysFieldBuilder() : null;
            } else {
              waysBuilder_.addAllMessages(other.ways_);
            }
          }
        }
        if (relationsBuilder_ == null) {
          if (!other.relations_.isEmpty()) {
            if (relations_.isEmpty()) {
              relations_ = other.relations_;
              bitField0_ = (bitField0_ & ~0x00000008);
            } else {
              ensureRelationsIsMutable();
              relations_.addAll(other.relations_);
            }
            onChanged();
          }
        } else {
          if (!other.relations_.isEmpty()) {
            if (relationsBuilder_.isEmpty()) {
              relationsBuilder_.dispose();
              relationsBuilder_ = null;
              relations_ = other.relations_;
              bitField0_ = (bitField0_ & ~0x00000008);
              relationsBuilder_ =
                  proto4.GeneratedMessage.alwaysUseFieldBuilders
                      ? getRelationsFieldBuilder()
                      : null;
            } else {
              relationsBuilder_.addAllMessages(other.relations_);
            }
          }
        }
        if (changesetsBuilder_ == null) {
          if (!other.changesets_.isEmpty()) {
            if (changesets_.isEmpty()) {
              changesets_ = other.changesets_;
              bitField0_ = (bitField0_ & ~0x00000010);
            } else {
              ensureChangesetsIsMutable();
              changesets_.addAll(other.changesets_);
            }
            onChanged();
          }
        } else {
          if (!other.changesets_.isEmpty()) {
            if (changesetsBuilder_.isEmpty()) {
              changesetsBuilder_.dispose();
              changesetsBuilder_ = null;
              changesets_ = other.changesets_;
              bitField0_ = (bitField0_ & ~0x00000010);
              changesetsBuilder_ =
                  proto4.GeneratedMessage.alwaysUseFieldBuilders
                      ? getChangesetsFieldBuilder()
                      : null;
            } else {
              changesetsBuilder_.addAllMessages(other.changesets_);
            }
          }
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        for (int i = 0; i < getNodesCount(); i++) {
          if (!getNodes(i).isInitialized()) {
            return false;
          }
        }
        for (int i = 0; i < getWaysCount(); i++) {
          if (!getWays(i).isInitialized()) {
            return false;
          }
        }
        for (int i = 0; i < getRelationsCount(); i++) {
          if (!getRelations(i).isInitialized()) {
            return false;
          }
        }
        for (int i = 0; i < getChangesetsCount(); i++) {
          if (!getChangesets(i).isInitialized()) {
            return false;
          }
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 10:
                {
                  Osmformat.Node m = input.readMessage(Osmformat.Node.parser(), extensionRegistry);
                  if (nodesBuilder_ == null) {
                    ensureNodesIsMutable();
                    nodes_.add(m);
                  } else {
                    nodesBuilder_.addMessage(m);
                  }
                  break;
                } // case 10
              case 18:
                {
                  input.readMessage(getDenseFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000002;
                  break;
                } // case 18
              case 26:
                {
                  Osmformat.Way m = input.readMessage(Osmformat.Way.parser(), extensionRegistry);
                  if (waysBuilder_ == null) {
                    ensureWaysIsMutable();
                    ways_.add(m);
                  } else {
                    waysBuilder_.addMessage(m);
                  }
                  break;
                } // case 26
              case 34:
                {
                  Osmformat.Relation m =
                      input.readMessage(Osmformat.Relation.parser(), extensionRegistry);
                  if (relationsBuilder_ == null) {
                    ensureRelationsIsMutable();
                    relations_.add(m);
                  } else {
                    relationsBuilder_.addMessage(m);
                  }
                  break;
                } // case 34
              case 42:
                {
                  Osmformat.ChangeSet m =
                      input.readMessage(Osmformat.ChangeSet.parser(), extensionRegistry);
                  if (changesetsBuilder_ == null) {
                    ensureChangesetsIsMutable();
                    changesets_.add(m);
                  } else {
                    changesetsBuilder_.addMessage(m);
                  }
                  break;
                } // case 42
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private java.util.List<Osmformat.Node> nodes_ = java.util.Collections.emptyList();

      private void ensureNodesIsMutable() {
        if (!((bitField0_ & 0x00000001) != 0)) {
          nodes_ = new java.util.ArrayList<Osmformat.Node>(nodes_);
          bitField0_ |= 0x00000001;
        }
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.Node, Osmformat.Node.Builder, Osmformat.NodeOrBuilder>
          nodesBuilder_;

      /** <code>repeated .Node nodes = 1;</code> */
      public java.util.List<Osmformat.Node> getNodesList() {
        if (nodesBuilder_ == null) {
          return java.util.Collections.unmodifiableList(nodes_);
        } else {
          return nodesBuilder_.getMessageList();
        }
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public int getNodesCount() {
        if (nodesBuilder_ == null) {
          return nodes_.size();
        } else {
          return nodesBuilder_.getCount();
        }
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Osmformat.Node getNodes(int index) {
        if (nodesBuilder_ == null) {
          return nodes_.get(index);
        } else {
          return nodesBuilder_.getMessage(index);
        }
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder setNodes(int index, Osmformat.Node value) {
        if (nodesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureNodesIsMutable();
          nodes_.set(index, value);
          onChanged();
        } else {
          nodesBuilder_.setMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder setNodes(int index, Osmformat.Node.Builder builderForValue) {
        if (nodesBuilder_ == null) {
          ensureNodesIsMutable();
          nodes_.set(index, builderForValue.build());
          onChanged();
        } else {
          nodesBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder addNodes(Osmformat.Node value) {
        if (nodesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureNodesIsMutable();
          nodes_.add(value);
          onChanged();
        } else {
          nodesBuilder_.addMessage(value);
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder addNodes(int index, Osmformat.Node value) {
        if (nodesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureNodesIsMutable();
          nodes_.add(index, value);
          onChanged();
        } else {
          nodesBuilder_.addMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder addNodes(Osmformat.Node.Builder builderForValue) {
        if (nodesBuilder_ == null) {
          ensureNodesIsMutable();
          nodes_.add(builderForValue.build());
          onChanged();
        } else {
          nodesBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder addNodes(int index, Osmformat.Node.Builder builderForValue) {
        if (nodesBuilder_ == null) {
          ensureNodesIsMutable();
          nodes_.add(index, builderForValue.build());
          onChanged();
        } else {
          nodesBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder addAllNodes(Iterable<? extends Osmformat.Node> values) {
        if (nodesBuilder_ == null) {
          ensureNodesIsMutable();
          proto4.AbstractMessageLite.Builder.addAll(values, nodes_);
          onChanged();
        } else {
          nodesBuilder_.addAllMessages(values);
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder clearNodes() {
        if (nodesBuilder_ == null) {
          nodes_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
          onChanged();
        } else {
          nodesBuilder_.clear();
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Builder removeNodes(int index) {
        if (nodesBuilder_ == null) {
          ensureNodesIsMutable();
          nodes_.remove(index);
          onChanged();
        } else {
          nodesBuilder_.remove(index);
        }
        return this;
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Osmformat.Node.Builder getNodesBuilder(int index) {
        return getNodesFieldBuilder().getBuilder(index);
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Osmformat.NodeOrBuilder getNodesOrBuilder(int index) {
        if (nodesBuilder_ == null) {
          return nodes_.get(index);
        } else {
          return nodesBuilder_.getMessageOrBuilder(index);
        }
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public java.util.List<? extends Osmformat.NodeOrBuilder> getNodesOrBuilderList() {
        if (nodesBuilder_ != null) {
          return nodesBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(nodes_);
        }
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Osmformat.Node.Builder addNodesBuilder() {
        return getNodesFieldBuilder().addBuilder(Osmformat.Node.getDefaultInstance());
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public Osmformat.Node.Builder addNodesBuilder(int index) {
        return getNodesFieldBuilder().addBuilder(index, Osmformat.Node.getDefaultInstance());
      }

      /** <code>repeated .Node nodes = 1;</code> */
      public java.util.List<Osmformat.Node.Builder> getNodesBuilderList() {
        return getNodesFieldBuilder().getBuilderList();
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.Node, Osmformat.Node.Builder, Osmformat.NodeOrBuilder>
          getNodesFieldBuilder() {
        if (nodesBuilder_ == null) {
          nodesBuilder_ =
              new proto4.RepeatedFieldBuilder<
                  Osmformat.Node, Osmformat.Node.Builder, Osmformat.NodeOrBuilder>(
                  nodes_, ((bitField0_ & 0x00000001) != 0), getParentForChildren(), isClean());
          nodes_ = null;
        }
        return nodesBuilder_;
      }

      private Osmformat.DenseNodes dense_;
      private proto4.SingleFieldBuilder<
              Osmformat.DenseNodes, Osmformat.DenseNodes.Builder, Osmformat.DenseNodesOrBuilder>
          denseBuilder_;

      /**
       * <code>optional .DenseNodes dense = 2;</code>
       *
       * @return Whether the dense field is set.
       */
      public boolean hasDense() {
        return ((bitField0_ & 0x00000002) != 0);
      }

      /**
       * <code>optional .DenseNodes dense = 2;</code>
       *
       * @return The dense.
       */
      public Osmformat.DenseNodes getDense() {
        if (denseBuilder_ == null) {
          return dense_ == null ? Osmformat.DenseNodes.getDefaultInstance() : dense_;
        } else {
          return denseBuilder_.getMessage();
        }
      }

      /** <code>optional .DenseNodes dense = 2;</code> */
      public Builder setDense(Osmformat.DenseNodes value) {
        if (denseBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          dense_ = value;
        } else {
          denseBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /** <code>optional .DenseNodes dense = 2;</code> */
      public Builder setDense(Osmformat.DenseNodes.Builder builderForValue) {
        if (denseBuilder_ == null) {
          dense_ = builderForValue.build();
        } else {
          denseBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /** <code>optional .DenseNodes dense = 2;</code> */
      public Builder mergeDense(Osmformat.DenseNodes value) {
        if (denseBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0)
              && dense_ != null
              && dense_ != Osmformat.DenseNodes.getDefaultInstance()) {
            getDenseBuilder().mergeFrom(value);
          } else {
            dense_ = value;
          }
        } else {
          denseBuilder_.mergeFrom(value);
        }
        if (dense_ != null) {
          bitField0_ |= 0x00000002;
          onChanged();
        }
        return this;
      }

      /** <code>optional .DenseNodes dense = 2;</code> */
      public Builder clearDense() {
        bitField0_ = (bitField0_ & ~0x00000002);
        dense_ = null;
        if (denseBuilder_ != null) {
          denseBuilder_.dispose();
          denseBuilder_ = null;
        }
        onChanged();
        return this;
      }

      /** <code>optional .DenseNodes dense = 2;</code> */
      public Osmformat.DenseNodes.Builder getDenseBuilder() {
        bitField0_ |= 0x00000002;
        onChanged();
        return getDenseFieldBuilder().getBuilder();
      }

      /** <code>optional .DenseNodes dense = 2;</code> */
      public Osmformat.DenseNodesOrBuilder getDenseOrBuilder() {
        if (denseBuilder_ != null) {
          return denseBuilder_.getMessageOrBuilder();
        } else {
          return dense_ == null ? Osmformat.DenseNodes.getDefaultInstance() : dense_;
        }
      }

      /** <code>optional .DenseNodes dense = 2;</code> */
      private proto4.SingleFieldBuilder<
              Osmformat.DenseNodes, Osmformat.DenseNodes.Builder, Osmformat.DenseNodesOrBuilder>
          getDenseFieldBuilder() {
        if (denseBuilder_ == null) {
          denseBuilder_ =
              new proto4.SingleFieldBuilder<
                  Osmformat.DenseNodes,
                  Osmformat.DenseNodes.Builder,
                  Osmformat.DenseNodesOrBuilder>(getDense(), getParentForChildren(), isClean());
          dense_ = null;
        }
        return denseBuilder_;
      }

      private java.util.List<Osmformat.Way> ways_ = java.util.Collections.emptyList();

      private void ensureWaysIsMutable() {
        if (!((bitField0_ & 0x00000004) != 0)) {
          ways_ = new java.util.ArrayList<Osmformat.Way>(ways_);
          bitField0_ |= 0x00000004;
        }
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.Way, Osmformat.Way.Builder, Osmformat.WayOrBuilder>
          waysBuilder_;

      /** <code>repeated .Way ways = 3;</code> */
      public java.util.List<Osmformat.Way> getWaysList() {
        if (waysBuilder_ == null) {
          return java.util.Collections.unmodifiableList(ways_);
        } else {
          return waysBuilder_.getMessageList();
        }
      }

      /** <code>repeated .Way ways = 3;</code> */
      public int getWaysCount() {
        if (waysBuilder_ == null) {
          return ways_.size();
        } else {
          return waysBuilder_.getCount();
        }
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Osmformat.Way getWays(int index) {
        if (waysBuilder_ == null) {
          return ways_.get(index);
        } else {
          return waysBuilder_.getMessage(index);
        }
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder setWays(int index, Osmformat.Way value) {
        if (waysBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureWaysIsMutable();
          ways_.set(index, value);
          onChanged();
        } else {
          waysBuilder_.setMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder setWays(int index, Osmformat.Way.Builder builderForValue) {
        if (waysBuilder_ == null) {
          ensureWaysIsMutable();
          ways_.set(index, builderForValue.build());
          onChanged();
        } else {
          waysBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder addWays(Osmformat.Way value) {
        if (waysBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureWaysIsMutable();
          ways_.add(value);
          onChanged();
        } else {
          waysBuilder_.addMessage(value);
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder addWays(int index, Osmformat.Way value) {
        if (waysBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureWaysIsMutable();
          ways_.add(index, value);
          onChanged();
        } else {
          waysBuilder_.addMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder addWays(Osmformat.Way.Builder builderForValue) {
        if (waysBuilder_ == null) {
          ensureWaysIsMutable();
          ways_.add(builderForValue.build());
          onChanged();
        } else {
          waysBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder addWays(int index, Osmformat.Way.Builder builderForValue) {
        if (waysBuilder_ == null) {
          ensureWaysIsMutable();
          ways_.add(index, builderForValue.build());
          onChanged();
        } else {
          waysBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder addAllWays(Iterable<? extends Osmformat.Way> values) {
        if (waysBuilder_ == null) {
          ensureWaysIsMutable();
          proto4.AbstractMessageLite.Builder.addAll(values, ways_);
          onChanged();
        } else {
          waysBuilder_.addAllMessages(values);
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder clearWays() {
        if (waysBuilder_ == null) {
          ways_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000004);
          onChanged();
        } else {
          waysBuilder_.clear();
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Builder removeWays(int index) {
        if (waysBuilder_ == null) {
          ensureWaysIsMutable();
          ways_.remove(index);
          onChanged();
        } else {
          waysBuilder_.remove(index);
        }
        return this;
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Osmformat.Way.Builder getWaysBuilder(int index) {
        return getWaysFieldBuilder().getBuilder(index);
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Osmformat.WayOrBuilder getWaysOrBuilder(int index) {
        if (waysBuilder_ == null) {
          return ways_.get(index);
        } else {
          return waysBuilder_.getMessageOrBuilder(index);
        }
      }

      /** <code>repeated .Way ways = 3;</code> */
      public java.util.List<? extends Osmformat.WayOrBuilder> getWaysOrBuilderList() {
        if (waysBuilder_ != null) {
          return waysBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(ways_);
        }
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Osmformat.Way.Builder addWaysBuilder() {
        return getWaysFieldBuilder().addBuilder(Osmformat.Way.getDefaultInstance());
      }

      /** <code>repeated .Way ways = 3;</code> */
      public Osmformat.Way.Builder addWaysBuilder(int index) {
        return getWaysFieldBuilder().addBuilder(index, Osmformat.Way.getDefaultInstance());
      }

      /** <code>repeated .Way ways = 3;</code> */
      public java.util.List<Osmformat.Way.Builder> getWaysBuilderList() {
        return getWaysFieldBuilder().getBuilderList();
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.Way, Osmformat.Way.Builder, Osmformat.WayOrBuilder>
          getWaysFieldBuilder() {
        if (waysBuilder_ == null) {
          waysBuilder_ =
              new proto4.RepeatedFieldBuilder<
                  Osmformat.Way, Osmformat.Way.Builder, Osmformat.WayOrBuilder>(
                  ways_, ((bitField0_ & 0x00000004) != 0), getParentForChildren(), isClean());
          ways_ = null;
        }
        return waysBuilder_;
      }

      private java.util.List<Osmformat.Relation> relations_ = java.util.Collections.emptyList();

      private void ensureRelationsIsMutable() {
        if (!((bitField0_ & 0x00000008) != 0)) {
          relations_ = new java.util.ArrayList<Osmformat.Relation>(relations_);
          bitField0_ |= 0x00000008;
        }
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.Relation, Osmformat.Relation.Builder, Osmformat.RelationOrBuilder>
          relationsBuilder_;

      /** <code>repeated .Relation relations = 4;</code> */
      public java.util.List<Osmformat.Relation> getRelationsList() {
        if (relationsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(relations_);
        } else {
          return relationsBuilder_.getMessageList();
        }
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public int getRelationsCount() {
        if (relationsBuilder_ == null) {
          return relations_.size();
        } else {
          return relationsBuilder_.getCount();
        }
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Osmformat.Relation getRelations(int index) {
        if (relationsBuilder_ == null) {
          return relations_.get(index);
        } else {
          return relationsBuilder_.getMessage(index);
        }
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder setRelations(int index, Osmformat.Relation value) {
        if (relationsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureRelationsIsMutable();
          relations_.set(index, value);
          onChanged();
        } else {
          relationsBuilder_.setMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder setRelations(int index, Osmformat.Relation.Builder builderForValue) {
        if (relationsBuilder_ == null) {
          ensureRelationsIsMutable();
          relations_.set(index, builderForValue.build());
          onChanged();
        } else {
          relationsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder addRelations(Osmformat.Relation value) {
        if (relationsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureRelationsIsMutable();
          relations_.add(value);
          onChanged();
        } else {
          relationsBuilder_.addMessage(value);
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder addRelations(int index, Osmformat.Relation value) {
        if (relationsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureRelationsIsMutable();
          relations_.add(index, value);
          onChanged();
        } else {
          relationsBuilder_.addMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder addRelations(Osmformat.Relation.Builder builderForValue) {
        if (relationsBuilder_ == null) {
          ensureRelationsIsMutable();
          relations_.add(builderForValue.build());
          onChanged();
        } else {
          relationsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder addRelations(int index, Osmformat.Relation.Builder builderForValue) {
        if (relationsBuilder_ == null) {
          ensureRelationsIsMutable();
          relations_.add(index, builderForValue.build());
          onChanged();
        } else {
          relationsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder addAllRelations(Iterable<? extends Osmformat.Relation> values) {
        if (relationsBuilder_ == null) {
          ensureRelationsIsMutable();
          proto4.AbstractMessageLite.Builder.addAll(values, relations_);
          onChanged();
        } else {
          relationsBuilder_.addAllMessages(values);
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder clearRelations() {
        if (relationsBuilder_ == null) {
          relations_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000008);
          onChanged();
        } else {
          relationsBuilder_.clear();
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Builder removeRelations(int index) {
        if (relationsBuilder_ == null) {
          ensureRelationsIsMutable();
          relations_.remove(index);
          onChanged();
        } else {
          relationsBuilder_.remove(index);
        }
        return this;
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Osmformat.Relation.Builder getRelationsBuilder(int index) {
        return getRelationsFieldBuilder().getBuilder(index);
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Osmformat.RelationOrBuilder getRelationsOrBuilder(int index) {
        if (relationsBuilder_ == null) {
          return relations_.get(index);
        } else {
          return relationsBuilder_.getMessageOrBuilder(index);
        }
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public java.util.List<? extends Osmformat.RelationOrBuilder> getRelationsOrBuilderList() {
        if (relationsBuilder_ != null) {
          return relationsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(relations_);
        }
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Osmformat.Relation.Builder addRelationsBuilder() {
        return getRelationsFieldBuilder().addBuilder(Osmformat.Relation.getDefaultInstance());
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public Osmformat.Relation.Builder addRelationsBuilder(int index) {
        return getRelationsFieldBuilder()
            .addBuilder(index, Osmformat.Relation.getDefaultInstance());
      }

      /** <code>repeated .Relation relations = 4;</code> */
      public java.util.List<Osmformat.Relation.Builder> getRelationsBuilderList() {
        return getRelationsFieldBuilder().getBuilderList();
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.Relation, Osmformat.Relation.Builder, Osmformat.RelationOrBuilder>
          getRelationsFieldBuilder() {
        if (relationsBuilder_ == null) {
          relationsBuilder_ =
              new proto4.RepeatedFieldBuilder<
                  Osmformat.Relation, Osmformat.Relation.Builder, Osmformat.RelationOrBuilder>(
                  relations_, ((bitField0_ & 0x00000008) != 0), getParentForChildren(), isClean());
          relations_ = null;
        }
        return relationsBuilder_;
      }

      private java.util.List<Osmformat.ChangeSet> changesets_ = java.util.Collections.emptyList();

      private void ensureChangesetsIsMutable() {
        if (!((bitField0_ & 0x00000010) != 0)) {
          changesets_ = new java.util.ArrayList<Osmformat.ChangeSet>(changesets_);
          bitField0_ |= 0x00000010;
        }
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.ChangeSet, Osmformat.ChangeSet.Builder, Osmformat.ChangeSetOrBuilder>
          changesetsBuilder_;

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public java.util.List<Osmformat.ChangeSet> getChangesetsList() {
        if (changesetsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(changesets_);
        } else {
          return changesetsBuilder_.getMessageList();
        }
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public int getChangesetsCount() {
        if (changesetsBuilder_ == null) {
          return changesets_.size();
        } else {
          return changesetsBuilder_.getCount();
        }
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Osmformat.ChangeSet getChangesets(int index) {
        if (changesetsBuilder_ == null) {
          return changesets_.get(index);
        } else {
          return changesetsBuilder_.getMessage(index);
        }
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder setChangesets(int index, Osmformat.ChangeSet value) {
        if (changesetsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureChangesetsIsMutable();
          changesets_.set(index, value);
          onChanged();
        } else {
          changesetsBuilder_.setMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder setChangesets(int index, Osmformat.ChangeSet.Builder builderForValue) {
        if (changesetsBuilder_ == null) {
          ensureChangesetsIsMutable();
          changesets_.set(index, builderForValue.build());
          onChanged();
        } else {
          changesetsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder addChangesets(Osmformat.ChangeSet value) {
        if (changesetsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureChangesetsIsMutable();
          changesets_.add(value);
          onChanged();
        } else {
          changesetsBuilder_.addMessage(value);
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder addChangesets(int index, Osmformat.ChangeSet value) {
        if (changesetsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureChangesetsIsMutable();
          changesets_.add(index, value);
          onChanged();
        } else {
          changesetsBuilder_.addMessage(index, value);
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder addChangesets(Osmformat.ChangeSet.Builder builderForValue) {
        if (changesetsBuilder_ == null) {
          ensureChangesetsIsMutable();
          changesets_.add(builderForValue.build());
          onChanged();
        } else {
          changesetsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder addChangesets(int index, Osmformat.ChangeSet.Builder builderForValue) {
        if (changesetsBuilder_ == null) {
          ensureChangesetsIsMutable();
          changesets_.add(index, builderForValue.build());
          onChanged();
        } else {
          changesetsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder addAllChangesets(Iterable<? extends Osmformat.ChangeSet> values) {
        if (changesetsBuilder_ == null) {
          ensureChangesetsIsMutable();
          proto4.AbstractMessageLite.Builder.addAll(values, changesets_);
          onChanged();
        } else {
          changesetsBuilder_.addAllMessages(values);
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder clearChangesets() {
        if (changesetsBuilder_ == null) {
          changesets_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000010);
          onChanged();
        } else {
          changesetsBuilder_.clear();
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Builder removeChangesets(int index) {
        if (changesetsBuilder_ == null) {
          ensureChangesetsIsMutable();
          changesets_.remove(index);
          onChanged();
        } else {
          changesetsBuilder_.remove(index);
        }
        return this;
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Osmformat.ChangeSet.Builder getChangesetsBuilder(int index) {
        return getChangesetsFieldBuilder().getBuilder(index);
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Osmformat.ChangeSetOrBuilder getChangesetsOrBuilder(int index) {
        if (changesetsBuilder_ == null) {
          return changesets_.get(index);
        } else {
          return changesetsBuilder_.getMessageOrBuilder(index);
        }
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public java.util.List<? extends Osmformat.ChangeSetOrBuilder> getChangesetsOrBuilderList() {
        if (changesetsBuilder_ != null) {
          return changesetsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(changesets_);
        }
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Osmformat.ChangeSet.Builder addChangesetsBuilder() {
        return getChangesetsFieldBuilder().addBuilder(Osmformat.ChangeSet.getDefaultInstance());
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public Osmformat.ChangeSet.Builder addChangesetsBuilder(int index) {
        return getChangesetsFieldBuilder()
            .addBuilder(index, Osmformat.ChangeSet.getDefaultInstance());
      }

      /** <code>repeated .ChangeSet changesets = 5;</code> */
      public java.util.List<Osmformat.ChangeSet.Builder> getChangesetsBuilderList() {
        return getChangesetsFieldBuilder().getBuilderList();
      }

      private proto4.RepeatedFieldBuilder<
              Osmformat.ChangeSet, Osmformat.ChangeSet.Builder, Osmformat.ChangeSetOrBuilder>
          getChangesetsFieldBuilder() {
        if (changesetsBuilder_ == null) {
          changesetsBuilder_ =
              new proto4.RepeatedFieldBuilder<
                  Osmformat.ChangeSet, Osmformat.ChangeSet.Builder, Osmformat.ChangeSetOrBuilder>(
                  changesets_, ((bitField0_ & 0x00000010) != 0), getParentForChildren(), isClean());
          changesets_ = null;
        }
        return changesetsBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:PrimitiveGroup)
    }

    // @@protoc_insertion_point(class_scope:PrimitiveGroup)
    private static final Osmformat.PrimitiveGroup DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.PrimitiveGroup();
    }

    public static Osmformat.PrimitiveGroup getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<PrimitiveGroup> PARSER =
        new proto4.AbstractParser<PrimitiveGroup>() {
          @Override
          public PrimitiveGroup parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<PrimitiveGroup> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<PrimitiveGroup> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.PrimitiveGroup getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface StringTableOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:StringTable)
      proto4.MessageOrBuilder {

    /**
     * <code>repeated bytes s = 1;</code>
     *
     * @return A list containing the s.
     */
    java.util.List<proto4.ByteString> getSList();

    /**
     * <code>repeated bytes s = 1;</code>
     *
     * @return The count of s.
     */
    int getSCount();

    /**
     * <code>repeated bytes s = 1;</code>
     *
     * @param index The index of the element to return.
     * @return The s at the given index.
     */
    proto4.ByteString getS(int index);
  }

  /**
   *
   *
   * <pre>
   * * String table, contains the common strings in each block.
   *
   * Note that we reserve index '0' as a delimiter, so the entry at that
   * index in the table is ALWAYS blank and unused.
   * </pre>
   *
   * <p>Protobuf type {@code StringTable}
   */
  public static final class StringTable extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:StringTable)
      StringTableOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          StringTable.class.getName());
    }

    // Use StringTable.newBuilder() to construct.
    private StringTable(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private StringTable() {
      s_ = emptyList(proto4.ByteString.class);
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_StringTable_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_StringTable_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.StringTable.class, Osmformat.StringTable.Builder.class);
    }

    public static final int S_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private proto4.Internal.ProtobufList<proto4.ByteString> s_ = emptyList(proto4.ByteString.class);

    /**
     * <code>repeated bytes s = 1;</code>
     *
     * @return A list containing the s.
     */
    @Override
    public java.util.List<proto4.ByteString> getSList() {
      return s_;
    }

    /**
     * <code>repeated bytes s = 1;</code>
     *
     * @return The count of s.
     */
    public int getSCount() {
      return s_.size();
    }

    /**
     * <code>repeated bytes s = 1;</code>
     *
     * @param index The index of the element to return.
     * @return The s at the given index.
     */
    public proto4.ByteString getS(int index) {
      return s_.get(index);
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      for (int i = 0; i < s_.size(); i++) {
        output.writeBytes(1, s_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      {
        int dataSize = 0;
        for (int i = 0; i < s_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeBytesSizeNoTag(s_.get(i));
        }
        size += dataSize;
        size += 1 * getSList().size();
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.StringTable)) {
        return super.equals(obj);
      }
      Osmformat.StringTable other = (Osmformat.StringTable) obj;

      if (!getSList().equals(other.getSList())) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (getSCount() > 0) {
        hash = (37 * hash) + S_FIELD_NUMBER;
        hash = (53 * hash) + getSList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.StringTable parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.StringTable parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.StringTable parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.StringTable parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.StringTable parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.StringTable parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.StringTable parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.StringTable parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.StringTable parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.StringTable parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.StringTable parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.StringTable parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.StringTable prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /**
     *
     *
     * <pre>
     * * String table, contains the common strings in each block.
     *
     * Note that we reserve index '0' as a delimiter, so the entry at that
     * index in the table is ALWAYS blank and unused.
     * </pre>
     *
     * <p>Protobuf type {@code StringTable}
     */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:StringTable)
        Osmformat.StringTableOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_StringTable_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_StringTable_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.StringTable.class, Osmformat.StringTable.Builder.class);
      }

      // Construct using Osmformat.StringTable.newBuilder()
      private Builder() {}

      private Builder(BuilderParent parent) {
        super(parent);
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        s_ = emptyList(proto4.ByteString.class);
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_StringTable_descriptor;
      }

      @Override
      public Osmformat.StringTable getDefaultInstanceForType() {
        return Osmformat.StringTable.getDefaultInstance();
      }

      @Override
      public Osmformat.StringTable build() {
        Osmformat.StringTable result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.StringTable buildPartial() {
        Osmformat.StringTable result = new Osmformat.StringTable(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.StringTable result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          s_.makeImmutable();
          result.s_ = s_;
        }
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.StringTable) {
          return mergeFrom((Osmformat.StringTable) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.StringTable other) {
        if (other == Osmformat.StringTable.getDefaultInstance()) return this;
        if (!other.s_.isEmpty()) {
          if (s_.isEmpty()) {
            s_ = other.s_;
            s_.makeImmutable();
            bitField0_ |= 0x00000001;
          } else {
            ensureSIsMutable();
            s_.addAll(other.s_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 10:
                {
                  proto4.ByteString v = input.readBytes();
                  ensureSIsMutable();
                  s_.add(v);
                  break;
                } // case 10
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private proto4.Internal.ProtobufList<proto4.ByteString> s_ =
          emptyList(proto4.ByteString.class);

      private void ensureSIsMutable() {
        if (!s_.isModifiable()) {
          s_ = makeMutableCopy(s_);
        }
        bitField0_ |= 0x00000001;
      }

      /**
       * <code>repeated bytes s = 1;</code>
       *
       * @return A list containing the s.
       */
      public java.util.List<proto4.ByteString> getSList() {
        s_.makeImmutable();
        return s_;
      }

      /**
       * <code>repeated bytes s = 1;</code>
       *
       * @return The count of s.
       */
      public int getSCount() {
        return s_.size();
      }

      /**
       * <code>repeated bytes s = 1;</code>
       *
       * @param index The index of the element to return.
       * @return The s at the given index.
       */
      public proto4.ByteString getS(int index) {
        return s_.get(index);
      }

      /**
       * <code>repeated bytes s = 1;</code>
       *
       * @param index The index to set the value at.
       * @param value The s to set.
       * @return This builder for chaining.
       */
      public Builder setS(int index, proto4.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureSIsMutable();
        s_.set(index, value);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>repeated bytes s = 1;</code>
       *
       * @param value The s to add.
       * @return This builder for chaining.
       */
      public Builder addS(proto4.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureSIsMutable();
        s_.add(value);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>repeated bytes s = 1;</code>
       *
       * @param values The s to add.
       * @return This builder for chaining.
       */
      public Builder addAllS(Iterable<? extends proto4.ByteString> values) {
        ensureSIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, s_);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>repeated bytes s = 1;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearS() {
        s_ = emptyList(proto4.ByteString.class);
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:StringTable)
    }

    // @@protoc_insertion_point(class_scope:StringTable)
    private static final Osmformat.StringTable DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.StringTable();
    }

    public static Osmformat.StringTable getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<StringTable> PARSER =
        new proto4.AbstractParser<StringTable>() {
          @Override
          public StringTable parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<StringTable> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<StringTable> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.StringTable getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface InfoOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:Info)
      proto4.MessageOrBuilder {

    /**
     * <code>optional int32 version = 1 [default = -1];</code>
     *
     * @return Whether the version field is set.
     */
    boolean hasVersion();

    /**
     * <code>optional int32 version = 1 [default = -1];</code>
     *
     * @return The version.
     */
    int getVersion();

    /**
     * <code>optional int64 timestamp = 2;</code>
     *
     * @return Whether the timestamp field is set.
     */
    boolean hasTimestamp();

    /**
     * <code>optional int64 timestamp = 2;</code>
     *
     * @return The timestamp.
     */
    long getTimestamp();

    /**
     * <code>optional int64 changeset = 3;</code>
     *
     * @return Whether the changeset field is set.
     */
    boolean hasChangeset();

    /**
     * <code>optional int64 changeset = 3;</code>
     *
     * @return The changeset.
     */
    long getChangeset();

    /**
     * <code>optional int32 uid = 4;</code>
     *
     * @return Whether the uid field is set.
     */
    boolean hasUid();

    /**
     * <code>optional int32 uid = 4;</code>
     *
     * @return The uid.
     */
    int getUid();

    /**
     *
     *
     * <pre>
     * String IDs
     * </pre>
     *
     * <code>optional uint32 user_sid = 5;</code>
     *
     * @return Whether the userSid field is set.
     */
    boolean hasUserSid();

    /**
     *
     *
     * <pre>
     * String IDs
     * </pre>
     *
     * <code>optional uint32 user_sid = 5;</code>
     *
     * @return The userSid.
     */
    int getUserSid();

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>optional bool visible = 6;</code>
     *
     * @return Whether the visible field is set.
     */
    boolean hasVisible();

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>optional bool visible = 6;</code>
     *
     * @return The visible.
     */
    boolean getVisible();
  }

  /**
   *
   *
   * <pre>
   * Optional metadata that may be included into each primitive.
   * </pre>
   *
   * <p>Protobuf type {@code Info}
   */
  public static final class Info extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:Info)
      InfoOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          Info.class.getName());
    }

    // Use Info.newBuilder() to construct.
    private Info(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private Info() {
      version_ = -1;
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_Info_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_Info_fieldAccessorTable
          .ensureFieldAccessorsInitialized(Osmformat.Info.class, Osmformat.Info.Builder.class);
    }

    private int bitField0_;
    public static final int VERSION_FIELD_NUMBER = 1;
    private int version_ = -1;

    /**
     * <code>optional int32 version = 1 [default = -1];</code>
     *
     * @return Whether the version field is set.
     */
    @Override
    public boolean hasVersion() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     * <code>optional int32 version = 1 [default = -1];</code>
     *
     * @return The version.
     */
    @Override
    public int getVersion() {
      return version_;
    }

    public static final int TIMESTAMP_FIELD_NUMBER = 2;
    private long timestamp_ = 0L;

    /**
     * <code>optional int64 timestamp = 2;</code>
     *
     * @return Whether the timestamp field is set.
     */
    @Override
    public boolean hasTimestamp() {
      return ((bitField0_ & 0x00000002) != 0);
    }

    /**
     * <code>optional int64 timestamp = 2;</code>
     *
     * @return The timestamp.
     */
    @Override
    public long getTimestamp() {
      return timestamp_;
    }

    public static final int CHANGESET_FIELD_NUMBER = 3;
    private long changeset_ = 0L;

    /**
     * <code>optional int64 changeset = 3;</code>
     *
     * @return Whether the changeset field is set.
     */
    @Override
    public boolean hasChangeset() {
      return ((bitField0_ & 0x00000004) != 0);
    }

    /**
     * <code>optional int64 changeset = 3;</code>
     *
     * @return The changeset.
     */
    @Override
    public long getChangeset() {
      return changeset_;
    }

    public static final int UID_FIELD_NUMBER = 4;
    private int uid_ = 0;

    /**
     * <code>optional int32 uid = 4;</code>
     *
     * @return Whether the uid field is set.
     */
    @Override
    public boolean hasUid() {
      return ((bitField0_ & 0x00000008) != 0);
    }

    /**
     * <code>optional int32 uid = 4;</code>
     *
     * @return The uid.
     */
    @Override
    public int getUid() {
      return uid_;
    }

    public static final int USER_SID_FIELD_NUMBER = 5;
    private int userSid_ = 0;

    /**
     *
     *
     * <pre>
     * String IDs
     * </pre>
     *
     * <code>optional uint32 user_sid = 5;</code>
     *
     * @return Whether the userSid field is set.
     */
    @Override
    public boolean hasUserSid() {
      return ((bitField0_ & 0x00000010) != 0);
    }

    /**
     *
     *
     * <pre>
     * String IDs
     * </pre>
     *
     * <code>optional uint32 user_sid = 5;</code>
     *
     * @return The userSid.
     */
    @Override
    public int getUserSid() {
      return userSid_;
    }

    public static final int VISIBLE_FIELD_NUMBER = 6;
    private boolean visible_ = false;

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>optional bool visible = 6;</code>
     *
     * @return Whether the visible field is set.
     */
    @Override
    public boolean hasVisible() {
      return ((bitField0_ & 0x00000020) != 0);
    }

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>optional bool visible = 6;</code>
     *
     * @return The visible.
     */
    @Override
    public boolean getVisible() {
      return visible_;
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt32(1, version_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeInt64(2, timestamp_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeInt64(3, changeset_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeInt32(4, uid_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeUInt32(5, userSid_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        output.writeBool(6, visible_);
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeInt32Size(1, version_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(2, timestamp_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(3, changeset_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += proto4.CodedOutputStream.computeInt32Size(4, uid_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += proto4.CodedOutputStream.computeUInt32Size(5, userSid_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        size += proto4.CodedOutputStream.computeBoolSize(6, visible_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.Info)) {
        return super.equals(obj);
      }
      Osmformat.Info other = (Osmformat.Info) obj;

      if (hasVersion() != other.hasVersion()) return false;
      if (hasVersion()) {
        if (getVersion() != other.getVersion()) return false;
      }
      if (hasTimestamp() != other.hasTimestamp()) return false;
      if (hasTimestamp()) {
        if (getTimestamp() != other.getTimestamp()) return false;
      }
      if (hasChangeset() != other.hasChangeset()) return false;
      if (hasChangeset()) {
        if (getChangeset() != other.getChangeset()) return false;
      }
      if (hasUid() != other.hasUid()) return false;
      if (hasUid()) {
        if (getUid() != other.getUid()) return false;
      }
      if (hasUserSid() != other.hasUserSid()) return false;
      if (hasUserSid()) {
        if (getUserSid() != other.getUserSid()) return false;
      }
      if (hasVisible() != other.hasVisible()) return false;
      if (hasVisible()) {
        if (getVisible() != other.getVisible()) return false;
      }
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasVersion()) {
        hash = (37 * hash) + VERSION_FIELD_NUMBER;
        hash = (53 * hash) + getVersion();
      }
      if (hasTimestamp()) {
        hash = (37 * hash) + TIMESTAMP_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getTimestamp());
      }
      if (hasChangeset()) {
        hash = (37 * hash) + CHANGESET_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getChangeset());
      }
      if (hasUid()) {
        hash = (37 * hash) + UID_FIELD_NUMBER;
        hash = (53 * hash) + getUid();
      }
      if (hasUserSid()) {
        hash = (37 * hash) + USER_SID_FIELD_NUMBER;
        hash = (53 * hash) + getUserSid();
      }
      if (hasVisible()) {
        hash = (37 * hash) + VISIBLE_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashBoolean(getVisible());
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.Info parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Info parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Info parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Info parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Info parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Info parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Info parseFrom(java.io.InputStream input) throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.Info parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.Info parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.Info parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.Info parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.Info parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.Info prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /**
     *
     *
     * <pre>
     * Optional metadata that may be included into each primitive.
     * </pre>
     *
     * <p>Protobuf type {@code Info}
     */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:Info)
        Osmformat.InfoOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Info_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Info_fieldAccessorTable
            .ensureFieldAccessorsInitialized(Osmformat.Info.class, Osmformat.Info.Builder.class);
      }

      // Construct using Osmformat.Info.newBuilder()
      private Builder() {}

      private Builder(BuilderParent parent) {
        super(parent);
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        version_ = -1;
        timestamp_ = 0L;
        changeset_ = 0L;
        uid_ = 0;
        userSid_ = 0;
        visible_ = false;
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Info_descriptor;
      }

      @Override
      public Osmformat.Info getDefaultInstanceForType() {
        return Osmformat.Info.getDefaultInstance();
      }

      @Override
      public Osmformat.Info build() {
        Osmformat.Info result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.Info buildPartial() {
        Osmformat.Info result = new Osmformat.Info(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.Info result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.version_ = version_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.timestamp_ = timestamp_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.changeset_ = changeset_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.uid_ = uid_;
          to_bitField0_ |= 0x00000008;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.userSid_ = userSid_;
          to_bitField0_ |= 0x00000010;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          result.visible_ = visible_;
          to_bitField0_ |= 0x00000020;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.Info) {
          return mergeFrom((Osmformat.Info) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.Info other) {
        if (other == Osmformat.Info.getDefaultInstance()) return this;
        if (other.hasVersion()) {
          setVersion(other.getVersion());
        }
        if (other.hasTimestamp()) {
          setTimestamp(other.getTimestamp());
        }
        if (other.hasChangeset()) {
          setChangeset(other.getChangeset());
        }
        if (other.hasUid()) {
          setUid(other.getUid());
        }
        if (other.hasUserSid()) {
          setUserSid(other.getUserSid());
        }
        if (other.hasVisible()) {
          setVisible(other.getVisible());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8:
                {
                  version_ = input.readInt32();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 8
              case 16:
                {
                  timestamp_ = input.readInt64();
                  bitField0_ |= 0x00000002;
                  break;
                } // case 16
              case 24:
                {
                  changeset_ = input.readInt64();
                  bitField0_ |= 0x00000004;
                  break;
                } // case 24
              case 32:
                {
                  uid_ = input.readInt32();
                  bitField0_ |= 0x00000008;
                  break;
                } // case 32
              case 40:
                {
                  userSid_ = input.readUInt32();
                  bitField0_ |= 0x00000010;
                  break;
                } // case 40
              case 48:
                {
                  visible_ = input.readBool();
                  bitField0_ |= 0x00000020;
                  break;
                } // case 48
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private int version_ = -1;

      /**
       * <code>optional int32 version = 1 [default = -1];</code>
       *
       * @return Whether the version field is set.
       */
      @Override
      public boolean hasVersion() {
        return ((bitField0_ & 0x00000001) != 0);
      }

      /**
       * <code>optional int32 version = 1 [default = -1];</code>
       *
       * @return The version.
       */
      @Override
      public int getVersion() {
        return version_;
      }

      /**
       * <code>optional int32 version = 1 [default = -1];</code>
       *
       * @param value The version to set.
       * @return This builder for chaining.
       */
      public Builder setVersion(int value) {

        version_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>optional int32 version = 1 [default = -1];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearVersion() {
        bitField0_ = (bitField0_ & ~0x00000001);
        version_ = -1;
        onChanged();
        return this;
      }

      private long timestamp_;

      /**
       * <code>optional int64 timestamp = 2;</code>
       *
       * @return Whether the timestamp field is set.
       */
      @Override
      public boolean hasTimestamp() {
        return ((bitField0_ & 0x00000002) != 0);
      }

      /**
       * <code>optional int64 timestamp = 2;</code>
       *
       * @return The timestamp.
       */
      @Override
      public long getTimestamp() {
        return timestamp_;
      }

      /**
       * <code>optional int64 timestamp = 2;</code>
       *
       * @param value The timestamp to set.
       * @return This builder for chaining.
       */
      public Builder setTimestamp(long value) {

        timestamp_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       * <code>optional int64 timestamp = 2;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearTimestamp() {
        bitField0_ = (bitField0_ & ~0x00000002);
        timestamp_ = 0L;
        onChanged();
        return this;
      }

      private long changeset_;

      /**
       * <code>optional int64 changeset = 3;</code>
       *
       * @return Whether the changeset field is set.
       */
      @Override
      public boolean hasChangeset() {
        return ((bitField0_ & 0x00000004) != 0);
      }

      /**
       * <code>optional int64 changeset = 3;</code>
       *
       * @return The changeset.
       */
      @Override
      public long getChangeset() {
        return changeset_;
      }

      /**
       * <code>optional int64 changeset = 3;</code>
       *
       * @param value The changeset to set.
       * @return This builder for chaining.
       */
      public Builder setChangeset(long value) {

        changeset_ = value;
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>optional int64 changeset = 3;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearChangeset() {
        bitField0_ = (bitField0_ & ~0x00000004);
        changeset_ = 0L;
        onChanged();
        return this;
      }

      private int uid_;

      /**
       * <code>optional int32 uid = 4;</code>
       *
       * @return Whether the uid field is set.
       */
      @Override
      public boolean hasUid() {
        return ((bitField0_ & 0x00000008) != 0);
      }

      /**
       * <code>optional int32 uid = 4;</code>
       *
       * @return The uid.
       */
      @Override
      public int getUid() {
        return uid_;
      }

      /**
       * <code>optional int32 uid = 4;</code>
       *
       * @param value The uid to set.
       * @return This builder for chaining.
       */
      public Builder setUid(int value) {

        uid_ = value;
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       * <code>optional int32 uid = 4;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearUid() {
        bitField0_ = (bitField0_ & ~0x00000008);
        uid_ = 0;
        onChanged();
        return this;
      }

      private int userSid_;

      /**
       *
       *
       * <pre>
       * String IDs
       * </pre>
       *
       * <code>optional uint32 user_sid = 5;</code>
       *
       * @return Whether the userSid field is set.
       */
      @Override
      public boolean hasUserSid() {
        return ((bitField0_ & 0x00000010) != 0);
      }

      /**
       *
       *
       * <pre>
       * String IDs
       * </pre>
       *
       * <code>optional uint32 user_sid = 5;</code>
       *
       * @return The userSid.
       */
      @Override
      public int getUserSid() {
        return userSid_;
      }

      /**
       *
       *
       * <pre>
       * String IDs
       * </pre>
       *
       * <code>optional uint32 user_sid = 5;</code>
       *
       * @param value The userSid to set.
       * @return This builder for chaining.
       */
      public Builder setUserSid(int value) {

        userSid_ = value;
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * String IDs
       * </pre>
       *
       * <code>optional uint32 user_sid = 5;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearUserSid() {
        bitField0_ = (bitField0_ & ~0x00000010);
        userSid_ = 0;
        onChanged();
        return this;
      }

      private boolean visible_;

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>optional bool visible = 6;</code>
       *
       * @return Whether the visible field is set.
       */
      @Override
      public boolean hasVisible() {
        return ((bitField0_ & 0x00000020) != 0);
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>optional bool visible = 6;</code>
       *
       * @return The visible.
       */
      @Override
      public boolean getVisible() {
        return visible_;
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>optional bool visible = 6;</code>
       *
       * @param value The visible to set.
       * @return This builder for chaining.
       */
      public Builder setVisible(boolean value) {

        visible_ = value;
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>optional bool visible = 6;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearVisible() {
        bitField0_ = (bitField0_ & ~0x00000020);
        visible_ = false;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:Info)
    }

    // @@protoc_insertion_point(class_scope:Info)
    private static final Osmformat.Info DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.Info();
    }

    public static Osmformat.Info getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<Info> PARSER =
        new proto4.AbstractParser<Info>() {
          @Override
          public Info parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<Info> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<Info> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.Info getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface DenseInfoOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:DenseInfo)
      proto4.MessageOrBuilder {

    /**
     * <code>repeated int32 version = 1 [packed = true];</code>
     *
     * @return A list containing the version.
     */
    java.util.List<Integer> getVersionList();

    /**
     * <code>repeated int32 version = 1 [packed = true];</code>
     *
     * @return The count of version.
     */
    int getVersionCount();

    /**
     * <code>repeated int32 version = 1 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The version at the given index.
     */
    int getVersion(int index);

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 timestamp = 2 [packed = true];</code>
     *
     * @return A list containing the timestamp.
     */
    java.util.List<Long> getTimestampList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 timestamp = 2 [packed = true];</code>
     *
     * @return The count of timestamp.
     */
    int getTimestampCount();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 timestamp = 2 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The timestamp at the given index.
     */
    long getTimestamp(int index);

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 changeset = 3 [packed = true];</code>
     *
     * @return A list containing the changeset.
     */
    java.util.List<Long> getChangesetList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 changeset = 3 [packed = true];</code>
     *
     * @return The count of changeset.
     */
    int getChangesetCount();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 changeset = 3 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The changeset at the given index.
     */
    long getChangeset(int index);

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint32 uid = 4 [packed = true];</code>
     *
     * @return A list containing the uid.
     */
    java.util.List<Integer> getUidList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint32 uid = 4 [packed = true];</code>
     *
     * @return The count of uid.
     */
    int getUidCount();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint32 uid = 4 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The uid at the given index.
     */
    int getUid(int index);

    /**
     *
     *
     * <pre>
     * String IDs for usernames. DELTA coded
     * </pre>
     *
     * <code>repeated sint32 user_sid = 5 [packed = true];</code>
     *
     * @return A list containing the userSid.
     */
    java.util.List<Integer> getUserSidList();

    /**
     *
     *
     * <pre>
     * String IDs for usernames. DELTA coded
     * </pre>
     *
     * <code>repeated sint32 user_sid = 5 [packed = true];</code>
     *
     * @return The count of userSid.
     */
    int getUserSidCount();

    /**
     *
     *
     * <pre>
     * String IDs for usernames. DELTA coded
     * </pre>
     *
     * <code>repeated sint32 user_sid = 5 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The userSid at the given index.
     */
    int getUserSid(int index);

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>repeated bool visible = 6 [packed = true];</code>
     *
     * @return A list containing the visible.
     */
    java.util.List<Boolean> getVisibleList();

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>repeated bool visible = 6 [packed = true];</code>
     *
     * @return The count of visible.
     */
    int getVisibleCount();

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>repeated bool visible = 6 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The visible at the given index.
     */
    boolean getVisible(int index);
  }

  /**
   *
   *
   * <pre>
   * * Optional metadata that may be included into each primitive. Special dense format used in DenseNodes.
   * </pre>
   *
   * <p>Protobuf type {@code DenseInfo}
   */
  public static final class DenseInfo extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:DenseInfo)
      DenseInfoOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          DenseInfo.class.getName());
    }

    // Use DenseInfo.newBuilder() to construct.
    private DenseInfo(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private DenseInfo() {
      version_ = emptyIntList();
      timestamp_ = emptyLongList();
      changeset_ = emptyLongList();
      uid_ = emptyIntList();
      userSid_ = emptyIntList();
      visible_ = emptyBooleanList();
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_DenseInfo_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_DenseInfo_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.DenseInfo.class, Osmformat.DenseInfo.Builder.class);
    }

    public static final int VERSION_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList version_ = emptyIntList();

    /**
     * <code>repeated int32 version = 1 [packed = true];</code>
     *
     * @return A list containing the version.
     */
    @Override
    public java.util.List<Integer> getVersionList() {
      return version_;
    }

    /**
     * <code>repeated int32 version = 1 [packed = true];</code>
     *
     * @return The count of version.
     */
    public int getVersionCount() {
      return version_.size();
    }

    /**
     * <code>repeated int32 version = 1 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The version at the given index.
     */
    public int getVersion(int index) {
      return version_.getInt(index);
    }

    private int versionMemoizedSerializedSize = -1;

    public static final int TIMESTAMP_FIELD_NUMBER = 2;

    @SuppressWarnings("serial")
    private proto4.Internal.LongList timestamp_ = emptyLongList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 timestamp = 2 [packed = true];</code>
     *
     * @return A list containing the timestamp.
     */
    @Override
    public java.util.List<Long> getTimestampList() {
      return timestamp_;
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 timestamp = 2 [packed = true];</code>
     *
     * @return The count of timestamp.
     */
    public int getTimestampCount() {
      return timestamp_.size();
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 timestamp = 2 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The timestamp at the given index.
     */
    public long getTimestamp(int index) {
      return timestamp_.getLong(index);
    }

    private int timestampMemoizedSerializedSize = -1;

    public static final int CHANGESET_FIELD_NUMBER = 3;

    @SuppressWarnings("serial")
    private proto4.Internal.LongList changeset_ = emptyLongList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 changeset = 3 [packed = true];</code>
     *
     * @return A list containing the changeset.
     */
    @Override
    public java.util.List<Long> getChangesetList() {
      return changeset_;
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 changeset = 3 [packed = true];</code>
     *
     * @return The count of changeset.
     */
    public int getChangesetCount() {
      return changeset_.size();
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 changeset = 3 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The changeset at the given index.
     */
    public long getChangeset(int index) {
      return changeset_.getLong(index);
    }

    private int changesetMemoizedSerializedSize = -1;

    public static final int UID_FIELD_NUMBER = 4;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList uid_ = emptyIntList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint32 uid = 4 [packed = true];</code>
     *
     * @return A list containing the uid.
     */
    @Override
    public java.util.List<Integer> getUidList() {
      return uid_;
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint32 uid = 4 [packed = true];</code>
     *
     * @return The count of uid.
     */
    public int getUidCount() {
      return uid_.size();
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint32 uid = 4 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The uid at the given index.
     */
    public int getUid(int index) {
      return uid_.getInt(index);
    }

    private int uidMemoizedSerializedSize = -1;

    public static final int USER_SID_FIELD_NUMBER = 5;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList userSid_ = emptyIntList();

    /**
     *
     *
     * <pre>
     * String IDs for usernames. DELTA coded
     * </pre>
     *
     * <code>repeated sint32 user_sid = 5 [packed = true];</code>
     *
     * @return A list containing the userSid.
     */
    @Override
    public java.util.List<Integer> getUserSidList() {
      return userSid_;
    }

    /**
     *
     *
     * <pre>
     * String IDs for usernames. DELTA coded
     * </pre>
     *
     * <code>repeated sint32 user_sid = 5 [packed = true];</code>
     *
     * @return The count of userSid.
     */
    public int getUserSidCount() {
      return userSid_.size();
    }

    /**
     *
     *
     * <pre>
     * String IDs for usernames. DELTA coded
     * </pre>
     *
     * <code>repeated sint32 user_sid = 5 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The userSid at the given index.
     */
    public int getUserSid(int index) {
      return userSid_.getInt(index);
    }

    private int userSidMemoizedSerializedSize = -1;

    public static final int VISIBLE_FIELD_NUMBER = 6;

    @SuppressWarnings("serial")
    private proto4.Internal.BooleanList visible_ = emptyBooleanList();

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>repeated bool visible = 6 [packed = true];</code>
     *
     * @return A list containing the visible.
     */
    @Override
    public java.util.List<Boolean> getVisibleList() {
      return visible_;
    }

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>repeated bool visible = 6 [packed = true];</code>
     *
     * @return The count of visible.
     */
    public int getVisibleCount() {
      return visible_.size();
    }

    /**
     *
     *
     * <pre>
     * The visible flag is used to store history information. It indicates that
     * the current object version has been created by a delete operation on the
     * OSM API.
     * When a writer sets this flag, it MUST add a required_features tag with
     * value "HistoricalInformation" to the HeaderBlock.
     * If this flag is not available for some object it MUST be assumed to be
     * true if the file has the required_features tag "HistoricalInformation"
     * set.
     * </pre>
     *
     * <code>repeated bool visible = 6 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The visible at the given index.
     */
    public boolean getVisible(int index) {
      return visible_.getBoolean(index);
    }

    private int visibleMemoizedSerializedSize = -1;

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      getSerializedSize();
      if (getVersionList().size() > 0) {
        output.writeUInt32NoTag(10);
        output.writeUInt32NoTag(versionMemoizedSerializedSize);
      }
      for (int i = 0; i < version_.size(); i++) {
        output.writeInt32NoTag(version_.getInt(i));
      }
      if (getTimestampList().size() > 0) {
        output.writeUInt32NoTag(18);
        output.writeUInt32NoTag(timestampMemoizedSerializedSize);
      }
      for (int i = 0; i < timestamp_.size(); i++) {
        output.writeSInt64NoTag(timestamp_.getLong(i));
      }
      if (getChangesetList().size() > 0) {
        output.writeUInt32NoTag(26);
        output.writeUInt32NoTag(changesetMemoizedSerializedSize);
      }
      for (int i = 0; i < changeset_.size(); i++) {
        output.writeSInt64NoTag(changeset_.getLong(i));
      }
      if (getUidList().size() > 0) {
        output.writeUInt32NoTag(34);
        output.writeUInt32NoTag(uidMemoizedSerializedSize);
      }
      for (int i = 0; i < uid_.size(); i++) {
        output.writeSInt32NoTag(uid_.getInt(i));
      }
      if (getUserSidList().size() > 0) {
        output.writeUInt32NoTag(42);
        output.writeUInt32NoTag(userSidMemoizedSerializedSize);
      }
      for (int i = 0; i < userSid_.size(); i++) {
        output.writeSInt32NoTag(userSid_.getInt(i));
      }
      if (getVisibleList().size() > 0) {
        output.writeUInt32NoTag(50);
        output.writeUInt32NoTag(visibleMemoizedSerializedSize);
      }
      for (int i = 0; i < visible_.size(); i++) {
        output.writeBoolNoTag(visible_.getBoolean(i));
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      {
        int dataSize = 0;
        for (int i = 0; i < version_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeInt32SizeNoTag(version_.getInt(i));
        }
        size += dataSize;
        if (!getVersionList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        versionMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < timestamp_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt64SizeNoTag(timestamp_.getLong(i));
        }
        size += dataSize;
        if (!getTimestampList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        timestampMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < changeset_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt64SizeNoTag(changeset_.getLong(i));
        }
        size += dataSize;
        if (!getChangesetList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        changesetMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < uid_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt32SizeNoTag(uid_.getInt(i));
        }
        size += dataSize;
        if (!getUidList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        uidMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < userSid_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt32SizeNoTag(userSid_.getInt(i));
        }
        size += dataSize;
        if (!getUserSidList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        userSidMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        dataSize = 1 * getVisibleList().size();
        size += dataSize;
        if (!getVisibleList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        visibleMemoizedSerializedSize = dataSize;
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.DenseInfo)) {
        return super.equals(obj);
      }
      Osmformat.DenseInfo other = (Osmformat.DenseInfo) obj;

      if (!getVersionList().equals(other.getVersionList())) return false;
      if (!getTimestampList().equals(other.getTimestampList())) return false;
      if (!getChangesetList().equals(other.getChangesetList())) return false;
      if (!getUidList().equals(other.getUidList())) return false;
      if (!getUserSidList().equals(other.getUserSidList())) return false;
      if (!getVisibleList().equals(other.getVisibleList())) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (getVersionCount() > 0) {
        hash = (37 * hash) + VERSION_FIELD_NUMBER;
        hash = (53 * hash) + getVersionList().hashCode();
      }
      if (getTimestampCount() > 0) {
        hash = (37 * hash) + TIMESTAMP_FIELD_NUMBER;
        hash = (53 * hash) + getTimestampList().hashCode();
      }
      if (getChangesetCount() > 0) {
        hash = (37 * hash) + CHANGESET_FIELD_NUMBER;
        hash = (53 * hash) + getChangesetList().hashCode();
      }
      if (getUidCount() > 0) {
        hash = (37 * hash) + UID_FIELD_NUMBER;
        hash = (53 * hash) + getUidList().hashCode();
      }
      if (getUserSidCount() > 0) {
        hash = (37 * hash) + USER_SID_FIELD_NUMBER;
        hash = (53 * hash) + getUserSidList().hashCode();
      }
      if (getVisibleCount() > 0) {
        hash = (37 * hash) + VISIBLE_FIELD_NUMBER;
        hash = (53 * hash) + getVisibleList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.DenseInfo parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.DenseInfo parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.DenseInfo parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.DenseInfo parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.DenseInfo parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.DenseInfo parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.DenseInfo parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.DenseInfo parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.DenseInfo parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.DenseInfo parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.DenseInfo parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.DenseInfo parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.DenseInfo prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /**
     *
     *
     * <pre>
     * * Optional metadata that may be included into each primitive. Special dense format used in DenseNodes.
     * </pre>
     *
     * <p>Protobuf type {@code DenseInfo}
     */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:DenseInfo)
        Osmformat.DenseInfoOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_DenseInfo_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_DenseInfo_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.DenseInfo.class, Osmformat.DenseInfo.Builder.class);
      }

      // Construct using Osmformat.DenseInfo.newBuilder()
      private Builder() {}

      private Builder(BuilderParent parent) {
        super(parent);
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        version_ = emptyIntList();
        timestamp_ = emptyLongList();
        changeset_ = emptyLongList();
        uid_ = emptyIntList();
        userSid_ = emptyIntList();
        visible_ = emptyBooleanList();
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_DenseInfo_descriptor;
      }

      @Override
      public Osmformat.DenseInfo getDefaultInstanceForType() {
        return Osmformat.DenseInfo.getDefaultInstance();
      }

      @Override
      public Osmformat.DenseInfo build() {
        Osmformat.DenseInfo result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.DenseInfo buildPartial() {
        Osmformat.DenseInfo result = new Osmformat.DenseInfo(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.DenseInfo result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          version_.makeImmutable();
          result.version_ = version_;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          timestamp_.makeImmutable();
          result.timestamp_ = timestamp_;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          changeset_.makeImmutable();
          result.changeset_ = changeset_;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          uid_.makeImmutable();
          result.uid_ = uid_;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          userSid_.makeImmutable();
          result.userSid_ = userSid_;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          visible_.makeImmutable();
          result.visible_ = visible_;
        }
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.DenseInfo) {
          return mergeFrom((Osmformat.DenseInfo) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.DenseInfo other) {
        if (other == Osmformat.DenseInfo.getDefaultInstance()) return this;
        if (!other.version_.isEmpty()) {
          if (version_.isEmpty()) {
            version_ = other.version_;
            version_.makeImmutable();
            bitField0_ |= 0x00000001;
          } else {
            ensureVersionIsMutable();
            version_.addAll(other.version_);
          }
          onChanged();
        }
        if (!other.timestamp_.isEmpty()) {
          if (timestamp_.isEmpty()) {
            timestamp_ = other.timestamp_;
            timestamp_.makeImmutable();
            bitField0_ |= 0x00000002;
          } else {
            ensureTimestampIsMutable();
            timestamp_.addAll(other.timestamp_);
          }
          onChanged();
        }
        if (!other.changeset_.isEmpty()) {
          if (changeset_.isEmpty()) {
            changeset_ = other.changeset_;
            changeset_.makeImmutable();
            bitField0_ |= 0x00000004;
          } else {
            ensureChangesetIsMutable();
            changeset_.addAll(other.changeset_);
          }
          onChanged();
        }
        if (!other.uid_.isEmpty()) {
          if (uid_.isEmpty()) {
            uid_ = other.uid_;
            uid_.makeImmutable();
            bitField0_ |= 0x00000008;
          } else {
            ensureUidIsMutable();
            uid_.addAll(other.uid_);
          }
          onChanged();
        }
        if (!other.userSid_.isEmpty()) {
          if (userSid_.isEmpty()) {
            userSid_ = other.userSid_;
            userSid_.makeImmutable();
            bitField0_ |= 0x00000010;
          } else {
            ensureUserSidIsMutable();
            userSid_.addAll(other.userSid_);
          }
          onChanged();
        }
        if (!other.visible_.isEmpty()) {
          if (visible_.isEmpty()) {
            visible_ = other.visible_;
            visible_.makeImmutable();
            bitField0_ |= 0x00000020;
          } else {
            ensureVisibleIsMutable();
            visible_.addAll(other.visible_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8:
                {
                  int v = input.readInt32();
                  ensureVersionIsMutable();
                  version_.addInt(v);
                  break;
                } // case 8
              case 10:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureVersionIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    version_.addInt(input.readInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 10
              case 16:
                {
                  long v = input.readSInt64();
                  ensureTimestampIsMutable();
                  timestamp_.addLong(v);
                  break;
                } // case 16
              case 18:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureTimestampIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    timestamp_.addLong(input.readSInt64());
                  }
                  input.popLimit(limit);
                  break;
                } // case 18
              case 24:
                {
                  long v = input.readSInt64();
                  ensureChangesetIsMutable();
                  changeset_.addLong(v);
                  break;
                } // case 24
              case 26:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureChangesetIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    changeset_.addLong(input.readSInt64());
                  }
                  input.popLimit(limit);
                  break;
                } // case 26
              case 32:
                {
                  int v = input.readSInt32();
                  ensureUidIsMutable();
                  uid_.addInt(v);
                  break;
                } // case 32
              case 34:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureUidIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    uid_.addInt(input.readSInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 34
              case 40:
                {
                  int v = input.readSInt32();
                  ensureUserSidIsMutable();
                  userSid_.addInt(v);
                  break;
                } // case 40
              case 42:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureUserSidIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    userSid_.addInt(input.readSInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 42
              case 48:
                {
                  boolean v = input.readBool();
                  ensureVisibleIsMutable();
                  visible_.addBoolean(v);
                  break;
                } // case 48
              case 50:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  int alloc = length > 4096 ? 4096 : length;
                  ensureVisibleIsMutable(alloc / 1);
                  while (input.getBytesUntilLimit() > 0) {
                    visible_.addBoolean(input.readBool());
                  }
                  input.popLimit(limit);
                  break;
                } // case 50
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private proto4.Internal.IntList version_ = emptyIntList();

      private void ensureVersionIsMutable() {
        if (!version_.isModifiable()) {
          version_ = makeMutableCopy(version_);
        }
        bitField0_ |= 0x00000001;
      }

      /**
       * <code>repeated int32 version = 1 [packed = true];</code>
       *
       * @return A list containing the version.
       */
      public java.util.List<Integer> getVersionList() {
        version_.makeImmutable();
        return version_;
      }

      /**
       * <code>repeated int32 version = 1 [packed = true];</code>
       *
       * @return The count of version.
       */
      public int getVersionCount() {
        return version_.size();
      }

      /**
       * <code>repeated int32 version = 1 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The version at the given index.
       */
      public int getVersion(int index) {
        return version_.getInt(index);
      }

      /**
       * <code>repeated int32 version = 1 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The version to set.
       * @return This builder for chaining.
       */
      public Builder setVersion(int index, int value) {

        ensureVersionIsMutable();
        version_.setInt(index, value);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>repeated int32 version = 1 [packed = true];</code>
       *
       * @param value The version to add.
       * @return This builder for chaining.
       */
      public Builder addVersion(int value) {

        ensureVersionIsMutable();
        version_.addInt(value);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>repeated int32 version = 1 [packed = true];</code>
       *
       * @param values The version to add.
       * @return This builder for chaining.
       */
      public Builder addAllVersion(Iterable<? extends Integer> values) {
        ensureVersionIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, version_);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>repeated int32 version = 1 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearVersion() {
        version_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }

      private proto4.Internal.LongList timestamp_ = emptyLongList();

      private void ensureTimestampIsMutable() {
        if (!timestamp_.isModifiable()) {
          timestamp_ = makeMutableCopy(timestamp_);
        }
        bitField0_ |= 0x00000002;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 timestamp = 2 [packed = true];</code>
       *
       * @return A list containing the timestamp.
       */
      public java.util.List<Long> getTimestampList() {
        timestamp_.makeImmutable();
        return timestamp_;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 timestamp = 2 [packed = true];</code>
       *
       * @return The count of timestamp.
       */
      public int getTimestampCount() {
        return timestamp_.size();
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 timestamp = 2 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The timestamp at the given index.
       */
      public long getTimestamp(int index) {
        return timestamp_.getLong(index);
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 timestamp = 2 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The timestamp to set.
       * @return This builder for chaining.
       */
      public Builder setTimestamp(int index, long value) {

        ensureTimestampIsMutable();
        timestamp_.setLong(index, value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 timestamp = 2 [packed = true];</code>
       *
       * @param value The timestamp to add.
       * @return This builder for chaining.
       */
      public Builder addTimestamp(long value) {

        ensureTimestampIsMutable();
        timestamp_.addLong(value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 timestamp = 2 [packed = true];</code>
       *
       * @param values The timestamp to add.
       * @return This builder for chaining.
       */
      public Builder addAllTimestamp(Iterable<? extends Long> values) {
        ensureTimestampIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, timestamp_);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 timestamp = 2 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearTimestamp() {
        timestamp_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }

      private proto4.Internal.LongList changeset_ = emptyLongList();

      private void ensureChangesetIsMutable() {
        if (!changeset_.isModifiable()) {
          changeset_ = makeMutableCopy(changeset_);
        }
        bitField0_ |= 0x00000004;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 changeset = 3 [packed = true];</code>
       *
       * @return A list containing the changeset.
       */
      public java.util.List<Long> getChangesetList() {
        changeset_.makeImmutable();
        return changeset_;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 changeset = 3 [packed = true];</code>
       *
       * @return The count of changeset.
       */
      public int getChangesetCount() {
        return changeset_.size();
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 changeset = 3 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The changeset at the given index.
       */
      public long getChangeset(int index) {
        return changeset_.getLong(index);
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 changeset = 3 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The changeset to set.
       * @return This builder for chaining.
       */
      public Builder setChangeset(int index, long value) {

        ensureChangesetIsMutable();
        changeset_.setLong(index, value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 changeset = 3 [packed = true];</code>
       *
       * @param value The changeset to add.
       * @return This builder for chaining.
       */
      public Builder addChangeset(long value) {

        ensureChangesetIsMutable();
        changeset_.addLong(value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 changeset = 3 [packed = true];</code>
       *
       * @param values The changeset to add.
       * @return This builder for chaining.
       */
      public Builder addAllChangeset(Iterable<? extends Long> values) {
        ensureChangesetIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, changeset_);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 changeset = 3 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearChangeset() {
        changeset_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
        return this;
      }

      private proto4.Internal.IntList uid_ = emptyIntList();

      private void ensureUidIsMutable() {
        if (!uid_.isModifiable()) {
          uid_ = makeMutableCopy(uid_);
        }
        bitField0_ |= 0x00000008;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint32 uid = 4 [packed = true];</code>
       *
       * @return A list containing the uid.
       */
      public java.util.List<Integer> getUidList() {
        uid_.makeImmutable();
        return uid_;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint32 uid = 4 [packed = true];</code>
       *
       * @return The count of uid.
       */
      public int getUidCount() {
        return uid_.size();
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint32 uid = 4 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The uid at the given index.
       */
      public int getUid(int index) {
        return uid_.getInt(index);
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint32 uid = 4 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The uid to set.
       * @return This builder for chaining.
       */
      public Builder setUid(int index, int value) {

        ensureUidIsMutable();
        uid_.setInt(index, value);
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint32 uid = 4 [packed = true];</code>
       *
       * @param value The uid to add.
       * @return This builder for chaining.
       */
      public Builder addUid(int value) {

        ensureUidIsMutable();
        uid_.addInt(value);
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint32 uid = 4 [packed = true];</code>
       *
       * @param values The uid to add.
       * @return This builder for chaining.
       */
      public Builder addAllUid(Iterable<? extends Integer> values) {
        ensureUidIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, uid_);
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint32 uid = 4 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearUid() {
        uid_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000008);
        onChanged();
        return this;
      }

      private proto4.Internal.IntList userSid_ = emptyIntList();

      private void ensureUserSidIsMutable() {
        if (!userSid_.isModifiable()) {
          userSid_ = makeMutableCopy(userSid_);
        }
        bitField0_ |= 0x00000010;
      }

      /**
       *
       *
       * <pre>
       * String IDs for usernames. DELTA coded
       * </pre>
       *
       * <code>repeated sint32 user_sid = 5 [packed = true];</code>
       *
       * @return A list containing the userSid.
       */
      public java.util.List<Integer> getUserSidList() {
        userSid_.makeImmutable();
        return userSid_;
      }

      /**
       *
       *
       * <pre>
       * String IDs for usernames. DELTA coded
       * </pre>
       *
       * <code>repeated sint32 user_sid = 5 [packed = true];</code>
       *
       * @return The count of userSid.
       */
      public int getUserSidCount() {
        return userSid_.size();
      }

      /**
       *
       *
       * <pre>
       * String IDs for usernames. DELTA coded
       * </pre>
       *
       * <code>repeated sint32 user_sid = 5 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The userSid at the given index.
       */
      public int getUserSid(int index) {
        return userSid_.getInt(index);
      }

      /**
       *
       *
       * <pre>
       * String IDs for usernames. DELTA coded
       * </pre>
       *
       * <code>repeated sint32 user_sid = 5 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The userSid to set.
       * @return This builder for chaining.
       */
      public Builder setUserSid(int index, int value) {

        ensureUserSidIsMutable();
        userSid_.setInt(index, value);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * String IDs for usernames. DELTA coded
       * </pre>
       *
       * <code>repeated sint32 user_sid = 5 [packed = true];</code>
       *
       * @param value The userSid to add.
       * @return This builder for chaining.
       */
      public Builder addUserSid(int value) {

        ensureUserSidIsMutable();
        userSid_.addInt(value);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * String IDs for usernames. DELTA coded
       * </pre>
       *
       * <code>repeated sint32 user_sid = 5 [packed = true];</code>
       *
       * @param values The userSid to add.
       * @return This builder for chaining.
       */
      public Builder addAllUserSid(Iterable<? extends Integer> values) {
        ensureUserSidIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, userSid_);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * String IDs for usernames. DELTA coded
       * </pre>
       *
       * <code>repeated sint32 user_sid = 5 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearUserSid() {
        userSid_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000010);
        onChanged();
        return this;
      }

      private proto4.Internal.BooleanList visible_ = emptyBooleanList();

      private void ensureVisibleIsMutable() {
        if (!visible_.isModifiable()) {
          visible_ = makeMutableCopy(visible_);
        }
        bitField0_ |= 0x00000020;
      }

      private void ensureVisibleIsMutable(int capacity) {
        if (!visible_.isModifiable()) {
          visible_ = makeMutableCopy(visible_, capacity);
        }
        bitField0_ |= 0x00000020;
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>repeated bool visible = 6 [packed = true];</code>
       *
       * @return A list containing the visible.
       */
      public java.util.List<Boolean> getVisibleList() {
        visible_.makeImmutable();
        return visible_;
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>repeated bool visible = 6 [packed = true];</code>
       *
       * @return The count of visible.
       */
      public int getVisibleCount() {
        return visible_.size();
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>repeated bool visible = 6 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The visible at the given index.
       */
      public boolean getVisible(int index) {
        return visible_.getBoolean(index);
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>repeated bool visible = 6 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The visible to set.
       * @return This builder for chaining.
       */
      public Builder setVisible(int index, boolean value) {

        ensureVisibleIsMutable();
        visible_.setBoolean(index, value);
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>repeated bool visible = 6 [packed = true];</code>
       *
       * @param value The visible to add.
       * @return This builder for chaining.
       */
      public Builder addVisible(boolean value) {

        ensureVisibleIsMutable();
        visible_.addBoolean(value);
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>repeated bool visible = 6 [packed = true];</code>
       *
       * @param values The visible to add.
       * @return This builder for chaining.
       */
      public Builder addAllVisible(Iterable<? extends Boolean> values) {
        ensureVisibleIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, visible_);
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * The visible flag is used to store history information. It indicates that
       * the current object version has been created by a delete operation on the
       * OSM API.
       * When a writer sets this flag, it MUST add a required_features tag with
       * value "HistoricalInformation" to the HeaderBlock.
       * If this flag is not available for some object it MUST be assumed to be
       * true if the file has the required_features tag "HistoricalInformation"
       * set.
       * </pre>
       *
       * <code>repeated bool visible = 6 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearVisible() {
        visible_ = emptyBooleanList();
        bitField0_ = (bitField0_ & ~0x00000020);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:DenseInfo)
    }

    // @@protoc_insertion_point(class_scope:DenseInfo)
    private static final Osmformat.DenseInfo DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.DenseInfo();
    }

    public static Osmformat.DenseInfo getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<DenseInfo> PARSER =
        new proto4.AbstractParser<DenseInfo>() {
          @Override
          public DenseInfo parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<DenseInfo> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<DenseInfo> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.DenseInfo getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface ChangeSetOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:ChangeSet)
      proto4.MessageOrBuilder {

    /**
     *
     *
     * <pre>
     *
     * // Parallel arrays.
     * repeated uint32 keys = 2 [packed = true]; // String IDs.
     * repeated uint32 vals = 3 [packed = true]; // String IDs.
     *
     * optional Info info = 4;
     * </pre>
     *
     * <code>required int64 id = 1;</code>
     *
     * @return Whether the id field is set.
     */
    boolean hasId();

    /**
     *
     *
     * <pre>
     *
     * // Parallel arrays.
     * repeated uint32 keys = 2 [packed = true]; // String IDs.
     * repeated uint32 vals = 3 [packed = true]; // String IDs.
     *
     * optional Info info = 4;
     * </pre>
     *
     * <code>required int64 id = 1;</code>
     *
     * @return The id.
     */
    long getId();
  }

  /**
   *
   *
   * <pre>
   * THIS IS STUB DESIGN FOR CHANGESETS. NOT USED RIGHT NOW.
   * TODO:    REMOVE THIS?
   * </pre>
   *
   * <p>Protobuf type {@code ChangeSet}
   */
  public static final class ChangeSet extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:ChangeSet)
      ChangeSetOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          ChangeSet.class.getName());
    }

    // Use ChangeSet.newBuilder() to construct.
    private ChangeSet(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private ChangeSet() {}

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_ChangeSet_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_ChangeSet_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.ChangeSet.class, Osmformat.ChangeSet.Builder.class);
    }

    private int bitField0_;
    public static final int ID_FIELD_NUMBER = 1;
    private long id_ = 0L;

    /**
     *
     *
     * <pre>
     *
     * // Parallel arrays.
     * repeated uint32 keys = 2 [packed = true]; // String IDs.
     * repeated uint32 vals = 3 [packed = true]; // String IDs.
     *
     * optional Info info = 4;
     * </pre>
     *
     * <code>required int64 id = 1;</code>
     *
     * @return Whether the id field is set.
     */
    @Override
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     *
     *
     * <pre>
     *
     * // Parallel arrays.
     * repeated uint32 keys = 2 [packed = true]; // String IDs.
     * repeated uint32 vals = 3 [packed = true]; // String IDs.
     *
     * optional Info info = 4;
     * </pre>
     *
     * <code>required int64 id = 1;</code>
     *
     * @return The id.
     */
    @Override
    public long getId() {
      return id_;
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt64(1, id_);
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(1, id_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.ChangeSet)) {
        return super.equals(obj);
      }
      Osmformat.ChangeSet other = (Osmformat.ChangeSet) obj;

      if (hasId() != other.hasId()) return false;
      if (hasId()) {
        if (getId() != other.getId()) return false;
      }
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasId()) {
        hash = (37 * hash) + ID_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getId());
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.ChangeSet parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.ChangeSet parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.ChangeSet parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.ChangeSet parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.ChangeSet parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.ChangeSet parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.ChangeSet parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.ChangeSet parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.ChangeSet parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.ChangeSet parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.ChangeSet parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.ChangeSet parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.ChangeSet prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /**
     *
     *
     * <pre>
     * THIS IS STUB DESIGN FOR CHANGESETS. NOT USED RIGHT NOW.
     * TODO:    REMOVE THIS?
     * </pre>
     *
     * <p>Protobuf type {@code ChangeSet}
     */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:ChangeSet)
        Osmformat.ChangeSetOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_ChangeSet_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_ChangeSet_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.ChangeSet.class, Osmformat.ChangeSet.Builder.class);
      }

      // Construct using Osmformat.ChangeSet.newBuilder()
      private Builder() {}

      private Builder(BuilderParent parent) {
        super(parent);
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = 0L;
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_ChangeSet_descriptor;
      }

      @Override
      public Osmformat.ChangeSet getDefaultInstanceForType() {
        return Osmformat.ChangeSet.getDefaultInstance();
      }

      @Override
      public Osmformat.ChangeSet build() {
        Osmformat.ChangeSet result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.ChangeSet buildPartial() {
        Osmformat.ChangeSet result = new Osmformat.ChangeSet(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.ChangeSet result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
          to_bitField0_ |= 0x00000001;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.ChangeSet) {
          return mergeFrom((Osmformat.ChangeSet) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.ChangeSet other) {
        if (other == Osmformat.ChangeSet.getDefaultInstance()) return this;
        if (other.hasId()) {
          setId(other.getId());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        if (!hasId()) {
          return false;
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8:
                {
                  id_ = input.readInt64();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 8
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private long id_;

      /**
       *
       *
       * <pre>
       *
       * // Parallel arrays.
       * repeated uint32 keys = 2 [packed = true]; // String IDs.
       * repeated uint32 vals = 3 [packed = true]; // String IDs.
       *
       * optional Info info = 4;
       * </pre>
       *
       * <code>required int64 id = 1;</code>
       *
       * @return Whether the id field is set.
       */
      @Override
      public boolean hasId() {
        return ((bitField0_ & 0x00000001) != 0);
      }

      /**
       *
       *
       * <pre>
       *
       * // Parallel arrays.
       * repeated uint32 keys = 2 [packed = true]; // String IDs.
       * repeated uint32 vals = 3 [packed = true]; // String IDs.
       *
       * optional Info info = 4;
       * </pre>
       *
       * <code>required int64 id = 1;</code>
       *
       * @return The id.
       */
      @Override
      public long getId() {
        return id_;
      }

      /**
       *
       *
       * <pre>
       *
       * // Parallel arrays.
       * repeated uint32 keys = 2 [packed = true]; // String IDs.
       * repeated uint32 vals = 3 [packed = true]; // String IDs.
       *
       * optional Info info = 4;
       * </pre>
       *
       * <code>required int64 id = 1;</code>
       *
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(long value) {

        id_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       *
       * // Parallel arrays.
       * repeated uint32 keys = 2 [packed = true]; // String IDs.
       * repeated uint32 vals = 3 [packed = true]; // String IDs.
       *
       * optional Info info = 4;
       * </pre>
       *
       * <code>required int64 id = 1;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:ChangeSet)
    }

    // @@protoc_insertion_point(class_scope:ChangeSet)
    private static final Osmformat.ChangeSet DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.ChangeSet();
    }

    public static Osmformat.ChangeSet getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<ChangeSet> PARSER =
        new proto4.AbstractParser<ChangeSet>() {
          @Override
          public ChangeSet parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<ChangeSet> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<ChangeSet> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.ChangeSet getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface NodeOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:Node)
      proto4.MessageOrBuilder {

    /**
     * <code>required sint64 id = 1;</code>
     *
     * @return Whether the id field is set.
     */
    boolean hasId();

    /**
     * <code>required sint64 id = 1;</code>
     *
     * @return The id.
     */
    long getId();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return A list containing the keys.
     */
    java.util.List<Integer> getKeysList();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return The count of keys.
     */
    int getKeysCount();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The keys at the given index.
     */
    int getKeys(int index);

    /**
     *
     *
     * <pre>
     * String IDs.
     * </pre>
     *
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return A list containing the vals.
     */
    java.util.List<Integer> getValsList();

    /**
     *
     *
     * <pre>
     * String IDs.
     * </pre>
     *
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return The count of vals.
     */
    int getValsCount();

    /**
     *
     *
     * <pre>
     * String IDs.
     * </pre>
     *
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The vals at the given index.
     */
    int getVals(int index);

    /**
     *
     *
     * <pre>
     * May be omitted in omitmeta
     * </pre>
     *
     * <code>optional .Info info = 4;</code>
     *
     * @return Whether the info field is set.
     */
    boolean hasInfo();

    /**
     *
     *
     * <pre>
     * May be omitted in omitmeta
     * </pre>
     *
     * <code>optional .Info info = 4;</code>
     *
     * @return The info.
     */
    Osmformat.Info getInfo();

    /**
     *
     *
     * <pre>
     * May be omitted in omitmeta
     * </pre>
     *
     * <code>optional .Info info = 4;</code>
     */
    Osmformat.InfoOrBuilder getInfoOrBuilder();

    /**
     * <code>required sint64 lat = 8;</code>
     *
     * @return Whether the lat field is set.
     */
    boolean hasLat();

    /**
     * <code>required sint64 lat = 8;</code>
     *
     * @return The lat.
     */
    long getLat();

    /**
     * <code>required sint64 lon = 9;</code>
     *
     * @return Whether the lon field is set.
     */
    boolean hasLon();

    /**
     * <code>required sint64 lon = 9;</code>
     *
     * @return The lon.
     */
    long getLon();
  }

  /** Protobuf type {@code Node} */
  public static final class Node extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:Node)
      NodeOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          Node.class.getName());
    }

    // Use Node.newBuilder() to construct.
    private Node(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private Node() {
      keys_ = emptyIntList();
      vals_ = emptyIntList();
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_Node_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_Node_fieldAccessorTable
          .ensureFieldAccessorsInitialized(Osmformat.Node.class, Osmformat.Node.Builder.class);
    }

    private int bitField0_;
    public static final int ID_FIELD_NUMBER = 1;
    private long id_ = 0L;

    /**
     * <code>required sint64 id = 1;</code>
     *
     * @return Whether the id field is set.
     */
    @Override
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     * <code>required sint64 id = 1;</code>
     *
     * @return The id.
     */
    @Override
    public long getId() {
      return id_;
    }

    public static final int KEYS_FIELD_NUMBER = 2;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList keys_ = emptyIntList();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return A list containing the keys.
     */
    @Override
    public java.util.List<Integer> getKeysList() {
      return keys_;
    }

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return The count of keys.
     */
    public int getKeysCount() {
      return keys_.size();
    }

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The keys at the given index.
     */
    public int getKeys(int index) {
      return keys_.getInt(index);
    }

    private int keysMemoizedSerializedSize = -1;

    public static final int VALS_FIELD_NUMBER = 3;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList vals_ = emptyIntList();

    /**
     *
     *
     * <pre>
     * String IDs.
     * </pre>
     *
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return A list containing the vals.
     */
    @Override
    public java.util.List<Integer> getValsList() {
      return vals_;
    }

    /**
     *
     *
     * <pre>
     * String IDs.
     * </pre>
     *
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return The count of vals.
     */
    public int getValsCount() {
      return vals_.size();
    }

    /**
     *
     *
     * <pre>
     * String IDs.
     * </pre>
     *
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The vals at the given index.
     */
    public int getVals(int index) {
      return vals_.getInt(index);
    }

    private int valsMemoizedSerializedSize = -1;

    public static final int INFO_FIELD_NUMBER = 4;
    private Osmformat.Info info_;

    /**
     *
     *
     * <pre>
     * May be omitted in omitmeta
     * </pre>
     *
     * <code>optional .Info info = 4;</code>
     *
     * @return Whether the info field is set.
     */
    @Override
    public boolean hasInfo() {
      return ((bitField0_ & 0x00000002) != 0);
    }

    /**
     *
     *
     * <pre>
     * May be omitted in omitmeta
     * </pre>
     *
     * <code>optional .Info info = 4;</code>
     *
     * @return The info.
     */
    @Override
    public Osmformat.Info getInfo() {
      return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
    }

    /**
     *
     *
     * <pre>
     * May be omitted in omitmeta
     * </pre>
     *
     * <code>optional .Info info = 4;</code>
     */
    @Override
    public Osmformat.InfoOrBuilder getInfoOrBuilder() {
      return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
    }

    public static final int LAT_FIELD_NUMBER = 8;
    private long lat_ = 0L;

    /**
     * <code>required sint64 lat = 8;</code>
     *
     * @return Whether the lat field is set.
     */
    @Override
    public boolean hasLat() {
      return ((bitField0_ & 0x00000004) != 0);
    }

    /**
     * <code>required sint64 lat = 8;</code>
     *
     * @return The lat.
     */
    @Override
    public long getLat() {
      return lat_;
    }

    public static final int LON_FIELD_NUMBER = 9;
    private long lon_ = 0L;

    /**
     * <code>required sint64 lon = 9;</code>
     *
     * @return Whether the lon field is set.
     */
    @Override
    public boolean hasLon() {
      return ((bitField0_ & 0x00000008) != 0);
    }

    /**
     * <code>required sint64 lon = 9;</code>
     *
     * @return The lon.
     */
    @Override
    public long getLon() {
      return lon_;
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasLat()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasLon()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeSInt64(1, id_);
      }
      if (getKeysList().size() > 0) {
        output.writeUInt32NoTag(18);
        output.writeUInt32NoTag(keysMemoizedSerializedSize);
      }
      for (int i = 0; i < keys_.size(); i++) {
        output.writeUInt32NoTag(keys_.getInt(i));
      }
      if (getValsList().size() > 0) {
        output.writeUInt32NoTag(26);
        output.writeUInt32NoTag(valsMemoizedSerializedSize);
      }
      for (int i = 0; i < vals_.size(); i++) {
        output.writeUInt32NoTag(vals_.getInt(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(4, getInfo());
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeSInt64(8, lat_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeSInt64(9, lon_);
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeSInt64Size(1, id_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < keys_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeUInt32SizeNoTag(keys_.getInt(i));
        }
        size += dataSize;
        if (!getKeysList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        keysMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < vals_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeUInt32SizeNoTag(vals_.getInt(i));
        }
        size += dataSize;
        if (!getValsList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        valsMemoizedSerializedSize = dataSize;
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += proto4.CodedOutputStream.computeMessageSize(4, getInfo());
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += proto4.CodedOutputStream.computeSInt64Size(8, lat_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += proto4.CodedOutputStream.computeSInt64Size(9, lon_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.Node)) {
        return super.equals(obj);
      }
      Osmformat.Node other = (Osmformat.Node) obj;

      if (hasId() != other.hasId()) return false;
      if (hasId()) {
        if (getId() != other.getId()) return false;
      }
      if (!getKeysList().equals(other.getKeysList())) return false;
      if (!getValsList().equals(other.getValsList())) return false;
      if (hasInfo() != other.hasInfo()) return false;
      if (hasInfo()) {
        if (!getInfo().equals(other.getInfo())) return false;
      }
      if (hasLat() != other.hasLat()) return false;
      if (hasLat()) {
        if (getLat() != other.getLat()) return false;
      }
      if (hasLon() != other.hasLon()) return false;
      if (hasLon()) {
        if (getLon() != other.getLon()) return false;
      }
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasId()) {
        hash = (37 * hash) + ID_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getId());
      }
      if (getKeysCount() > 0) {
        hash = (37 * hash) + KEYS_FIELD_NUMBER;
        hash = (53 * hash) + getKeysList().hashCode();
      }
      if (getValsCount() > 0) {
        hash = (37 * hash) + VALS_FIELD_NUMBER;
        hash = (53 * hash) + getValsList().hashCode();
      }
      if (hasInfo()) {
        hash = (37 * hash) + INFO_FIELD_NUMBER;
        hash = (53 * hash) + getInfo().hashCode();
      }
      if (hasLat()) {
        hash = (37 * hash) + LAT_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getLat());
      }
      if (hasLon()) {
        hash = (37 * hash) + LON_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getLon());
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.Node parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Node parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Node parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Node parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Node parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Node parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Node parseFrom(java.io.InputStream input) throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.Node parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.Node parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.Node parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.Node parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.Node parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.Node prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /** Protobuf type {@code Node} */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:Node)
        Osmformat.NodeOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Node_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Node_fieldAccessorTable
            .ensureFieldAccessorsInitialized(Osmformat.Node.class, Osmformat.Node.Builder.class);
      }

      // Construct using Osmformat.Node.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }

      private void maybeForceBuilderInitialization() {
        if (proto4.GeneratedMessage.alwaysUseFieldBuilders) {
          getInfoFieldBuilder();
        }
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = 0L;
        keys_ = emptyIntList();
        vals_ = emptyIntList();
        info_ = null;
        if (infoBuilder_ != null) {
          infoBuilder_.dispose();
          infoBuilder_ = null;
        }
        lat_ = 0L;
        lon_ = 0L;
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Node_descriptor;
      }

      @Override
      public Osmformat.Node getDefaultInstanceForType() {
        return Osmformat.Node.getDefaultInstance();
      }

      @Override
      public Osmformat.Node build() {
        Osmformat.Node result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.Node buildPartial() {
        Osmformat.Node result = new Osmformat.Node(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.Node result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          keys_.makeImmutable();
          result.keys_ = keys_;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          vals_.makeImmutable();
          result.vals_ = vals_;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.info_ = infoBuilder_ == null ? info_ : infoBuilder_.build();
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.lat_ = lat_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          result.lon_ = lon_;
          to_bitField0_ |= 0x00000008;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.Node) {
          return mergeFrom((Osmformat.Node) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.Node other) {
        if (other == Osmformat.Node.getDefaultInstance()) return this;
        if (other.hasId()) {
          setId(other.getId());
        }
        if (!other.keys_.isEmpty()) {
          if (keys_.isEmpty()) {
            keys_ = other.keys_;
            keys_.makeImmutable();
            bitField0_ |= 0x00000002;
          } else {
            ensureKeysIsMutable();
            keys_.addAll(other.keys_);
          }
          onChanged();
        }
        if (!other.vals_.isEmpty()) {
          if (vals_.isEmpty()) {
            vals_ = other.vals_;
            vals_.makeImmutable();
            bitField0_ |= 0x00000004;
          } else {
            ensureValsIsMutable();
            vals_.addAll(other.vals_);
          }
          onChanged();
        }
        if (other.hasInfo()) {
          mergeInfo(other.getInfo());
        }
        if (other.hasLat()) {
          setLat(other.getLat());
        }
        if (other.hasLon()) {
          setLon(other.getLon());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        if (!hasId()) {
          return false;
        }
        if (!hasLat()) {
          return false;
        }
        if (!hasLon()) {
          return false;
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8:
                {
                  id_ = input.readSInt64();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 8
              case 16:
                {
                  int v = input.readUInt32();
                  ensureKeysIsMutable();
                  keys_.addInt(v);
                  break;
                } // case 16
              case 18:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureKeysIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    keys_.addInt(input.readUInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 18
              case 24:
                {
                  int v = input.readUInt32();
                  ensureValsIsMutable();
                  vals_.addInt(v);
                  break;
                } // case 24
              case 26:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureValsIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    vals_.addInt(input.readUInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 26
              case 34:
                {
                  input.readMessage(getInfoFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000008;
                  break;
                } // case 34
              case 64:
                {
                  lat_ = input.readSInt64();
                  bitField0_ |= 0x00000010;
                  break;
                } // case 64
              case 72:
                {
                  lon_ = input.readSInt64();
                  bitField0_ |= 0x00000020;
                  break;
                } // case 72
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private long id_;

      /**
       * <code>required sint64 id = 1;</code>
       *
       * @return Whether the id field is set.
       */
      @Override
      public boolean hasId() {
        return ((bitField0_ & 0x00000001) != 0);
      }

      /**
       * <code>required sint64 id = 1;</code>
       *
       * @return The id.
       */
      @Override
      public long getId() {
        return id_;
      }

      /**
       * <code>required sint64 id = 1;</code>
       *
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(long value) {

        id_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>required sint64 id = 1;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = 0L;
        onChanged();
        return this;
      }

      private proto4.Internal.IntList keys_ = emptyIntList();

      private void ensureKeysIsMutable() {
        if (!keys_.isModifiable()) {
          keys_ = makeMutableCopy(keys_);
        }
        bitField0_ |= 0x00000002;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return A list containing the keys.
       */
      public java.util.List<Integer> getKeysList() {
        keys_.makeImmutable();
        return keys_;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return The count of keys.
       */
      public int getKeysCount() {
        return keys_.size();
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The keys at the given index.
       */
      public int getKeys(int index) {
        return keys_.getInt(index);
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The keys to set.
       * @return This builder for chaining.
       */
      public Builder setKeys(int index, int value) {

        ensureKeysIsMutable();
        keys_.setInt(index, value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param value The keys to add.
       * @return This builder for chaining.
       */
      public Builder addKeys(int value) {

        ensureKeysIsMutable();
        keys_.addInt(value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param values The keys to add.
       * @return This builder for chaining.
       */
      public Builder addAllKeys(Iterable<? extends Integer> values) {
        ensureKeysIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, keys_);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearKeys() {
        keys_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }

      private proto4.Internal.IntList vals_ = emptyIntList();

      private void ensureValsIsMutable() {
        if (!vals_.isModifiable()) {
          vals_ = makeMutableCopy(vals_);
        }
        bitField0_ |= 0x00000004;
      }

      /**
       *
       *
       * <pre>
       * String IDs.
       * </pre>
       *
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return A list containing the vals.
       */
      public java.util.List<Integer> getValsList() {
        vals_.makeImmutable();
        return vals_;
      }

      /**
       *
       *
       * <pre>
       * String IDs.
       * </pre>
       *
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return The count of vals.
       */
      public int getValsCount() {
        return vals_.size();
      }

      /**
       *
       *
       * <pre>
       * String IDs.
       * </pre>
       *
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The vals at the given index.
       */
      public int getVals(int index) {
        return vals_.getInt(index);
      }

      /**
       *
       *
       * <pre>
       * String IDs.
       * </pre>
       *
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The vals to set.
       * @return This builder for chaining.
       */
      public Builder setVals(int index, int value) {

        ensureValsIsMutable();
        vals_.setInt(index, value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * String IDs.
       * </pre>
       *
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param value The vals to add.
       * @return This builder for chaining.
       */
      public Builder addVals(int value) {

        ensureValsIsMutable();
        vals_.addInt(value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * String IDs.
       * </pre>
       *
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param values The vals to add.
       * @return This builder for chaining.
       */
      public Builder addAllVals(Iterable<? extends Integer> values) {
        ensureValsIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, vals_);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * String IDs.
       * </pre>
       *
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearVals() {
        vals_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
        return this;
      }

      private Osmformat.Info info_;
      private proto4.SingleFieldBuilder<
              Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>
          infoBuilder_;

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       *
       * @return Whether the info field is set.
       */
      public boolean hasInfo() {
        return ((bitField0_ & 0x00000008) != 0);
      }

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       *
       * @return The info.
       */
      public Osmformat.Info getInfo() {
        if (infoBuilder_ == null) {
          return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
        } else {
          return infoBuilder_.getMessage();
        }
      }

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       */
      public Builder setInfo(Osmformat.Info value) {
        if (infoBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          info_ = value;
        } else {
          infoBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       */
      public Builder setInfo(Osmformat.Info.Builder builderForValue) {
        if (infoBuilder_ == null) {
          info_ = builderForValue.build();
        } else {
          infoBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       */
      public Builder mergeInfo(Osmformat.Info value) {
        if (infoBuilder_ == null) {
          if (((bitField0_ & 0x00000008) != 0)
              && info_ != null
              && info_ != Osmformat.Info.getDefaultInstance()) {
            getInfoBuilder().mergeFrom(value);
          } else {
            info_ = value;
          }
        } else {
          infoBuilder_.mergeFrom(value);
        }
        if (info_ != null) {
          bitField0_ |= 0x00000008;
          onChanged();
        }
        return this;
      }

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       */
      public Builder clearInfo() {
        bitField0_ = (bitField0_ & ~0x00000008);
        info_ = null;
        if (infoBuilder_ != null) {
          infoBuilder_.dispose();
          infoBuilder_ = null;
        }
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       */
      public Osmformat.Info.Builder getInfoBuilder() {
        bitField0_ |= 0x00000008;
        onChanged();
        return getInfoFieldBuilder().getBuilder();
      }

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       */
      public Osmformat.InfoOrBuilder getInfoOrBuilder() {
        if (infoBuilder_ != null) {
          return infoBuilder_.getMessageOrBuilder();
        } else {
          return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
        }
      }

      /**
       *
       *
       * <pre>
       * May be omitted in omitmeta
       * </pre>
       *
       * <code>optional .Info info = 4;</code>
       */
      private proto4.SingleFieldBuilder<
              Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>
          getInfoFieldBuilder() {
        if (infoBuilder_ == null) {
          infoBuilder_ =
              new proto4.SingleFieldBuilder<
                  Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>(
                  getInfo(), getParentForChildren(), isClean());
          info_ = null;
        }
        return infoBuilder_;
      }

      private long lat_;

      /**
       * <code>required sint64 lat = 8;</code>
       *
       * @return Whether the lat field is set.
       */
      @Override
      public boolean hasLat() {
        return ((bitField0_ & 0x00000010) != 0);
      }

      /**
       * <code>required sint64 lat = 8;</code>
       *
       * @return The lat.
       */
      @Override
      public long getLat() {
        return lat_;
      }

      /**
       * <code>required sint64 lat = 8;</code>
       *
       * @param value The lat to set.
       * @return This builder for chaining.
       */
      public Builder setLat(long value) {

        lat_ = value;
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       * <code>required sint64 lat = 8;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearLat() {
        bitField0_ = (bitField0_ & ~0x00000010);
        lat_ = 0L;
        onChanged();
        return this;
      }

      private long lon_;

      /**
       * <code>required sint64 lon = 9;</code>
       *
       * @return Whether the lon field is set.
       */
      @Override
      public boolean hasLon() {
        return ((bitField0_ & 0x00000020) != 0);
      }

      /**
       * <code>required sint64 lon = 9;</code>
       *
       * @return The lon.
       */
      @Override
      public long getLon() {
        return lon_;
      }

      /**
       * <code>required sint64 lon = 9;</code>
       *
       * @param value The lon to set.
       * @return This builder for chaining.
       */
      public Builder setLon(long value) {

        lon_ = value;
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       * <code>required sint64 lon = 9;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearLon() {
        bitField0_ = (bitField0_ & ~0x00000020);
        lon_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:Node)
    }

    // @@protoc_insertion_point(class_scope:Node)
    private static final Osmformat.Node DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.Node();
    }

    public static Osmformat.Node getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<Node> PARSER =
        new proto4.AbstractParser<Node>() {
          @Override
          public Node parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<Node> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<Node> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.Node getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface DenseNodesOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:DenseNodes)
      proto4.MessageOrBuilder {

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 id = 1 [packed = true];</code>
     *
     * @return A list containing the id.
     */
    java.util.List<Long> getIdList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 id = 1 [packed = true];</code>
     *
     * @return The count of id.
     */
    int getIdCount();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 id = 1 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The id at the given index.
     */
    long getId(int index);

    /**
     *
     *
     * <pre>
     * repeated Info info = 4;
     * </pre>
     *
     * <code>optional .DenseInfo denseinfo = 5;</code>
     *
     * @return Whether the denseinfo field is set.
     */
    boolean hasDenseinfo();

    /**
     *
     *
     * <pre>
     * repeated Info info = 4;
     * </pre>
     *
     * <code>optional .DenseInfo denseinfo = 5;</code>
     *
     * @return The denseinfo.
     */
    Osmformat.DenseInfo getDenseinfo();

    /**
     *
     *
     * <pre>
     * repeated Info info = 4;
     * </pre>
     *
     * <code>optional .DenseInfo denseinfo = 5;</code>
     */
    Osmformat.DenseInfoOrBuilder getDenseinfoOrBuilder();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lat = 8 [packed = true];</code>
     *
     * @return A list containing the lat.
     */
    java.util.List<Long> getLatList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lat = 8 [packed = true];</code>
     *
     * @return The count of lat.
     */
    int getLatCount();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lat = 8 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The lat at the given index.
     */
    long getLat(int index);

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lon = 9 [packed = true];</code>
     *
     * @return A list containing the lon.
     */
    java.util.List<Long> getLonList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lon = 9 [packed = true];</code>
     *
     * @return The count of lon.
     */
    int getLonCount();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lon = 9 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The lon at the given index.
     */
    long getLon(int index);

    /**
     *
     *
     * <pre>
     * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
     * </pre>
     *
     * <code>repeated int32 keys_vals = 10 [packed = true];</code>
     *
     * @return A list containing the keysVals.
     */
    java.util.List<Integer> getKeysValsList();

    /**
     *
     *
     * <pre>
     * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
     * </pre>
     *
     * <code>repeated int32 keys_vals = 10 [packed = true];</code>
     *
     * @return The count of keysVals.
     */
    int getKeysValsCount();

    /**
     *
     *
     * <pre>
     * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
     * </pre>
     *
     * <code>repeated int32 keys_vals = 10 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The keysVals at the given index.
     */
    int getKeysVals(int index);
  }

  /** Protobuf type {@code DenseNodes} */
  public static final class DenseNodes extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:DenseNodes)
      DenseNodesOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          DenseNodes.class.getName());
    }

    // Use DenseNodes.newBuilder() to construct.
    private DenseNodes(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private DenseNodes() {
      id_ = emptyLongList();
      lat_ = emptyLongList();
      lon_ = emptyLongList();
      keysVals_ = emptyIntList();
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_DenseNodes_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_DenseNodes_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.DenseNodes.class, Osmformat.DenseNodes.Builder.class);
    }

    private int bitField0_;
    public static final int ID_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private proto4.Internal.LongList id_ = emptyLongList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 id = 1 [packed = true];</code>
     *
     * @return A list containing the id.
     */
    @Override
    public java.util.List<Long> getIdList() {
      return id_;
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 id = 1 [packed = true];</code>
     *
     * @return The count of id.
     */
    public int getIdCount() {
      return id_.size();
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 id = 1 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The id at the given index.
     */
    public long getId(int index) {
      return id_.getLong(index);
    }

    private int idMemoizedSerializedSize = -1;

    public static final int DENSEINFO_FIELD_NUMBER = 5;
    private Osmformat.DenseInfo denseinfo_;

    /**
     *
     *
     * <pre>
     * repeated Info info = 4;
     * </pre>
     *
     * <code>optional .DenseInfo denseinfo = 5;</code>
     *
     * @return Whether the denseinfo field is set.
     */
    @Override
    public boolean hasDenseinfo() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     *
     *
     * <pre>
     * repeated Info info = 4;
     * </pre>
     *
     * <code>optional .DenseInfo denseinfo = 5;</code>
     *
     * @return The denseinfo.
     */
    @Override
    public Osmformat.DenseInfo getDenseinfo() {
      return denseinfo_ == null ? Osmformat.DenseInfo.getDefaultInstance() : denseinfo_;
    }

    /**
     *
     *
     * <pre>
     * repeated Info info = 4;
     * </pre>
     *
     * <code>optional .DenseInfo denseinfo = 5;</code>
     */
    @Override
    public Osmformat.DenseInfoOrBuilder getDenseinfoOrBuilder() {
      return denseinfo_ == null ? Osmformat.DenseInfo.getDefaultInstance() : denseinfo_;
    }

    public static final int LAT_FIELD_NUMBER = 8;

    @SuppressWarnings("serial")
    private proto4.Internal.LongList lat_ = emptyLongList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lat = 8 [packed = true];</code>
     *
     * @return A list containing the lat.
     */
    @Override
    public java.util.List<Long> getLatList() {
      return lat_;
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lat = 8 [packed = true];</code>
     *
     * @return The count of lat.
     */
    public int getLatCount() {
      return lat_.size();
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lat = 8 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The lat at the given index.
     */
    public long getLat(int index) {
      return lat_.getLong(index);
    }

    private int latMemoizedSerializedSize = -1;

    public static final int LON_FIELD_NUMBER = 9;

    @SuppressWarnings("serial")
    private proto4.Internal.LongList lon_ = emptyLongList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lon = 9 [packed = true];</code>
     *
     * @return A list containing the lon.
     */
    @Override
    public java.util.List<Long> getLonList() {
      return lon_;
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lon = 9 [packed = true];</code>
     *
     * @return The count of lon.
     */
    public int getLonCount() {
      return lon_.size();
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 lon = 9 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The lon at the given index.
     */
    public long getLon(int index) {
      return lon_.getLong(index);
    }

    private int lonMemoizedSerializedSize = -1;

    public static final int KEYS_VALS_FIELD_NUMBER = 10;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList keysVals_ = emptyIntList();

    /**
     *
     *
     * <pre>
     * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
     * </pre>
     *
     * <code>repeated int32 keys_vals = 10 [packed = true];</code>
     *
     * @return A list containing the keysVals.
     */
    @Override
    public java.util.List<Integer> getKeysValsList() {
      return keysVals_;
    }

    /**
     *
     *
     * <pre>
     * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
     * </pre>
     *
     * <code>repeated int32 keys_vals = 10 [packed = true];</code>
     *
     * @return The count of keysVals.
     */
    public int getKeysValsCount() {
      return keysVals_.size();
    }

    /**
     *
     *
     * <pre>
     * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
     * </pre>
     *
     * <code>repeated int32 keys_vals = 10 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The keysVals at the given index.
     */
    public int getKeysVals(int index) {
      return keysVals_.getInt(index);
    }

    private int keysValsMemoizedSerializedSize = -1;

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      getSerializedSize();
      if (getIdList().size() > 0) {
        output.writeUInt32NoTag(10);
        output.writeUInt32NoTag(idMemoizedSerializedSize);
      }
      for (int i = 0; i < id_.size(); i++) {
        output.writeSInt64NoTag(id_.getLong(i));
      }
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(5, getDenseinfo());
      }
      if (getLatList().size() > 0) {
        output.writeUInt32NoTag(66);
        output.writeUInt32NoTag(latMemoizedSerializedSize);
      }
      for (int i = 0; i < lat_.size(); i++) {
        output.writeSInt64NoTag(lat_.getLong(i));
      }
      if (getLonList().size() > 0) {
        output.writeUInt32NoTag(74);
        output.writeUInt32NoTag(lonMemoizedSerializedSize);
      }
      for (int i = 0; i < lon_.size(); i++) {
        output.writeSInt64NoTag(lon_.getLong(i));
      }
      if (getKeysValsList().size() > 0) {
        output.writeUInt32NoTag(82);
        output.writeUInt32NoTag(keysValsMemoizedSerializedSize);
      }
      for (int i = 0; i < keysVals_.size(); i++) {
        output.writeInt32NoTag(keysVals_.getInt(i));
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      {
        int dataSize = 0;
        for (int i = 0; i < id_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt64SizeNoTag(id_.getLong(i));
        }
        size += dataSize;
        if (!getIdList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        idMemoizedSerializedSize = dataSize;
      }
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeMessageSize(5, getDenseinfo());
      }
      {
        int dataSize = 0;
        for (int i = 0; i < lat_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt64SizeNoTag(lat_.getLong(i));
        }
        size += dataSize;
        if (!getLatList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        latMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < lon_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt64SizeNoTag(lon_.getLong(i));
        }
        size += dataSize;
        if (!getLonList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        lonMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < keysVals_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeInt32SizeNoTag(keysVals_.getInt(i));
        }
        size += dataSize;
        if (!getKeysValsList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        keysValsMemoizedSerializedSize = dataSize;
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.DenseNodes)) {
        return super.equals(obj);
      }
      Osmformat.DenseNodes other = (Osmformat.DenseNodes) obj;

      if (!getIdList().equals(other.getIdList())) return false;
      if (hasDenseinfo() != other.hasDenseinfo()) return false;
      if (hasDenseinfo()) {
        if (!getDenseinfo().equals(other.getDenseinfo())) return false;
      }
      if (!getLatList().equals(other.getLatList())) return false;
      if (!getLonList().equals(other.getLonList())) return false;
      if (!getKeysValsList().equals(other.getKeysValsList())) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (getIdCount() > 0) {
        hash = (37 * hash) + ID_FIELD_NUMBER;
        hash = (53 * hash) + getIdList().hashCode();
      }
      if (hasDenseinfo()) {
        hash = (37 * hash) + DENSEINFO_FIELD_NUMBER;
        hash = (53 * hash) + getDenseinfo().hashCode();
      }
      if (getLatCount() > 0) {
        hash = (37 * hash) + LAT_FIELD_NUMBER;
        hash = (53 * hash) + getLatList().hashCode();
      }
      if (getLonCount() > 0) {
        hash = (37 * hash) + LON_FIELD_NUMBER;
        hash = (53 * hash) + getLonList().hashCode();
      }
      if (getKeysValsCount() > 0) {
        hash = (37 * hash) + KEYS_VALS_FIELD_NUMBER;
        hash = (53 * hash) + getKeysValsList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.DenseNodes parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.DenseNodes parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.DenseNodes parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.DenseNodes parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.DenseNodes parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.DenseNodes parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.DenseNodes parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.DenseNodes parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.DenseNodes parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.DenseNodes parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.DenseNodes parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.DenseNodes parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.DenseNodes prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /** Protobuf type {@code DenseNodes} */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:DenseNodes)
        Osmformat.DenseNodesOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_DenseNodes_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_DenseNodes_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.DenseNodes.class, Osmformat.DenseNodes.Builder.class);
      }

      // Construct using Osmformat.DenseNodes.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }

      private void maybeForceBuilderInitialization() {
        if (proto4.GeneratedMessage.alwaysUseFieldBuilders) {
          getDenseinfoFieldBuilder();
        }
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = emptyLongList();
        denseinfo_ = null;
        if (denseinfoBuilder_ != null) {
          denseinfoBuilder_.dispose();
          denseinfoBuilder_ = null;
        }
        lat_ = emptyLongList();
        lon_ = emptyLongList();
        keysVals_ = emptyIntList();
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_DenseNodes_descriptor;
      }

      @Override
      public Osmformat.DenseNodes getDefaultInstanceForType() {
        return Osmformat.DenseNodes.getDefaultInstance();
      }

      @Override
      public Osmformat.DenseNodes build() {
        Osmformat.DenseNodes result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.DenseNodes buildPartial() {
        Osmformat.DenseNodes result = new Osmformat.DenseNodes(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.DenseNodes result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          id_.makeImmutable();
          result.id_ = id_;
        }
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.denseinfo_ = denseinfoBuilder_ == null ? denseinfo_ : denseinfoBuilder_.build();
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          lat_.makeImmutable();
          result.lat_ = lat_;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          lon_.makeImmutable();
          result.lon_ = lon_;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          keysVals_.makeImmutable();
          result.keysVals_ = keysVals_;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.DenseNodes) {
          return mergeFrom((Osmformat.DenseNodes) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.DenseNodes other) {
        if (other == Osmformat.DenseNodes.getDefaultInstance()) return this;
        if (!other.id_.isEmpty()) {
          if (id_.isEmpty()) {
            id_ = other.id_;
            id_.makeImmutable();
            bitField0_ |= 0x00000001;
          } else {
            ensureIdIsMutable();
            id_.addAll(other.id_);
          }
          onChanged();
        }
        if (other.hasDenseinfo()) {
          mergeDenseinfo(other.getDenseinfo());
        }
        if (!other.lat_.isEmpty()) {
          if (lat_.isEmpty()) {
            lat_ = other.lat_;
            lat_.makeImmutable();
            bitField0_ |= 0x00000004;
          } else {
            ensureLatIsMutable();
            lat_.addAll(other.lat_);
          }
          onChanged();
        }
        if (!other.lon_.isEmpty()) {
          if (lon_.isEmpty()) {
            lon_ = other.lon_;
            lon_.makeImmutable();
            bitField0_ |= 0x00000008;
          } else {
            ensureLonIsMutable();
            lon_.addAll(other.lon_);
          }
          onChanged();
        }
        if (!other.keysVals_.isEmpty()) {
          if (keysVals_.isEmpty()) {
            keysVals_ = other.keysVals_;
            keysVals_.makeImmutable();
            bitField0_ |= 0x00000010;
          } else {
            ensureKeysValsIsMutable();
            keysVals_.addAll(other.keysVals_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8:
                {
                  long v = input.readSInt64();
                  ensureIdIsMutable();
                  id_.addLong(v);
                  break;
                } // case 8
              case 10:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureIdIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    id_.addLong(input.readSInt64());
                  }
                  input.popLimit(limit);
                  break;
                } // case 10
              case 42:
                {
                  input.readMessage(getDenseinfoFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000002;
                  break;
                } // case 42
              case 64:
                {
                  long v = input.readSInt64();
                  ensureLatIsMutable();
                  lat_.addLong(v);
                  break;
                } // case 64
              case 66:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureLatIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    lat_.addLong(input.readSInt64());
                  }
                  input.popLimit(limit);
                  break;
                } // case 66
              case 72:
                {
                  long v = input.readSInt64();
                  ensureLonIsMutable();
                  lon_.addLong(v);
                  break;
                } // case 72
              case 74:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureLonIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    lon_.addLong(input.readSInt64());
                  }
                  input.popLimit(limit);
                  break;
                } // case 74
              case 80:
                {
                  int v = input.readInt32();
                  ensureKeysValsIsMutable();
                  keysVals_.addInt(v);
                  break;
                } // case 80
              case 82:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureKeysValsIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    keysVals_.addInt(input.readInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 82
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private proto4.Internal.LongList id_ = emptyLongList();

      private void ensureIdIsMutable() {
        if (!id_.isModifiable()) {
          id_ = makeMutableCopy(id_);
        }
        bitField0_ |= 0x00000001;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 id = 1 [packed = true];</code>
       *
       * @return A list containing the id.
       */
      public java.util.List<Long> getIdList() {
        id_.makeImmutable();
        return id_;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 id = 1 [packed = true];</code>
       *
       * @return The count of id.
       */
      public int getIdCount() {
        return id_.size();
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 id = 1 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The id at the given index.
       */
      public long getId(int index) {
        return id_.getLong(index);
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 id = 1 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(int index, long value) {

        ensureIdIsMutable();
        id_.setLong(index, value);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 id = 1 [packed = true];</code>
       *
       * @param value The id to add.
       * @return This builder for chaining.
       */
      public Builder addId(long value) {

        ensureIdIsMutable();
        id_.addLong(value);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 id = 1 [packed = true];</code>
       *
       * @param values The id to add.
       * @return This builder for chaining.
       */
      public Builder addAllId(Iterable<? extends Long> values) {
        ensureIdIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, id_);
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 id = 1 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearId() {
        id_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }

      private Osmformat.DenseInfo denseinfo_;
      private proto4.SingleFieldBuilder<
              Osmformat.DenseInfo, Osmformat.DenseInfo.Builder, Osmformat.DenseInfoOrBuilder>
          denseinfoBuilder_;

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       *
       * @return Whether the denseinfo field is set.
       */
      public boolean hasDenseinfo() {
        return ((bitField0_ & 0x00000002) != 0);
      }

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       *
       * @return The denseinfo.
       */
      public Osmformat.DenseInfo getDenseinfo() {
        if (denseinfoBuilder_ == null) {
          return denseinfo_ == null ? Osmformat.DenseInfo.getDefaultInstance() : denseinfo_;
        } else {
          return denseinfoBuilder_.getMessage();
        }
      }

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       */
      public Builder setDenseinfo(Osmformat.DenseInfo value) {
        if (denseinfoBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          denseinfo_ = value;
        } else {
          denseinfoBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       */
      public Builder setDenseinfo(Osmformat.DenseInfo.Builder builderForValue) {
        if (denseinfoBuilder_ == null) {
          denseinfo_ = builderForValue.build();
        } else {
          denseinfoBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       */
      public Builder mergeDenseinfo(Osmformat.DenseInfo value) {
        if (denseinfoBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0)
              && denseinfo_ != null
              && denseinfo_ != Osmformat.DenseInfo.getDefaultInstance()) {
            getDenseinfoBuilder().mergeFrom(value);
          } else {
            denseinfo_ = value;
          }
        } else {
          denseinfoBuilder_.mergeFrom(value);
        }
        if (denseinfo_ != null) {
          bitField0_ |= 0x00000002;
          onChanged();
        }
        return this;
      }

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       */
      public Builder clearDenseinfo() {
        bitField0_ = (bitField0_ & ~0x00000002);
        denseinfo_ = null;
        if (denseinfoBuilder_ != null) {
          denseinfoBuilder_.dispose();
          denseinfoBuilder_ = null;
        }
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       */
      public Osmformat.DenseInfo.Builder getDenseinfoBuilder() {
        bitField0_ |= 0x00000002;
        onChanged();
        return getDenseinfoFieldBuilder().getBuilder();
      }

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       */
      public Osmformat.DenseInfoOrBuilder getDenseinfoOrBuilder() {
        if (denseinfoBuilder_ != null) {
          return denseinfoBuilder_.getMessageOrBuilder();
        } else {
          return denseinfo_ == null ? Osmformat.DenseInfo.getDefaultInstance() : denseinfo_;
        }
      }

      /**
       *
       *
       * <pre>
       * repeated Info info = 4;
       * </pre>
       *
       * <code>optional .DenseInfo denseinfo = 5;</code>
       */
      private proto4.SingleFieldBuilder<
              Osmformat.DenseInfo, Osmformat.DenseInfo.Builder, Osmformat.DenseInfoOrBuilder>
          getDenseinfoFieldBuilder() {
        if (denseinfoBuilder_ == null) {
          denseinfoBuilder_ =
              new proto4.SingleFieldBuilder<
                  Osmformat.DenseInfo, Osmformat.DenseInfo.Builder, Osmformat.DenseInfoOrBuilder>(
                  getDenseinfo(), getParentForChildren(), isClean());
          denseinfo_ = null;
        }
        return denseinfoBuilder_;
      }

      private proto4.Internal.LongList lat_ = emptyLongList();

      private void ensureLatIsMutable() {
        if (!lat_.isModifiable()) {
          lat_ = makeMutableCopy(lat_);
        }
        bitField0_ |= 0x00000004;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lat = 8 [packed = true];</code>
       *
       * @return A list containing the lat.
       */
      public java.util.List<Long> getLatList() {
        lat_.makeImmutable();
        return lat_;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lat = 8 [packed = true];</code>
       *
       * @return The count of lat.
       */
      public int getLatCount() {
        return lat_.size();
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lat = 8 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The lat at the given index.
       */
      public long getLat(int index) {
        return lat_.getLong(index);
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lat = 8 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The lat to set.
       * @return This builder for chaining.
       */
      public Builder setLat(int index, long value) {

        ensureLatIsMutable();
        lat_.setLong(index, value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lat = 8 [packed = true];</code>
       *
       * @param value The lat to add.
       * @return This builder for chaining.
       */
      public Builder addLat(long value) {

        ensureLatIsMutable();
        lat_.addLong(value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lat = 8 [packed = true];</code>
       *
       * @param values The lat to add.
       * @return This builder for chaining.
       */
      public Builder addAllLat(Iterable<? extends Long> values) {
        ensureLatIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, lat_);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lat = 8 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearLat() {
        lat_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
        return this;
      }

      private proto4.Internal.LongList lon_ = emptyLongList();

      private void ensureLonIsMutable() {
        if (!lon_.isModifiable()) {
          lon_ = makeMutableCopy(lon_);
        }
        bitField0_ |= 0x00000008;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lon = 9 [packed = true];</code>
       *
       * @return A list containing the lon.
       */
      public java.util.List<Long> getLonList() {
        lon_.makeImmutable();
        return lon_;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lon = 9 [packed = true];</code>
       *
       * @return The count of lon.
       */
      public int getLonCount() {
        return lon_.size();
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lon = 9 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The lon at the given index.
       */
      public long getLon(int index) {
        return lon_.getLong(index);
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lon = 9 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The lon to set.
       * @return This builder for chaining.
       */
      public Builder setLon(int index, long value) {

        ensureLonIsMutable();
        lon_.setLong(index, value);
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lon = 9 [packed = true];</code>
       *
       * @param value The lon to add.
       * @return This builder for chaining.
       */
      public Builder addLon(long value) {

        ensureLonIsMutable();
        lon_.addLong(value);
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lon = 9 [packed = true];</code>
       *
       * @param values The lon to add.
       * @return This builder for chaining.
       */
      public Builder addAllLon(Iterable<? extends Long> values) {
        ensureLonIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, lon_);
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 lon = 9 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearLon() {
        lon_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000008);
        onChanged();
        return this;
      }

      private proto4.Internal.IntList keysVals_ = emptyIntList();

      private void ensureKeysValsIsMutable() {
        if (!keysVals_.isModifiable()) {
          keysVals_ = makeMutableCopy(keysVals_);
        }
        bitField0_ |= 0x00000010;
      }

      /**
       *
       *
       * <pre>
       * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
       * </pre>
       *
       * <code>repeated int32 keys_vals = 10 [packed = true];</code>
       *
       * @return A list containing the keysVals.
       */
      public java.util.List<Integer> getKeysValsList() {
        keysVals_.makeImmutable();
        return keysVals_;
      }

      /**
       *
       *
       * <pre>
       * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
       * </pre>
       *
       * <code>repeated int32 keys_vals = 10 [packed = true];</code>
       *
       * @return The count of keysVals.
       */
      public int getKeysValsCount() {
        return keysVals_.size();
      }

      /**
       *
       *
       * <pre>
       * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
       * </pre>
       *
       * <code>repeated int32 keys_vals = 10 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The keysVals at the given index.
       */
      public int getKeysVals(int index) {
        return keysVals_.getInt(index);
      }

      /**
       *
       *
       * <pre>
       * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
       * </pre>
       *
       * <code>repeated int32 keys_vals = 10 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The keysVals to set.
       * @return This builder for chaining.
       */
      public Builder setKeysVals(int index, int value) {

        ensureKeysValsIsMutable();
        keysVals_.setInt(index, value);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
       * </pre>
       *
       * <code>repeated int32 keys_vals = 10 [packed = true];</code>
       *
       * @param value The keysVals to add.
       * @return This builder for chaining.
       */
      public Builder addKeysVals(int value) {

        ensureKeysValsIsMutable();
        keysVals_.addInt(value);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
       * </pre>
       *
       * <code>repeated int32 keys_vals = 10 [packed = true];</code>
       *
       * @param values The keysVals to add.
       * @return This builder for chaining.
       */
      public Builder addAllKeysVals(Iterable<? extends Integer> values) {
        ensureKeysValsIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, keysVals_);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Special packing of keys and vals into one array. May be empty if all nodes in this block are tagless.
       * </pre>
       *
       * <code>repeated int32 keys_vals = 10 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearKeysVals() {
        keysVals_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000010);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:DenseNodes)
    }

    // @@protoc_insertion_point(class_scope:DenseNodes)
    private static final Osmformat.DenseNodes DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.DenseNodes();
    }

    public static Osmformat.DenseNodes getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<DenseNodes> PARSER =
        new proto4.AbstractParser<DenseNodes>() {
          @Override
          public DenseNodes parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<DenseNodes> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<DenseNodes> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.DenseNodes getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface WayOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:Way)
      proto4.MessageOrBuilder {

    /**
     * <code>required int64 id = 1;</code>
     *
     * @return Whether the id field is set.
     */
    boolean hasId();

    /**
     * <code>required int64 id = 1;</code>
     *
     * @return The id.
     */
    long getId();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return A list containing the keys.
     */
    java.util.List<Integer> getKeysList();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return The count of keys.
     */
    int getKeysCount();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The keys at the given index.
     */
    int getKeys(int index);

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return A list containing the vals.
     */
    java.util.List<Integer> getValsList();

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return The count of vals.
     */
    int getValsCount();

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The vals at the given index.
     */
    int getVals(int index);

    /**
     * <code>optional .Info info = 4;</code>
     *
     * @return Whether the info field is set.
     */
    boolean hasInfo();

    /**
     * <code>optional .Info info = 4;</code>
     *
     * @return The info.
     */
    Osmformat.Info getInfo();

    /** <code>optional .Info info = 4;</code> */
    Osmformat.InfoOrBuilder getInfoOrBuilder();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 refs = 8 [packed = true];</code>
     *
     * @return A list containing the refs.
     */
    java.util.List<Long> getRefsList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 refs = 8 [packed = true];</code>
     *
     * @return The count of refs.
     */
    int getRefsCount();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 refs = 8 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The refs at the given index.
     */
    long getRefs(int index);
  }

  /** Protobuf type {@code Way} */
  public static final class Way extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:Way)
      WayOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          Way.class.getName());
    }

    // Use Way.newBuilder() to construct.
    private Way(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private Way() {
      keys_ = emptyIntList();
      vals_ = emptyIntList();
      refs_ = emptyLongList();
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_Way_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_Way_fieldAccessorTable
          .ensureFieldAccessorsInitialized(Osmformat.Way.class, Osmformat.Way.Builder.class);
    }

    private int bitField0_;
    public static final int ID_FIELD_NUMBER = 1;
    private long id_ = 0L;

    /**
     * <code>required int64 id = 1;</code>
     *
     * @return Whether the id field is set.
     */
    @Override
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     * <code>required int64 id = 1;</code>
     *
     * @return The id.
     */
    @Override
    public long getId() {
      return id_;
    }

    public static final int KEYS_FIELD_NUMBER = 2;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList keys_ = emptyIntList();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return A list containing the keys.
     */
    @Override
    public java.util.List<Integer> getKeysList() {
      return keys_;
    }

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return The count of keys.
     */
    public int getKeysCount() {
      return keys_.size();
    }

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The keys at the given index.
     */
    public int getKeys(int index) {
      return keys_.getInt(index);
    }

    private int keysMemoizedSerializedSize = -1;

    public static final int VALS_FIELD_NUMBER = 3;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList vals_ = emptyIntList();

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return A list containing the vals.
     */
    @Override
    public java.util.List<Integer> getValsList() {
      return vals_;
    }

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return The count of vals.
     */
    public int getValsCount() {
      return vals_.size();
    }

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The vals at the given index.
     */
    public int getVals(int index) {
      return vals_.getInt(index);
    }

    private int valsMemoizedSerializedSize = -1;

    public static final int INFO_FIELD_NUMBER = 4;
    private Osmformat.Info info_;

    /**
     * <code>optional .Info info = 4;</code>
     *
     * @return Whether the info field is set.
     */
    @Override
    public boolean hasInfo() {
      return ((bitField0_ & 0x00000002) != 0);
    }

    /**
     * <code>optional .Info info = 4;</code>
     *
     * @return The info.
     */
    @Override
    public Osmformat.Info getInfo() {
      return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
    }

    /** <code>optional .Info info = 4;</code> */
    @Override
    public Osmformat.InfoOrBuilder getInfoOrBuilder() {
      return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
    }

    public static final int REFS_FIELD_NUMBER = 8;

    @SuppressWarnings("serial")
    private proto4.Internal.LongList refs_ = emptyLongList();

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 refs = 8 [packed = true];</code>
     *
     * @return A list containing the refs.
     */
    @Override
    public java.util.List<Long> getRefsList() {
      return refs_;
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 refs = 8 [packed = true];</code>
     *
     * @return The count of refs.
     */
    public int getRefsCount() {
      return refs_.size();
    }

    /**
     *
     *
     * <pre>
     * DELTA coded
     * </pre>
     *
     * <code>repeated sint64 refs = 8 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The refs at the given index.
     */
    public long getRefs(int index) {
      return refs_.getLong(index);
    }

    private int refsMemoizedSerializedSize = -1;

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt64(1, id_);
      }
      if (getKeysList().size() > 0) {
        output.writeUInt32NoTag(18);
        output.writeUInt32NoTag(keysMemoizedSerializedSize);
      }
      for (int i = 0; i < keys_.size(); i++) {
        output.writeUInt32NoTag(keys_.getInt(i));
      }
      if (getValsList().size() > 0) {
        output.writeUInt32NoTag(26);
        output.writeUInt32NoTag(valsMemoizedSerializedSize);
      }
      for (int i = 0; i < vals_.size(); i++) {
        output.writeUInt32NoTag(vals_.getInt(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(4, getInfo());
      }
      if (getRefsList().size() > 0) {
        output.writeUInt32NoTag(66);
        output.writeUInt32NoTag(refsMemoizedSerializedSize);
      }
      for (int i = 0; i < refs_.size(); i++) {
        output.writeSInt64NoTag(refs_.getLong(i));
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(1, id_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < keys_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeUInt32SizeNoTag(keys_.getInt(i));
        }
        size += dataSize;
        if (!getKeysList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        keysMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < vals_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeUInt32SizeNoTag(vals_.getInt(i));
        }
        size += dataSize;
        if (!getValsList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        valsMemoizedSerializedSize = dataSize;
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += proto4.CodedOutputStream.computeMessageSize(4, getInfo());
      }
      {
        int dataSize = 0;
        for (int i = 0; i < refs_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt64SizeNoTag(refs_.getLong(i));
        }
        size += dataSize;
        if (!getRefsList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        refsMemoizedSerializedSize = dataSize;
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.Way)) {
        return super.equals(obj);
      }
      Osmformat.Way other = (Osmformat.Way) obj;

      if (hasId() != other.hasId()) return false;
      if (hasId()) {
        if (getId() != other.getId()) return false;
      }
      if (!getKeysList().equals(other.getKeysList())) return false;
      if (!getValsList().equals(other.getValsList())) return false;
      if (hasInfo() != other.hasInfo()) return false;
      if (hasInfo()) {
        if (!getInfo().equals(other.getInfo())) return false;
      }
      if (!getRefsList().equals(other.getRefsList())) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasId()) {
        hash = (37 * hash) + ID_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getId());
      }
      if (getKeysCount() > 0) {
        hash = (37 * hash) + KEYS_FIELD_NUMBER;
        hash = (53 * hash) + getKeysList().hashCode();
      }
      if (getValsCount() > 0) {
        hash = (37 * hash) + VALS_FIELD_NUMBER;
        hash = (53 * hash) + getValsList().hashCode();
      }
      if (hasInfo()) {
        hash = (37 * hash) + INFO_FIELD_NUMBER;
        hash = (53 * hash) + getInfo().hashCode();
      }
      if (getRefsCount() > 0) {
        hash = (37 * hash) + REFS_FIELD_NUMBER;
        hash = (53 * hash) + getRefsList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.Way parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Way parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Way parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Way parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Way parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Way parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Way parseFrom(java.io.InputStream input) throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.Way parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.Way parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.Way parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.Way parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.Way parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.Way prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /** Protobuf type {@code Way} */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:Way)
        Osmformat.WayOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Way_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Way_fieldAccessorTable
            .ensureFieldAccessorsInitialized(Osmformat.Way.class, Osmformat.Way.Builder.class);
      }

      // Construct using Osmformat.Way.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }

      private void maybeForceBuilderInitialization() {
        if (proto4.GeneratedMessage.alwaysUseFieldBuilders) {
          getInfoFieldBuilder();
        }
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = 0L;
        keys_ = emptyIntList();
        vals_ = emptyIntList();
        info_ = null;
        if (infoBuilder_ != null) {
          infoBuilder_.dispose();
          infoBuilder_ = null;
        }
        refs_ = emptyLongList();
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Way_descriptor;
      }

      @Override
      public Osmformat.Way getDefaultInstanceForType() {
        return Osmformat.Way.getDefaultInstance();
      }

      @Override
      public Osmformat.Way build() {
        Osmformat.Way result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.Way buildPartial() {
        Osmformat.Way result = new Osmformat.Way(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Osmformat.Way result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          keys_.makeImmutable();
          result.keys_ = keys_;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          vals_.makeImmutable();
          result.vals_ = vals_;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.info_ = infoBuilder_ == null ? info_ : infoBuilder_.build();
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          refs_.makeImmutable();
          result.refs_ = refs_;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.Way) {
          return mergeFrom((Osmformat.Way) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.Way other) {
        if (other == Osmformat.Way.getDefaultInstance()) return this;
        if (other.hasId()) {
          setId(other.getId());
        }
        if (!other.keys_.isEmpty()) {
          if (keys_.isEmpty()) {
            keys_ = other.keys_;
            keys_.makeImmutable();
            bitField0_ |= 0x00000002;
          } else {
            ensureKeysIsMutable();
            keys_.addAll(other.keys_);
          }
          onChanged();
        }
        if (!other.vals_.isEmpty()) {
          if (vals_.isEmpty()) {
            vals_ = other.vals_;
            vals_.makeImmutable();
            bitField0_ |= 0x00000004;
          } else {
            ensureValsIsMutable();
            vals_.addAll(other.vals_);
          }
          onChanged();
        }
        if (other.hasInfo()) {
          mergeInfo(other.getInfo());
        }
        if (!other.refs_.isEmpty()) {
          if (refs_.isEmpty()) {
            refs_ = other.refs_;
            refs_.makeImmutable();
            bitField0_ |= 0x00000010;
          } else {
            ensureRefsIsMutable();
            refs_.addAll(other.refs_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        if (!hasId()) {
          return false;
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8:
                {
                  id_ = input.readInt64();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 8
              case 16:
                {
                  int v = input.readUInt32();
                  ensureKeysIsMutable();
                  keys_.addInt(v);
                  break;
                } // case 16
              case 18:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureKeysIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    keys_.addInt(input.readUInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 18
              case 24:
                {
                  int v = input.readUInt32();
                  ensureValsIsMutable();
                  vals_.addInt(v);
                  break;
                } // case 24
              case 26:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureValsIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    vals_.addInt(input.readUInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 26
              case 34:
                {
                  input.readMessage(getInfoFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000008;
                  break;
                } // case 34
              case 64:
                {
                  long v = input.readSInt64();
                  ensureRefsIsMutable();
                  refs_.addLong(v);
                  break;
                } // case 64
              case 66:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureRefsIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    refs_.addLong(input.readSInt64());
                  }
                  input.popLimit(limit);
                  break;
                } // case 66
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private long id_;

      /**
       * <code>required int64 id = 1;</code>
       *
       * @return Whether the id field is set.
       */
      @Override
      public boolean hasId() {
        return ((bitField0_ & 0x00000001) != 0);
      }

      /**
       * <code>required int64 id = 1;</code>
       *
       * @return The id.
       */
      @Override
      public long getId() {
        return id_;
      }

      /**
       * <code>required int64 id = 1;</code>
       *
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(long value) {

        id_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>required int64 id = 1;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = 0L;
        onChanged();
        return this;
      }

      private proto4.Internal.IntList keys_ = emptyIntList();

      private void ensureKeysIsMutable() {
        if (!keys_.isModifiable()) {
          keys_ = makeMutableCopy(keys_);
        }
        bitField0_ |= 0x00000002;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return A list containing the keys.
       */
      public java.util.List<Integer> getKeysList() {
        keys_.makeImmutable();
        return keys_;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return The count of keys.
       */
      public int getKeysCount() {
        return keys_.size();
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The keys at the given index.
       */
      public int getKeys(int index) {
        return keys_.getInt(index);
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The keys to set.
       * @return This builder for chaining.
       */
      public Builder setKeys(int index, int value) {

        ensureKeysIsMutable();
        keys_.setInt(index, value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param value The keys to add.
       * @return This builder for chaining.
       */
      public Builder addKeys(int value) {

        ensureKeysIsMutable();
        keys_.addInt(value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param values The keys to add.
       * @return This builder for chaining.
       */
      public Builder addAllKeys(Iterable<? extends Integer> values) {
        ensureKeysIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, keys_);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearKeys() {
        keys_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }

      private proto4.Internal.IntList vals_ = emptyIntList();

      private void ensureValsIsMutable() {
        if (!vals_.isModifiable()) {
          vals_ = makeMutableCopy(vals_);
        }
        bitField0_ |= 0x00000004;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return A list containing the vals.
       */
      public java.util.List<Integer> getValsList() {
        vals_.makeImmutable();
        return vals_;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return The count of vals.
       */
      public int getValsCount() {
        return vals_.size();
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The vals at the given index.
       */
      public int getVals(int index) {
        return vals_.getInt(index);
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The vals to set.
       * @return This builder for chaining.
       */
      public Builder setVals(int index, int value) {

        ensureValsIsMutable();
        vals_.setInt(index, value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param value The vals to add.
       * @return This builder for chaining.
       */
      public Builder addVals(int value) {

        ensureValsIsMutable();
        vals_.addInt(value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param values The vals to add.
       * @return This builder for chaining.
       */
      public Builder addAllVals(Iterable<? extends Integer> values) {
        ensureValsIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, vals_);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearVals() {
        vals_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
        return this;
      }

      private Osmformat.Info info_;
      private proto4.SingleFieldBuilder<
              Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>
          infoBuilder_;

      /**
       * <code>optional .Info info = 4;</code>
       *
       * @return Whether the info field is set.
       */
      public boolean hasInfo() {
        return ((bitField0_ & 0x00000008) != 0);
      }

      /**
       * <code>optional .Info info = 4;</code>
       *
       * @return The info.
       */
      public Osmformat.Info getInfo() {
        if (infoBuilder_ == null) {
          return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
        } else {
          return infoBuilder_.getMessage();
        }
      }

      /** <code>optional .Info info = 4;</code> */
      public Builder setInfo(Osmformat.Info value) {
        if (infoBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          info_ = value;
        } else {
          infoBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /** <code>optional .Info info = 4;</code> */
      public Builder setInfo(Osmformat.Info.Builder builderForValue) {
        if (infoBuilder_ == null) {
          info_ = builderForValue.build();
        } else {
          infoBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /** <code>optional .Info info = 4;</code> */
      public Builder mergeInfo(Osmformat.Info value) {
        if (infoBuilder_ == null) {
          if (((bitField0_ & 0x00000008) != 0)
              && info_ != null
              && info_ != Osmformat.Info.getDefaultInstance()) {
            getInfoBuilder().mergeFrom(value);
          } else {
            info_ = value;
          }
        } else {
          infoBuilder_.mergeFrom(value);
        }
        if (info_ != null) {
          bitField0_ |= 0x00000008;
          onChanged();
        }
        return this;
      }

      /** <code>optional .Info info = 4;</code> */
      public Builder clearInfo() {
        bitField0_ = (bitField0_ & ~0x00000008);
        info_ = null;
        if (infoBuilder_ != null) {
          infoBuilder_.dispose();
          infoBuilder_ = null;
        }
        onChanged();
        return this;
      }

      /** <code>optional .Info info = 4;</code> */
      public Osmformat.Info.Builder getInfoBuilder() {
        bitField0_ |= 0x00000008;
        onChanged();
        return getInfoFieldBuilder().getBuilder();
      }

      /** <code>optional .Info info = 4;</code> */
      public Osmformat.InfoOrBuilder getInfoOrBuilder() {
        if (infoBuilder_ != null) {
          return infoBuilder_.getMessageOrBuilder();
        } else {
          return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
        }
      }

      /** <code>optional .Info info = 4;</code> */
      private proto4.SingleFieldBuilder<
              Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>
          getInfoFieldBuilder() {
        if (infoBuilder_ == null) {
          infoBuilder_ =
              new proto4.SingleFieldBuilder<
                  Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>(
                  getInfo(), getParentForChildren(), isClean());
          info_ = null;
        }
        return infoBuilder_;
      }

      private proto4.Internal.LongList refs_ = emptyLongList();

      private void ensureRefsIsMutable() {
        if (!refs_.isModifiable()) {
          refs_ = makeMutableCopy(refs_);
        }
        bitField0_ |= 0x00000010;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 refs = 8 [packed = true];</code>
       *
       * @return A list containing the refs.
       */
      public java.util.List<Long> getRefsList() {
        refs_.makeImmutable();
        return refs_;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 refs = 8 [packed = true];</code>
       *
       * @return The count of refs.
       */
      public int getRefsCount() {
        return refs_.size();
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 refs = 8 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The refs at the given index.
       */
      public long getRefs(int index) {
        return refs_.getLong(index);
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 refs = 8 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The refs to set.
       * @return This builder for chaining.
       */
      public Builder setRefs(int index, long value) {

        ensureRefsIsMutable();
        refs_.setLong(index, value);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 refs = 8 [packed = true];</code>
       *
       * @param value The refs to add.
       * @return This builder for chaining.
       */
      public Builder addRefs(long value) {

        ensureRefsIsMutable();
        refs_.addLong(value);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 refs = 8 [packed = true];</code>
       *
       * @param values The refs to add.
       * @return This builder for chaining.
       */
      public Builder addAllRefs(Iterable<? extends Long> values) {
        ensureRefsIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, refs_);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA coded
       * </pre>
       *
       * <code>repeated sint64 refs = 8 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearRefs() {
        refs_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000010);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:Way)
    }

    // @@protoc_insertion_point(class_scope:Way)
    private static final Osmformat.Way DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.Way();
    }

    public static Osmformat.Way getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<Way> PARSER =
        new proto4.AbstractParser<Way>() {
          @Override
          public Way parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<Way> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<Way> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.Way getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface RelationOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:Relation)
      proto4.MessageOrBuilder {

    /**
     * <code>required int64 id = 1;</code>
     *
     * @return Whether the id field is set.
     */
    boolean hasId();

    /**
     * <code>required int64 id = 1;</code>
     *
     * @return The id.
     */
    long getId();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return A list containing the keys.
     */
    java.util.List<Integer> getKeysList();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return The count of keys.
     */
    int getKeysCount();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The keys at the given index.
     */
    int getKeys(int index);

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return A list containing the vals.
     */
    java.util.List<Integer> getValsList();

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return The count of vals.
     */
    int getValsCount();

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The vals at the given index.
     */
    int getVals(int index);

    /**
     * <code>optional .Info info = 4;</code>
     *
     * @return Whether the info field is set.
     */
    boolean hasInfo();

    /**
     * <code>optional .Info info = 4;</code>
     *
     * @return The info.
     */
    Osmformat.Info getInfo();

    /** <code>optional .Info info = 4;</code> */
    Osmformat.InfoOrBuilder getInfoOrBuilder();

    /**
     *
     *
     * <pre>
     * Parallel arrays
     * </pre>
     *
     * <code>repeated int32 roles_sid = 8 [packed = true];</code>
     *
     * @return A list containing the rolesSid.
     */
    java.util.List<Integer> getRolesSidList();

    /**
     *
     *
     * <pre>
     * Parallel arrays
     * </pre>
     *
     * <code>repeated int32 roles_sid = 8 [packed = true];</code>
     *
     * @return The count of rolesSid.
     */
    int getRolesSidCount();

    /**
     *
     *
     * <pre>
     * Parallel arrays
     * </pre>
     *
     * <code>repeated int32 roles_sid = 8 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The rolesSid at the given index.
     */
    int getRolesSid(int index);

    /**
     *
     *
     * <pre>
     * DELTA encoded
     * </pre>
     *
     * <code>repeated sint64 memids = 9 [packed = true];</code>
     *
     * @return A list containing the memids.
     */
    java.util.List<Long> getMemidsList();

    /**
     *
     *
     * <pre>
     * DELTA encoded
     * </pre>
     *
     * <code>repeated sint64 memids = 9 [packed = true];</code>
     *
     * @return The count of memids.
     */
    int getMemidsCount();

    /**
     *
     *
     * <pre>
     * DELTA encoded
     * </pre>
     *
     * <code>repeated sint64 memids = 9 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The memids at the given index.
     */
    long getMemids(int index);

    /**
     * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
     *
     * @return A list containing the types.
     */
    java.util.List<Osmformat.Relation.MemberType> getTypesList();

    /**
     * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
     *
     * @return The count of types.
     */
    int getTypesCount();

    /**
     * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The types at the given index.
     */
    Osmformat.Relation.MemberType getTypes(int index);
  }

  /** Protobuf type {@code Relation} */
  public static final class Relation extends proto4.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:Relation)
      RelationOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      proto4.RuntimeVersion.validateProtobufGencodeVersion(
          proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          Relation.class.getName());
    }

    // Use Relation.newBuilder() to construct.
    private Relation(proto4.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private Relation() {
      keys_ = emptyIntList();
      vals_ = emptyIntList();
      rolesSid_ = emptyIntList();
      memids_ = emptyLongList();
      types_ = java.util.Collections.emptyList();
    }

    public static final proto4.Descriptors.Descriptor getDescriptor() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_Relation_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Osmformat.internal_static_org_apache_sedona_osm_build_Relation_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Osmformat.Relation.class, Osmformat.Relation.Builder.class);
    }

    /** Protobuf enum {@code Relation.MemberType} */
    public enum MemberType implements proto4.ProtocolMessageEnum {
      /** <code>NODE = 0;</code> */
      NODE(0),
      /** <code>WAY = 1;</code> */
      WAY(1),
      /** <code>RELATION = 2;</code> */
      RELATION(2),
      ;

      static {
        proto4.RuntimeVersion.validateProtobufGencodeVersion(
            proto4.RuntimeVersion.RuntimeDomain.PUBLIC,
            /* major= */ 4,
            /* minor= */ 27,
            /* patch= */ 0,
            /* suffix= */ "",
            MemberType.class.getName());
      }

      /** <code>NODE = 0;</code> */
      public static final int NODE_VALUE = 0;
      /** <code>WAY = 1;</code> */
      public static final int WAY_VALUE = 1;
      /** <code>RELATION = 2;</code> */
      public static final int RELATION_VALUE = 2;

      public final int getNumber() {
        return value;
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       * @deprecated Use {@link #forNumber(int)} instead.
       */
      @Deprecated
      public static MemberType valueOf(int value) {
        return forNumber(value);
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       */
      public static MemberType forNumber(int value) {
        switch (value) {
          case 0:
            return NODE;
          case 1:
            return WAY;
          case 2:
            return RELATION;
          default:
            return null;
        }
      }

      public static proto4.Internal.EnumLiteMap<MemberType> internalGetValueMap() {
        return internalValueMap;
      }

      private static final proto4.Internal.EnumLiteMap<MemberType> internalValueMap =
          new proto4.Internal.EnumLiteMap<MemberType>() {
            public MemberType findValueByNumber(int number) {
              return MemberType.forNumber(number);
            }
          };

      public final proto4.Descriptors.EnumValueDescriptor getValueDescriptor() {
        return getDescriptor().getValues().get(ordinal());
      }

      public final proto4.Descriptors.EnumDescriptor getDescriptorForType() {
        return getDescriptor();
      }

      public static final proto4.Descriptors.EnumDescriptor getDescriptor() {
        return Osmformat.Relation.getDescriptor().getEnumTypes().get(0);
      }

      private static final MemberType[] VALUES = values();

      public static MemberType valueOf(proto4.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new IllegalArgumentException("EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }

      private final int value;

      private MemberType(int value) {
        this.value = value;
      }

      // @@protoc_insertion_point(enum_scope:Relation.MemberType)
    }

    private int bitField0_;
    public static final int ID_FIELD_NUMBER = 1;
    private long id_ = 0L;

    /**
     * <code>required int64 id = 1;</code>
     *
     * @return Whether the id field is set.
     */
    @Override
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) != 0);
    }

    /**
     * <code>required int64 id = 1;</code>
     *
     * @return The id.
     */
    @Override
    public long getId() {
      return id_;
    }

    public static final int KEYS_FIELD_NUMBER = 2;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList keys_ = emptyIntList();

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return A list containing the keys.
     */
    @Override
    public java.util.List<Integer> getKeysList() {
      return keys_;
    }

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @return The count of keys.
     */
    public int getKeysCount() {
      return keys_.size();
    }

    /**
     *
     *
     * <pre>
     * Parallel arrays.
     * </pre>
     *
     * <code>repeated uint32 keys = 2 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The keys at the given index.
     */
    public int getKeys(int index) {
      return keys_.getInt(index);
    }

    private int keysMemoizedSerializedSize = -1;

    public static final int VALS_FIELD_NUMBER = 3;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList vals_ = emptyIntList();

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return A list containing the vals.
     */
    @Override
    public java.util.List<Integer> getValsList() {
      return vals_;
    }

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @return The count of vals.
     */
    public int getValsCount() {
      return vals_.size();
    }

    /**
     * <code>repeated uint32 vals = 3 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The vals at the given index.
     */
    public int getVals(int index) {
      return vals_.getInt(index);
    }

    private int valsMemoizedSerializedSize = -1;

    public static final int INFO_FIELD_NUMBER = 4;
    private Osmformat.Info info_;

    /**
     * <code>optional .Info info = 4;</code>
     *
     * @return Whether the info field is set.
     */
    @Override
    public boolean hasInfo() {
      return ((bitField0_ & 0x00000002) != 0);
    }

    /**
     * <code>optional .Info info = 4;</code>
     *
     * @return The info.
     */
    @Override
    public Osmformat.Info getInfo() {
      return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
    }

    /** <code>optional .Info info = 4;</code> */
    @Override
    public Osmformat.InfoOrBuilder getInfoOrBuilder() {
      return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
    }

    public static final int ROLES_SID_FIELD_NUMBER = 8;

    @SuppressWarnings("serial")
    private proto4.Internal.IntList rolesSid_ = emptyIntList();

    /**
     *
     *
     * <pre>
     * Parallel arrays
     * </pre>
     *
     * <code>repeated int32 roles_sid = 8 [packed = true];</code>
     *
     * @return A list containing the rolesSid.
     */
    @Override
    public java.util.List<Integer> getRolesSidList() {
      return rolesSid_;
    }

    /**
     *
     *
     * <pre>
     * Parallel arrays
     * </pre>
     *
     * <code>repeated int32 roles_sid = 8 [packed = true];</code>
     *
     * @return The count of rolesSid.
     */
    public int getRolesSidCount() {
      return rolesSid_.size();
    }

    /**
     *
     *
     * <pre>
     * Parallel arrays
     * </pre>
     *
     * <code>repeated int32 roles_sid = 8 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The rolesSid at the given index.
     */
    public int getRolesSid(int index) {
      return rolesSid_.getInt(index);
    }

    private int rolesSidMemoizedSerializedSize = -1;

    public static final int MEMIDS_FIELD_NUMBER = 9;

    @SuppressWarnings("serial")
    private proto4.Internal.LongList memids_ = emptyLongList();

    /**
     *
     *
     * <pre>
     * DELTA encoded
     * </pre>
     *
     * <code>repeated sint64 memids = 9 [packed = true];</code>
     *
     * @return A list containing the memids.
     */
    @Override
    public java.util.List<Long> getMemidsList() {
      return memids_;
    }

    /**
     *
     *
     * <pre>
     * DELTA encoded
     * </pre>
     *
     * <code>repeated sint64 memids = 9 [packed = true];</code>
     *
     * @return The count of memids.
     */
    public int getMemidsCount() {
      return memids_.size();
    }

    /**
     *
     *
     * <pre>
     * DELTA encoded
     * </pre>
     *
     * <code>repeated sint64 memids = 9 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The memids at the given index.
     */
    public long getMemids(int index) {
      return memids_.getLong(index);
    }

    private int memidsMemoizedSerializedSize = -1;

    public static final int TYPES_FIELD_NUMBER = 10;

    @SuppressWarnings("serial")
    private java.util.List<Integer> types_;

    private static final proto4.Internal.ListAdapter.Converter<
            Integer, Osmformat.Relation.MemberType>
        types_converter_ =
            new proto4.Internal.ListAdapter.Converter<Integer, Osmformat.Relation.MemberType>() {
              public Osmformat.Relation.MemberType convert(Integer from) {
                Osmformat.Relation.MemberType result =
                    Osmformat.Relation.MemberType.forNumber(from);
                return result == null ? Osmformat.Relation.MemberType.NODE : result;
              }
            };

    /**
     * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
     *
     * @return A list containing the types.
     */
    @Override
    public java.util.List<Osmformat.Relation.MemberType> getTypesList() {
      return new proto4.Internal.ListAdapter<Integer, Osmformat.Relation.MemberType>(
          types_, types_converter_);
    }

    /**
     * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
     *
     * @return The count of types.
     */
    @Override
    public int getTypesCount() {
      return types_.size();
    }

    /**
     * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
     *
     * @param index The index of the element to return.
     * @return The types at the given index.
     */
    @Override
    public Osmformat.Relation.MemberType getTypes(int index) {
      return types_converter_.convert(types_.get(index));
    }

    private int typesMemoizedSerializedSize;

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(proto4.CodedOutputStream output) throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt64(1, id_);
      }
      if (getKeysList().size() > 0) {
        output.writeUInt32NoTag(18);
        output.writeUInt32NoTag(keysMemoizedSerializedSize);
      }
      for (int i = 0; i < keys_.size(); i++) {
        output.writeUInt32NoTag(keys_.getInt(i));
      }
      if (getValsList().size() > 0) {
        output.writeUInt32NoTag(26);
        output.writeUInt32NoTag(valsMemoizedSerializedSize);
      }
      for (int i = 0; i < vals_.size(); i++) {
        output.writeUInt32NoTag(vals_.getInt(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(4, getInfo());
      }
      if (getRolesSidList().size() > 0) {
        output.writeUInt32NoTag(66);
        output.writeUInt32NoTag(rolesSidMemoizedSerializedSize);
      }
      for (int i = 0; i < rolesSid_.size(); i++) {
        output.writeInt32NoTag(rolesSid_.getInt(i));
      }
      if (getMemidsList().size() > 0) {
        output.writeUInt32NoTag(74);
        output.writeUInt32NoTag(memidsMemoizedSerializedSize);
      }
      for (int i = 0; i < memids_.size(); i++) {
        output.writeSInt64NoTag(memids_.getLong(i));
      }
      if (getTypesList().size() > 0) {
        output.writeUInt32NoTag(82);
        output.writeUInt32NoTag(typesMemoizedSerializedSize);
      }
      for (int i = 0; i < types_.size(); i++) {
        output.writeEnumNoTag(types_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += proto4.CodedOutputStream.computeInt64Size(1, id_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < keys_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeUInt32SizeNoTag(keys_.getInt(i));
        }
        size += dataSize;
        if (!getKeysList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        keysMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < vals_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeUInt32SizeNoTag(vals_.getInt(i));
        }
        size += dataSize;
        if (!getValsList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        valsMemoizedSerializedSize = dataSize;
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += proto4.CodedOutputStream.computeMessageSize(4, getInfo());
      }
      {
        int dataSize = 0;
        for (int i = 0; i < rolesSid_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeInt32SizeNoTag(rolesSid_.getInt(i));
        }
        size += dataSize;
        if (!getRolesSidList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        rolesSidMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < memids_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeSInt64SizeNoTag(memids_.getLong(i));
        }
        size += dataSize;
        if (!getMemidsList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeInt32SizeNoTag(dataSize);
        }
        memidsMemoizedSerializedSize = dataSize;
      }
      {
        int dataSize = 0;
        for (int i = 0; i < types_.size(); i++) {
          dataSize += proto4.CodedOutputStream.computeEnumSizeNoTag(types_.get(i));
        }
        size += dataSize;
        if (!getTypesList().isEmpty()) {
          size += 1;
          size += proto4.CodedOutputStream.computeUInt32SizeNoTag(dataSize);
        }
        typesMemoizedSerializedSize = dataSize;
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Osmformat.Relation)) {
        return super.equals(obj);
      }
      Osmformat.Relation other = (Osmformat.Relation) obj;

      if (hasId() != other.hasId()) return false;
      if (hasId()) {
        if (getId() != other.getId()) return false;
      }
      if (!getKeysList().equals(other.getKeysList())) return false;
      if (!getValsList().equals(other.getValsList())) return false;
      if (hasInfo() != other.hasInfo()) return false;
      if (hasInfo()) {
        if (!getInfo().equals(other.getInfo())) return false;
      }
      if (!getRolesSidList().equals(other.getRolesSidList())) return false;
      if (!getMemidsList().equals(other.getMemidsList())) return false;
      if (!types_.equals(other.types_)) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasId()) {
        hash = (37 * hash) + ID_FIELD_NUMBER;
        hash = (53 * hash) + proto4.Internal.hashLong(getId());
      }
      if (getKeysCount() > 0) {
        hash = (37 * hash) + KEYS_FIELD_NUMBER;
        hash = (53 * hash) + getKeysList().hashCode();
      }
      if (getValsCount() > 0) {
        hash = (37 * hash) + VALS_FIELD_NUMBER;
        hash = (53 * hash) + getValsList().hashCode();
      }
      if (hasInfo()) {
        hash = (37 * hash) + INFO_FIELD_NUMBER;
        hash = (53 * hash) + getInfo().hashCode();
      }
      if (getRolesSidCount() > 0) {
        hash = (37 * hash) + ROLES_SID_FIELD_NUMBER;
        hash = (53 * hash) + getRolesSidList().hashCode();
      }
      if (getMemidsCount() > 0) {
        hash = (37 * hash) + MEMIDS_FIELD_NUMBER;
        hash = (53 * hash) + getMemidsList().hashCode();
      }
      if (getTypesCount() > 0) {
        hash = (37 * hash) + TYPES_FIELD_NUMBER;
        hash = (53 * hash) + types_.hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Osmformat.Relation parseFrom(java.nio.ByteBuffer data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Relation parseFrom(
        java.nio.ByteBuffer data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Relation parseFrom(proto4.ByteString data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Relation parseFrom(
        proto4.ByteString data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Relation parseFrom(byte[] data)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Osmformat.Relation parseFrom(
        byte[] data, proto4.ExtensionRegistryLite extensionRegistry)
        throws proto4.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Osmformat.Relation parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.Relation parseFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Osmformat.Relation parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Osmformat.Relation parseDelimitedFrom(
        java.io.InputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Osmformat.Relation parseFrom(proto4.CodedInputStream input)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Osmformat.Relation parseFrom(
        proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return proto4.GeneratedMessage.parseWithIOException(PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Osmformat.Relation prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /** Protobuf type {@code Relation} */
    public static final class Builder extends proto4.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:Relation)
        Osmformat.RelationOrBuilder {
      public static final proto4.Descriptors.Descriptor getDescriptor() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Relation_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Relation_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Osmformat.Relation.class, Osmformat.Relation.Builder.class);
      }

      // Construct using Osmformat.Relation.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }

      private void maybeForceBuilderInitialization() {
        if (proto4.GeneratedMessage.alwaysUseFieldBuilders) {
          getInfoFieldBuilder();
        }
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = 0L;
        keys_ = emptyIntList();
        vals_ = emptyIntList();
        info_ = null;
        if (infoBuilder_ != null) {
          infoBuilder_.dispose();
          infoBuilder_ = null;
        }
        rolesSid_ = emptyIntList();
        memids_ = emptyLongList();
        types_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000040);
        return this;
      }

      @Override
      public proto4.Descriptors.Descriptor getDescriptorForType() {
        return Osmformat.internal_static_org_apache_sedona_osm_build_Relation_descriptor;
      }

      @Override
      public Osmformat.Relation getDefaultInstanceForType() {
        return Osmformat.Relation.getDefaultInstance();
      }

      @Override
      public Osmformat.Relation build() {
        Osmformat.Relation result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Osmformat.Relation buildPartial() {
        Osmformat.Relation result = new Osmformat.Relation(this);
        buildPartialRepeatedFields(result);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartialRepeatedFields(Osmformat.Relation result) {
        if (((bitField0_ & 0x00000040) != 0)) {
          types_ = java.util.Collections.unmodifiableList(types_);
          bitField0_ = (bitField0_ & ~0x00000040);
        }
        result.types_ = types_;
      }

      private void buildPartial0(Osmformat.Relation result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          keys_.makeImmutable();
          result.keys_ = keys_;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          vals_.makeImmutable();
          result.vals_ = vals_;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.info_ = infoBuilder_ == null ? info_ : infoBuilder_.build();
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          rolesSid_.makeImmutable();
          result.rolesSid_ = rolesSid_;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          memids_.makeImmutable();
          result.memids_ = memids_;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(proto4.Message other) {
        if (other instanceof Osmformat.Relation) {
          return mergeFrom((Osmformat.Relation) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Osmformat.Relation other) {
        if (other == Osmformat.Relation.getDefaultInstance()) return this;
        if (other.hasId()) {
          setId(other.getId());
        }
        if (!other.keys_.isEmpty()) {
          if (keys_.isEmpty()) {
            keys_ = other.keys_;
            keys_.makeImmutable();
            bitField0_ |= 0x00000002;
          } else {
            ensureKeysIsMutable();
            keys_.addAll(other.keys_);
          }
          onChanged();
        }
        if (!other.vals_.isEmpty()) {
          if (vals_.isEmpty()) {
            vals_ = other.vals_;
            vals_.makeImmutable();
            bitField0_ |= 0x00000004;
          } else {
            ensureValsIsMutable();
            vals_.addAll(other.vals_);
          }
          onChanged();
        }
        if (other.hasInfo()) {
          mergeInfo(other.getInfo());
        }
        if (!other.rolesSid_.isEmpty()) {
          if (rolesSid_.isEmpty()) {
            rolesSid_ = other.rolesSid_;
            rolesSid_.makeImmutable();
            bitField0_ |= 0x00000010;
          } else {
            ensureRolesSidIsMutable();
            rolesSid_.addAll(other.rolesSid_);
          }
          onChanged();
        }
        if (!other.memids_.isEmpty()) {
          if (memids_.isEmpty()) {
            memids_ = other.memids_;
            memids_.makeImmutable();
            bitField0_ |= 0x00000020;
          } else {
            ensureMemidsIsMutable();
            memids_.addAll(other.memids_);
          }
          onChanged();
        }
        if (!other.types_.isEmpty()) {
          if (types_.isEmpty()) {
            types_ = other.types_;
            bitField0_ = (bitField0_ & ~0x00000040);
          } else {
            ensureTypesIsMutable();
            types_.addAll(other.types_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        if (!hasId()) {
          return false;
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8:
                {
                  id_ = input.readInt64();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 8
              case 16:
                {
                  int v = input.readUInt32();
                  ensureKeysIsMutable();
                  keys_.addInt(v);
                  break;
                } // case 16
              case 18:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureKeysIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    keys_.addInt(input.readUInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 18
              case 24:
                {
                  int v = input.readUInt32();
                  ensureValsIsMutable();
                  vals_.addInt(v);
                  break;
                } // case 24
              case 26:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureValsIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    vals_.addInt(input.readUInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 26
              case 34:
                {
                  input.readMessage(getInfoFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000008;
                  break;
                } // case 34
              case 64:
                {
                  int v = input.readInt32();
                  ensureRolesSidIsMutable();
                  rolesSid_.addInt(v);
                  break;
                } // case 64
              case 66:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureRolesSidIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    rolesSid_.addInt(input.readInt32());
                  }
                  input.popLimit(limit);
                  break;
                } // case 66
              case 72:
                {
                  long v = input.readSInt64();
                  ensureMemidsIsMutable();
                  memids_.addLong(v);
                  break;
                } // case 72
              case 74:
                {
                  int length = input.readRawVarint32();
                  int limit = input.pushLimit(length);
                  ensureMemidsIsMutable();
                  while (input.getBytesUntilLimit() > 0) {
                    memids_.addLong(input.readSInt64());
                  }
                  input.popLimit(limit);
                  break;
                } // case 74
              case 80:
                {
                  int tmpRaw = input.readEnum();
                  Osmformat.Relation.MemberType tmpValue =
                      Osmformat.Relation.MemberType.forNumber(tmpRaw);
                  if (tmpValue == null) {
                    mergeUnknownVarintField(10, tmpRaw);
                  } else {
                    ensureTypesIsMutable();
                    types_.add(tmpRaw);
                  }
                  break;
                } // case 80
              case 82:
                {
                  int length = input.readRawVarint32();
                  int oldLimit = input.pushLimit(length);
                  while (input.getBytesUntilLimit() > 0) {
                    int tmpRaw = input.readEnum();
                    Osmformat.Relation.MemberType tmpValue =
                        Osmformat.Relation.MemberType.forNumber(tmpRaw);
                    if (tmpValue == null) {
                      mergeUnknownVarintField(10, tmpRaw);
                    } else {
                      ensureTypesIsMutable();
                      types_.add(tmpRaw);
                    }
                  }
                  input.popLimit(oldLimit);
                  break;
                } // case 82
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (proto4.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private long id_;

      /**
       * <code>required int64 id = 1;</code>
       *
       * @return Whether the id field is set.
       */
      @Override
      public boolean hasId() {
        return ((bitField0_ & 0x00000001) != 0);
      }

      /**
       * <code>required int64 id = 1;</code>
       *
       * @return The id.
       */
      @Override
      public long getId() {
        return id_;
      }

      /**
       * <code>required int64 id = 1;</code>
       *
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(long value) {

        id_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      /**
       * <code>required int64 id = 1;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = 0L;
        onChanged();
        return this;
      }

      private proto4.Internal.IntList keys_ = emptyIntList();

      private void ensureKeysIsMutable() {
        if (!keys_.isModifiable()) {
          keys_ = makeMutableCopy(keys_);
        }
        bitField0_ |= 0x00000002;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return A list containing the keys.
       */
      public java.util.List<Integer> getKeysList() {
        keys_.makeImmutable();
        return keys_;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return The count of keys.
       */
      public int getKeysCount() {
        return keys_.size();
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The keys at the given index.
       */
      public int getKeys(int index) {
        return keys_.getInt(index);
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The keys to set.
       * @return This builder for chaining.
       */
      public Builder setKeys(int index, int value) {

        ensureKeysIsMutable();
        keys_.setInt(index, value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param value The keys to add.
       * @return This builder for chaining.
       */
      public Builder addKeys(int value) {

        ensureKeysIsMutable();
        keys_.addInt(value);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @param values The keys to add.
       * @return This builder for chaining.
       */
      public Builder addAllKeys(Iterable<? extends Integer> values) {
        ensureKeysIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, keys_);
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays.
       * </pre>
       *
       * <code>repeated uint32 keys = 2 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearKeys() {
        keys_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }

      private proto4.Internal.IntList vals_ = emptyIntList();

      private void ensureValsIsMutable() {
        if (!vals_.isModifiable()) {
          vals_ = makeMutableCopy(vals_);
        }
        bitField0_ |= 0x00000004;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return A list containing the vals.
       */
      public java.util.List<Integer> getValsList() {
        vals_.makeImmutable();
        return vals_;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return The count of vals.
       */
      public int getValsCount() {
        return vals_.size();
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The vals at the given index.
       */
      public int getVals(int index) {
        return vals_.getInt(index);
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The vals to set.
       * @return This builder for chaining.
       */
      public Builder setVals(int index, int value) {

        ensureValsIsMutable();
        vals_.setInt(index, value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param value The vals to add.
       * @return This builder for chaining.
       */
      public Builder addVals(int value) {

        ensureValsIsMutable();
        vals_.addInt(value);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @param values The vals to add.
       * @return This builder for chaining.
       */
      public Builder addAllVals(Iterable<? extends Integer> values) {
        ensureValsIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, vals_);
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 vals = 3 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearVals() {
        vals_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
        return this;
      }

      private Osmformat.Info info_;
      private proto4.SingleFieldBuilder<
              Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>
          infoBuilder_;

      /**
       * <code>optional .Info info = 4;</code>
       *
       * @return Whether the info field is set.
       */
      public boolean hasInfo() {
        return ((bitField0_ & 0x00000008) != 0);
      }

      /**
       * <code>optional .Info info = 4;</code>
       *
       * @return The info.
       */
      public Osmformat.Info getInfo() {
        if (infoBuilder_ == null) {
          return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
        } else {
          return infoBuilder_.getMessage();
        }
      }

      /** <code>optional .Info info = 4;</code> */
      public Builder setInfo(Osmformat.Info value) {
        if (infoBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          info_ = value;
        } else {
          infoBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /** <code>optional .Info info = 4;</code> */
      public Builder setInfo(Osmformat.Info.Builder builderForValue) {
        if (infoBuilder_ == null) {
          info_ = builderForValue.build();
        } else {
          infoBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }

      /** <code>optional .Info info = 4;</code> */
      public Builder mergeInfo(Osmformat.Info value) {
        if (infoBuilder_ == null) {
          if (((bitField0_ & 0x00000008) != 0)
              && info_ != null
              && info_ != Osmformat.Info.getDefaultInstance()) {
            getInfoBuilder().mergeFrom(value);
          } else {
            info_ = value;
          }
        } else {
          infoBuilder_.mergeFrom(value);
        }
        if (info_ != null) {
          bitField0_ |= 0x00000008;
          onChanged();
        }
        return this;
      }

      /** <code>optional .Info info = 4;</code> */
      public Builder clearInfo() {
        bitField0_ = (bitField0_ & ~0x00000008);
        info_ = null;
        if (infoBuilder_ != null) {
          infoBuilder_.dispose();
          infoBuilder_ = null;
        }
        onChanged();
        return this;
      }

      /** <code>optional .Info info = 4;</code> */
      public Osmformat.Info.Builder getInfoBuilder() {
        bitField0_ |= 0x00000008;
        onChanged();
        return getInfoFieldBuilder().getBuilder();
      }

      /** <code>optional .Info info = 4;</code> */
      public Osmformat.InfoOrBuilder getInfoOrBuilder() {
        if (infoBuilder_ != null) {
          return infoBuilder_.getMessageOrBuilder();
        } else {
          return info_ == null ? Osmformat.Info.getDefaultInstance() : info_;
        }
      }

      /** <code>optional .Info info = 4;</code> */
      private proto4.SingleFieldBuilder<
              Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>
          getInfoFieldBuilder() {
        if (infoBuilder_ == null) {
          infoBuilder_ =
              new proto4.SingleFieldBuilder<
                  Osmformat.Info, Osmformat.Info.Builder, Osmformat.InfoOrBuilder>(
                  getInfo(), getParentForChildren(), isClean());
          info_ = null;
        }
        return infoBuilder_;
      }

      private proto4.Internal.IntList rolesSid_ = emptyIntList();

      private void ensureRolesSidIsMutable() {
        if (!rolesSid_.isModifiable()) {
          rolesSid_ = makeMutableCopy(rolesSid_);
        }
        bitField0_ |= 0x00000010;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays
       * </pre>
       *
       * <code>repeated int32 roles_sid = 8 [packed = true];</code>
       *
       * @return A list containing the rolesSid.
       */
      public java.util.List<Integer> getRolesSidList() {
        rolesSid_.makeImmutable();
        return rolesSid_;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays
       * </pre>
       *
       * <code>repeated int32 roles_sid = 8 [packed = true];</code>
       *
       * @return The count of rolesSid.
       */
      public int getRolesSidCount() {
        return rolesSid_.size();
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays
       * </pre>
       *
       * <code>repeated int32 roles_sid = 8 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The rolesSid at the given index.
       */
      public int getRolesSid(int index) {
        return rolesSid_.getInt(index);
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays
       * </pre>
       *
       * <code>repeated int32 roles_sid = 8 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The rolesSid to set.
       * @return This builder for chaining.
       */
      public Builder setRolesSid(int index, int value) {

        ensureRolesSidIsMutable();
        rolesSid_.setInt(index, value);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays
       * </pre>
       *
       * <code>repeated int32 roles_sid = 8 [packed = true];</code>
       *
       * @param value The rolesSid to add.
       * @return This builder for chaining.
       */
      public Builder addRolesSid(int value) {

        ensureRolesSidIsMutable();
        rolesSid_.addInt(value);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays
       * </pre>
       *
       * <code>repeated int32 roles_sid = 8 [packed = true];</code>
       *
       * @param values The rolesSid to add.
       * @return This builder for chaining.
       */
      public Builder addAllRolesSid(Iterable<? extends Integer> values) {
        ensureRolesSidIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, rolesSid_);
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * Parallel arrays
       * </pre>
       *
       * <code>repeated int32 roles_sid = 8 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearRolesSid() {
        rolesSid_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000010);
        onChanged();
        return this;
      }

      private proto4.Internal.LongList memids_ = emptyLongList();

      private void ensureMemidsIsMutable() {
        if (!memids_.isModifiable()) {
          memids_ = makeMutableCopy(memids_);
        }
        bitField0_ |= 0x00000020;
      }

      /**
       *
       *
       * <pre>
       * DELTA encoded
       * </pre>
       *
       * <code>repeated sint64 memids = 9 [packed = true];</code>
       *
       * @return A list containing the memids.
       */
      public java.util.List<Long> getMemidsList() {
        memids_.makeImmutable();
        return memids_;
      }

      /**
       *
       *
       * <pre>
       * DELTA encoded
       * </pre>
       *
       * <code>repeated sint64 memids = 9 [packed = true];</code>
       *
       * @return The count of memids.
       */
      public int getMemidsCount() {
        return memids_.size();
      }

      /**
       *
       *
       * <pre>
       * DELTA encoded
       * </pre>
       *
       * <code>repeated sint64 memids = 9 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The memids at the given index.
       */
      public long getMemids(int index) {
        return memids_.getLong(index);
      }

      /**
       *
       *
       * <pre>
       * DELTA encoded
       * </pre>
       *
       * <code>repeated sint64 memids = 9 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The memids to set.
       * @return This builder for chaining.
       */
      public Builder setMemids(int index, long value) {

        ensureMemidsIsMutable();
        memids_.setLong(index, value);
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA encoded
       * </pre>
       *
       * <code>repeated sint64 memids = 9 [packed = true];</code>
       *
       * @param value The memids to add.
       * @return This builder for chaining.
       */
      public Builder addMemids(long value) {

        ensureMemidsIsMutable();
        memids_.addLong(value);
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA encoded
       * </pre>
       *
       * <code>repeated sint64 memids = 9 [packed = true];</code>
       *
       * @param values The memids to add.
       * @return This builder for chaining.
       */
      public Builder addAllMemids(Iterable<? extends Long> values) {
        ensureMemidsIsMutable();
        proto4.AbstractMessageLite.Builder.addAll(values, memids_);
        bitField0_ |= 0x00000020;
        onChanged();
        return this;
      }

      /**
       *
       *
       * <pre>
       * DELTA encoded
       * </pre>
       *
       * <code>repeated sint64 memids = 9 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearMemids() {
        memids_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000020);
        onChanged();
        return this;
      }

      private java.util.List<Integer> types_ = java.util.Collections.emptyList();

      private void ensureTypesIsMutable() {
        if (!((bitField0_ & 0x00000040) != 0)) {
          types_ = new java.util.ArrayList<Integer>(types_);
          bitField0_ |= 0x00000040;
        }
      }

      /**
       * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
       *
       * @return A list containing the types.
       */
      public java.util.List<Osmformat.Relation.MemberType> getTypesList() {
        return new proto4.Internal.ListAdapter<Integer, Osmformat.Relation.MemberType>(
            types_, types_converter_);
      }

      /**
       * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
       *
       * @return The count of types.
       */
      public int getTypesCount() {
        return types_.size();
      }

      /**
       * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
       *
       * @param index The index of the element to return.
       * @return The types at the given index.
       */
      public Osmformat.Relation.MemberType getTypes(int index) {
        return types_converter_.convert(types_.get(index));
      }

      /**
       * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
       *
       * @param index The index to set the value at.
       * @param value The types to set.
       * @return This builder for chaining.
       */
      public Builder setTypes(int index, Osmformat.Relation.MemberType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureTypesIsMutable();
        types_.set(index, value.getNumber());
        onChanged();
        return this;
      }

      /**
       * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
       *
       * @param value The types to add.
       * @return This builder for chaining.
       */
      public Builder addTypes(Osmformat.Relation.MemberType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        ensureTypesIsMutable();
        types_.add(value.getNumber());
        onChanged();
        return this;
      }

      /**
       * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
       *
       * @param values The types to add.
       * @return This builder for chaining.
       */
      public Builder addAllTypes(Iterable<? extends Osmformat.Relation.MemberType> values) {
        ensureTypesIsMutable();
        for (Osmformat.Relation.MemberType value : values) {
          types_.add(value.getNumber());
        }
        onChanged();
        return this;
      }

      /**
       * <code>repeated .Relation.MemberType types = 10 [packed = true];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearTypes() {
        types_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000040);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:Relation)
    }

    // @@protoc_insertion_point(class_scope:Relation)
    private static final Osmformat.Relation DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Osmformat.Relation();
    }

    public static Osmformat.Relation getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final proto4.Parser<Relation> PARSER =
        new proto4.AbstractParser<Relation>() {
          @Override
          public Relation parsePartialFrom(
              proto4.CodedInputStream input, proto4.ExtensionRegistryLite extensionRegistry)
              throws proto4.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (proto4.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (proto4.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new proto4.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static proto4.Parser<Relation> parser() {
      return PARSER;
    }

    @Override
    public proto4.Parser<Relation> getParserForType() {
      return PARSER;
    }

    @Override
    public Osmformat.Relation getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_HeaderBlock_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_HeaderBlock_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_HeaderBBox_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_HeaderBBox_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_PrimitiveBlock_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_PrimitiveBlock_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_PrimitiveGroup_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_PrimitiveGroup_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_StringTable_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_StringTable_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_Info_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_Info_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_DenseInfo_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_DenseInfo_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_ChangeSet_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_ChangeSet_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_Node_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_Node_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_DenseNodes_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_DenseNodes_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_Way_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_Way_fieldAccessorTable;
  private static final proto4.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_Relation_descriptor;
  private static final proto4.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_Relation_fieldAccessorTable;

  public static proto4.Descriptors.FileDescriptor getDescriptor() {
    return descriptor;
  }

  private static proto4.Descriptors.FileDescriptor descriptor;

  static {
    String[] descriptorData = {
      "\n1main/java/org/apache/sedona/proto/osmf"
          + "ormat.proto\022\033org.apache.sedona.osm.build"
          + "\"\234\002\n\013HeaderBlock\0225\n\004bbox\030\001 \001(\0132\'.org.apa"
          + "che.sedona.osm.build.HeaderBBox\022\031\n\021requi"
          + "red_features\030\004 \003(\t\022\031\n\021optional_features\030"
          + "\005 \003(\t\022\026\n\016writingprogram\030\020 \001(\t\022\016\n\006source\030"
          + "\021 \001(\t\022%\n\035osmosis_replication_timestamp\030 "
          + " \001(\003\022+\n#osmosis_replication_sequence_num"
          + "ber\030! \001(\003\022$\n\034osmosis_replication_base_ur"
          + "l\030\" \001(\t\"F\n\nHeaderBBox\022\014\n\004left\030\001 \002(\022\022\r\n\005r"
          + "ight\030\002 \002(\022\022\013\n\003top\030\003 \002(\022\022\016\n\006bottom\030\004 \002(\022\""
          + "\374\001\n\016PrimitiveBlock\022=\n\013stringtable\030\001 \002(\0132"
          + "(.StringTabl"
          + "e\022C\n\016primitivegroup\030\002 \003(\0132+.org.apache.s"
          + "edona.osm.build.PrimitiveGroup\022\030\n\013granul"
          + "arity\030\021 \001(\005:\003100\022\025\n\nlat_offset\030\023 \001(\003:\0010\022"
          + "\025\n\nlon_offset\030\024 \001(\003:\0010\022\036\n\020date_granulari"
          + "ty\030\022 \001(\005:\0041000\"\240\002\n\016PrimitiveGroup\0220\n\005nod"
          + "es\030\001 \003(\0132!.N"
          + "ode\0226\n\005dense\030\002 \001(\0132\'.org.apache.sedona.o"
          + "sm.build.DenseNodes\022.\n\004ways\030\003 \003(\0132 .org."
          + "apache.sedona.osm.build.Way\0228\n\trelations"
          + "\030\004 \003(\0132%.Rel"
          + "ation\022:\n\nchangesets\030\005 \003(\0132&.org.apache.s"
          + "edona.osm.build.ChangeSet\"\030\n\013StringTable"
          + "\022\t\n\001s\030\001 \003(\014\"q\n\004Info\022\023\n\007version\030\001 \001(\005:\002-1"
          + "\022\021\n\ttimestamp\030\002 \001(\003\022\021\n\tchangeset\030\003 \001(\003\022\013"
          + "\n\003uid\030\004 \001(\005\022\020\n\010user_sid\030\005 \001(\r\022\017\n\007visible"
          + "\030\006 \001(\010\"\212\001\n\tDenseInfo\022\023\n\007version\030\001 \003(\005B\002\020"
          + "\001\022\025\n\ttimestamp\030\002 \003(\022B\002\020\001\022\025\n\tchangeset\030\003 "
          + "\003(\022B\002\020\001\022\017\n\003uid\030\004 \003(\021B\002\020\001\022\024\n\010user_sid\030\005 \003"
          + "(\021B\002\020\001\022\023\n\007visible\030\006 \003(\010B\002\020\001\"\027\n\tChangeSet"
          + "\022\n\n\002id\030\001 \002(\003\"\201\001\n\004Node\022\n\n\002id\030\001 \002(\022\022\020\n\004key"
          + "s\030\002 \003(\rB\002\020\001\022\020\n\004vals\030\003 \003(\rB\002\020\001\022/\n\004info\030\004 "
          + "\001(\0132!.Info\022\013"
          + "\n\003lat\030\010 \002(\022\022\013\n\003lon\030\t \002(\022\"\220\001\n\nDenseNodes\022"
          + "\016\n\002id\030\001 \003(\022B\002\020\001\0229\n\tdenseinfo\030\005 \001(\0132&.org"
          + ".apache.sedona.osm.build.DenseInfo\022\017\n\003la"
          + "t\030\010 \003(\022B\002\020\001\022\017\n\003lon\030\t \003(\022B\002\020\001\022\025\n\tkeys_val"
          + "s\030\n \003(\005B\002\020\001\"x\n\003Way\022\n\n\002id\030\001 \002(\003\022\020\n\004keys\030\002"
          + " \003(\rB\002\020\001\022\020\n\004vals\030\003 \003(\rB\002\020\001\022/\n\004info\030\004 \001(\013"
          + "2!.Info\022\020\n\004r"
          + "efs\030\010 \003(\022B\002\020\001\"\212\002\n\010Relation\022\n\n\002id\030\001 \002(\003\022\020"
          + "\n\004keys\030\002 \003(\rB\002\020\001\022\020\n\004vals\030\003 \003(\rB\002\020\001\022/\n\004in"
          + "fo\030\004 \001(\0132!.I"
          + "nfo\022\025\n\troles_sid\030\010 \003(\005B\002\020\001\022\022\n\006memids\030\t \003"
          + "(\022B\002\020\001\022C\n\005types\030\n \003(\01620.org.apache.sedon"
          + "a.osm.build.Relation.MemberTypeB\002\020\001\"-\n\nM"
          + "emberType\022\010\n\004NODE\020\000\022\007\n\003WAY\020\001\022\014\n\010RELATION"
          + "\020\002B\002H\003"
    };
    descriptor =
        proto4.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
            descriptorData, new proto4.Descriptors.FileDescriptor[] {});
    internal_static_org_apache_sedona_osm_build_HeaderBlock_descriptor =
        getDescriptor().getMessageTypes().get(0);
    internal_static_org_apache_sedona_osm_build_HeaderBlock_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_HeaderBlock_descriptor,
            new String[] {
              "Bbox",
              "RequiredFeatures",
              "OptionalFeatures",
              "Writingprogram",
              "Source",
              "OsmosisReplicationTimestamp",
              "OsmosisReplicationSequenceNumber",
              "OsmosisReplicationBaseUrl",
            });
    internal_static_org_apache_sedona_osm_build_HeaderBBox_descriptor =
        getDescriptor().getMessageTypes().get(1);
    internal_static_org_apache_sedona_osm_build_HeaderBBox_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_HeaderBBox_descriptor,
            new String[] {
              "Left", "Right", "Top", "Bottom",
            });
    internal_static_org_apache_sedona_osm_build_PrimitiveBlock_descriptor =
        getDescriptor().getMessageTypes().get(2);
    internal_static_org_apache_sedona_osm_build_PrimitiveBlock_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_PrimitiveBlock_descriptor,
            new String[] {
              "Stringtable",
              "Primitivegroup",
              "Granularity",
              "LatOffset",
              "LonOffset",
              "DateGranularity",
            });
    internal_static_org_apache_sedona_osm_build_PrimitiveGroup_descriptor =
        getDescriptor().getMessageTypes().get(3);
    internal_static_org_apache_sedona_osm_build_PrimitiveGroup_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_PrimitiveGroup_descriptor,
            new String[] {
              "Nodes", "Dense", "Ways", "Relations", "Changesets",
            });
    internal_static_org_apache_sedona_osm_build_StringTable_descriptor =
        getDescriptor().getMessageTypes().get(4);
    internal_static_org_apache_sedona_osm_build_StringTable_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_StringTable_descriptor,
            new String[] {
              "S",
            });
    internal_static_org_apache_sedona_osm_build_Info_descriptor =
        getDescriptor().getMessageTypes().get(5);
    internal_static_org_apache_sedona_osm_build_Info_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_Info_descriptor,
            new String[] {
              "Version", "Timestamp", "Changeset", "Uid", "UserSid", "Visible",
            });
    internal_static_org_apache_sedona_osm_build_DenseInfo_descriptor =
        getDescriptor().getMessageTypes().get(6);
    internal_static_org_apache_sedona_osm_build_DenseInfo_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_DenseInfo_descriptor,
            new String[] {
              "Version", "Timestamp", "Changeset", "Uid", "UserSid", "Visible",
            });
    internal_static_org_apache_sedona_osm_build_ChangeSet_descriptor =
        getDescriptor().getMessageTypes().get(7);
    internal_static_org_apache_sedona_osm_build_ChangeSet_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_ChangeSet_descriptor,
            new String[] {
              "Id",
            });
    internal_static_org_apache_sedona_osm_build_Node_descriptor =
        getDescriptor().getMessageTypes().get(8);
    internal_static_org_apache_sedona_osm_build_Node_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_Node_descriptor,
            new String[] {
              "Id", "Keys", "Vals", "Info", "Lat", "Lon",
            });
    internal_static_org_apache_sedona_osm_build_DenseNodes_descriptor =
        getDescriptor().getMessageTypes().get(9);
    internal_static_org_apache_sedona_osm_build_DenseNodes_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_DenseNodes_descriptor,
            new String[] {
              "Id", "Denseinfo", "Lat", "Lon", "KeysVals",
            });
    internal_static_org_apache_sedona_osm_build_Way_descriptor =
        getDescriptor().getMessageTypes().get(10);
    internal_static_org_apache_sedona_osm_build_Way_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_Way_descriptor,
            new String[] {
              "Id", "Keys", "Vals", "Info", "Refs",
            });
    internal_static_org_apache_sedona_osm_build_Relation_descriptor =
        getDescriptor().getMessageTypes().get(11);
    internal_static_org_apache_sedona_osm_build_Relation_fieldAccessorTable =
        new proto4.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_Relation_descriptor,
            new String[] {
              "Id", "Keys", "Vals", "Info", "RolesSid", "Memids", "Types",
            });
    descriptor.resolveAllFeaturesImmutable();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
