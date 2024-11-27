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

public final class Fileformat {
  private Fileformat() {}

  static {
    com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
        com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC,
        /* major= */ 4,
        /* minor= */ 27,
        /* patch= */ 0,
        /* suffix= */ "",
        Fileformat.class.getName());
  }

  public static void registerAllExtensions(com.google.protobuf.ExtensionRegistryLite registry) {}

  public static void registerAllExtensions(com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions((com.google.protobuf.ExtensionRegistryLite) registry);
  }

  public interface BlobOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:Blob)
      com.google.protobuf.MessageOrBuilder {

    /**
     *
     *
     * <pre>
     * No compression
     * </pre>
     *
     * <code>optional bytes raw = 1;</code>
     *
     * @return Whether the raw field is set.
     */
    boolean hasRaw();
    /**
     *
     *
     * <pre>
     * No compression
     * </pre>
     *
     * <code>optional bytes raw = 1;</code>
     *
     * @return The raw.
     */
    com.google.protobuf.ByteString getRaw();

    /**
     *
     *
     * <pre>
     * When compressed, the uncompressed size
     * </pre>
     *
     * <code>optional int32 raw_size = 2;</code>
     *
     * @return Whether the rawSize field is set.
     */
    boolean hasRawSize();
    /**
     *
     *
     * <pre>
     * When compressed, the uncompressed size
     * </pre>
     *
     * <code>optional int32 raw_size = 2;</code>
     *
     * @return The rawSize.
     */
    int getRawSize();

    /**
     *
     *
     * <pre>
     * Possible compressed versions of the data.
     * </pre>
     *
     * <code>optional bytes zlib_data = 3;</code>
     *
     * @return Whether the zlibData field is set.
     */
    boolean hasZlibData();
    /**
     *
     *
     * <pre>
     * Possible compressed versions of the data.
     * </pre>
     *
     * <code>optional bytes zlib_data = 3;</code>
     *
     * @return The zlibData.
     */
    com.google.protobuf.ByteString getZlibData();

    /**
     *
     *
     * <pre>
     * PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
     * </pre>
     *
     * <code>optional bytes lzma_data = 4;</code>
     *
     * @return Whether the lzmaData field is set.
     */
    boolean hasLzmaData();
    /**
     *
     *
     * <pre>
     * PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
     * </pre>
     *
     * <code>optional bytes lzma_data = 4;</code>
     *
     * @return The lzmaData.
     */
    com.google.protobuf.ByteString getLzmaData();

    /**
     *
     *
     * <pre>
     * Formerly used for bzip2 compressed data. Depreciated in 2010.
     * </pre>
     *
     * <code>optional bytes OBSOLETE_bzip2_data = 5 [deprecated = true];</code>
     *
     * @deprecated Blob.OBSOLETE_bzip2_data is deprecated. See
     *     main/java/org/apache/sedona/proto/fileformat.proto;l=32
     * @return Whether the oBSOLETEBzip2Data field is set.
     */
    @Deprecated
    boolean hasOBSOLETEBzip2Data();
    /**
     *
     *
     * <pre>
     * Formerly used for bzip2 compressed data. Depreciated in 2010.
     * </pre>
     *
     * <code>optional bytes OBSOLETE_bzip2_data = 5 [deprecated = true];</code>
     *
     * @deprecated Blob.OBSOLETE_bzip2_data is deprecated. See
     *     main/java/org/apache/sedona/proto/fileformat.proto;l=32
     * @return The oBSOLETEBzip2Data.
     */
    @Deprecated
    com.google.protobuf.ByteString getOBSOLETEBzip2Data();
  }
  /** Protobuf type {@code Blob} */
  public static final class Blob extends com.google.protobuf.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:Blob)
      BlobOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
          com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          Blob.class.getName());
    }
    // Use Blob.newBuilder() to construct.
    private Blob(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private Blob() {
      raw_ = com.google.protobuf.ByteString.EMPTY;
      zlibData_ = com.google.protobuf.ByteString.EMPTY;
      lzmaData_ = com.google.protobuf.ByteString.EMPTY;
      oBSOLETEBzip2Data_ = com.google.protobuf.ByteString.EMPTY;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return Fileformat.internal_static_org_apache_sedona_osm_build_Blob_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Fileformat.internal_static_org_apache_sedona_osm_build_Blob_fieldAccessorTable
          .ensureFieldAccessorsInitialized(Fileformat.Blob.class, Fileformat.Blob.Builder.class);
    }

    private int bitField0_;
    public static final int RAW_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString raw_ = com.google.protobuf.ByteString.EMPTY;
    /**
     *
     *
     * <pre>
     * No compression
     * </pre>
     *
     * <code>optional bytes raw = 1;</code>
     *
     * @return Whether the raw field is set.
     */
    @Override
    public boolean hasRaw() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     *
     *
     * <pre>
     * No compression
     * </pre>
     *
     * <code>optional bytes raw = 1;</code>
     *
     * @return The raw.
     */
    @Override
    public com.google.protobuf.ByteString getRaw() {
      return raw_;
    }

    public static final int RAW_SIZE_FIELD_NUMBER = 2;
    private int rawSize_ = 0;
    /**
     *
     *
     * <pre>
     * When compressed, the uncompressed size
     * </pre>
     *
     * <code>optional int32 raw_size = 2;</code>
     *
     * @return Whether the rawSize field is set.
     */
    @Override
    public boolean hasRawSize() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     *
     *
     * <pre>
     * When compressed, the uncompressed size
     * </pre>
     *
     * <code>optional int32 raw_size = 2;</code>
     *
     * @return The rawSize.
     */
    @Override
    public int getRawSize() {
      return rawSize_;
    }

    public static final int ZLIB_DATA_FIELD_NUMBER = 3;
    private com.google.protobuf.ByteString zlibData_ = com.google.protobuf.ByteString.EMPTY;
    /**
     *
     *
     * <pre>
     * Possible compressed versions of the data.
     * </pre>
     *
     * <code>optional bytes zlib_data = 3;</code>
     *
     * @return Whether the zlibData field is set.
     */
    @Override
    public boolean hasZlibData() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     *
     *
     * <pre>
     * Possible compressed versions of the data.
     * </pre>
     *
     * <code>optional bytes zlib_data = 3;</code>
     *
     * @return The zlibData.
     */
    @Override
    public com.google.protobuf.ByteString getZlibData() {
      return zlibData_;
    }

    public static final int LZMA_DATA_FIELD_NUMBER = 4;
    private com.google.protobuf.ByteString lzmaData_ = com.google.protobuf.ByteString.EMPTY;
    /**
     *
     *
     * <pre>
     * PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
     * </pre>
     *
     * <code>optional bytes lzma_data = 4;</code>
     *
     * @return Whether the lzmaData field is set.
     */
    @Override
    public boolean hasLzmaData() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     *
     *
     * <pre>
     * PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
     * </pre>
     *
     * <code>optional bytes lzma_data = 4;</code>
     *
     * @return The lzmaData.
     */
    @Override
    public com.google.protobuf.ByteString getLzmaData() {
      return lzmaData_;
    }

    public static final int OBSOLETE_BZIP2_DATA_FIELD_NUMBER = 5;
    private com.google.protobuf.ByteString oBSOLETEBzip2Data_ =
        com.google.protobuf.ByteString.EMPTY;
    /**
     *
     *
     * <pre>
     * Formerly used for bzip2 compressed data. Depreciated in 2010.
     * </pre>
     *
     * <code>optional bytes OBSOLETE_bzip2_data = 5 [deprecated = true];</code>
     *
     * @deprecated Blob.OBSOLETE_bzip2_data is deprecated. See
     *     main/java/org/apache/sedona/proto/fileformat.proto;l=32
     * @return Whether the oBSOLETEBzip2Data field is set.
     */
    @Override
    @Deprecated
    public boolean hasOBSOLETEBzip2Data() {
      return ((bitField0_ & 0x00000010) != 0);
    }
    /**
     *
     *
     * <pre>
     * Formerly used for bzip2 compressed data. Depreciated in 2010.
     * </pre>
     *
     * <code>optional bytes OBSOLETE_bzip2_data = 5 [deprecated = true];</code>
     *
     * @deprecated Blob.OBSOLETE_bzip2_data is deprecated. See
     *     main/java/org/apache/sedona/proto/fileformat.proto;l=32
     * @return The oBSOLETEBzip2Data.
     */
    @Override
    @Deprecated
    public com.google.protobuf.ByteString getOBSOLETEBzip2Data() {
      return oBSOLETEBzip2Data_;
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
    public void writeTo(com.google.protobuf.CodedOutputStream output) throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeBytes(1, raw_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeInt32(2, rawSize_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeBytes(3, zlibData_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeBytes(4, lzmaData_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeBytes(5, oBSOLETEBzip2Data_);
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.CodedOutputStream.computeBytesSize(1, raw_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream.computeInt32Size(2, rawSize_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream.computeBytesSize(3, zlibData_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += com.google.protobuf.CodedOutputStream.computeBytesSize(4, lzmaData_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += com.google.protobuf.CodedOutputStream.computeBytesSize(5, oBSOLETEBzip2Data_);
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
      if (!(obj instanceof Fileformat.Blob)) {
        return super.equals(obj);
      }
      Fileformat.Blob other = (Fileformat.Blob) obj;

      if (hasRaw() != other.hasRaw()) return false;
      if (hasRaw()) {
        if (!getRaw().equals(other.getRaw())) return false;
      }
      if (hasRawSize() != other.hasRawSize()) return false;
      if (hasRawSize()) {
        if (getRawSize() != other.getRawSize()) return false;
      }
      if (hasZlibData() != other.hasZlibData()) return false;
      if (hasZlibData()) {
        if (!getZlibData().equals(other.getZlibData())) return false;
      }
      if (hasLzmaData() != other.hasLzmaData()) return false;
      if (hasLzmaData()) {
        if (!getLzmaData().equals(other.getLzmaData())) return false;
      }
      if (hasOBSOLETEBzip2Data() != other.hasOBSOLETEBzip2Data()) return false;
      if (hasOBSOLETEBzip2Data()) {
        if (!getOBSOLETEBzip2Data().equals(other.getOBSOLETEBzip2Data())) return false;
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
      if (hasRaw()) {
        hash = (37 * hash) + RAW_FIELD_NUMBER;
        hash = (53 * hash) + getRaw().hashCode();
      }
      if (hasRawSize()) {
        hash = (37 * hash) + RAW_SIZE_FIELD_NUMBER;
        hash = (53 * hash) + getRawSize();
      }
      if (hasZlibData()) {
        hash = (37 * hash) + ZLIB_DATA_FIELD_NUMBER;
        hash = (53 * hash) + getZlibData().hashCode();
      }
      if (hasLzmaData()) {
        hash = (37 * hash) + LZMA_DATA_FIELD_NUMBER;
        hash = (53 * hash) + getLzmaData().hashCode();
      }
      if (hasOBSOLETEBzip2Data()) {
        hash = (37 * hash) + OBSOLETE_BZIP2_DATA_FIELD_NUMBER;
        hash = (53 * hash) + getOBSOLETEBzip2Data().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Fileformat.Blob parseFrom(java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Fileformat.Blob parseFrom(
        java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Fileformat.Blob parseFrom(com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Fileformat.Blob parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Fileformat.Blob parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Fileformat.Blob parseFrom(
        byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Fileformat.Blob parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Fileformat.Blob parseFrom(
        java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Fileformat.Blob parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Fileformat.Blob parseDelimitedFrom(
        java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Fileformat.Blob parseFrom(com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Fileformat.Blob parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Fileformat.Blob prototype) {
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
    /** Protobuf type {@code Blob} */
    public static final class Builder extends com.google.protobuf.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:Blob)
        Fileformat.BlobOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return Fileformat.internal_static_org_apache_sedona_osm_build_Blob_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Fileformat.internal_static_org_apache_sedona_osm_build_Blob_fieldAccessorTable
            .ensureFieldAccessorsInitialized(Fileformat.Blob.class, Fileformat.Blob.Builder.class);
      }

      // Construct using Fileformat.Blob.newBuilder()
      private Builder() {}

      private Builder(BuilderParent parent) {
        super(parent);
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        raw_ = com.google.protobuf.ByteString.EMPTY;
        rawSize_ = 0;
        zlibData_ = com.google.protobuf.ByteString.EMPTY;
        lzmaData_ = com.google.protobuf.ByteString.EMPTY;
        oBSOLETEBzip2Data_ = com.google.protobuf.ByteString.EMPTY;
        return this;
      }

      @Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return Fileformat.internal_static_org_apache_sedona_osm_build_Blob_descriptor;
      }

      @Override
      public Fileformat.Blob getDefaultInstanceForType() {
        return Fileformat.Blob.getDefaultInstance();
      }

      @Override
      public Fileformat.Blob build() {
        Fileformat.Blob result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Fileformat.Blob buildPartial() {
        Fileformat.Blob result = new Fileformat.Blob(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Fileformat.Blob result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.raw_ = raw_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.rawSize_ = rawSize_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.zlibData_ = zlibData_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.lzmaData_ = lzmaData_;
          to_bitField0_ |= 0x00000008;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.oBSOLETEBzip2Data_ = oBSOLETEBzip2Data_;
          to_bitField0_ |= 0x00000010;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof Fileformat.Blob) {
          return mergeFrom((Fileformat.Blob) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Fileformat.Blob other) {
        if (other == Fileformat.Blob.getDefaultInstance()) return this;
        if (other.hasRaw()) {
          setRaw(other.getRaw());
        }
        if (other.hasRawSize()) {
          setRawSize(other.getRawSize());
        }
        if (other.hasZlibData()) {
          setZlibData(other.getZlibData());
        }
        if (other.hasLzmaData()) {
          setLzmaData(other.getLzmaData());
        }
        if (other.hasOBSOLETEBzip2Data()) {
          setOBSOLETEBzip2Data(other.getOBSOLETEBzip2Data());
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
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
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
                  raw_ = input.readBytes();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
              case 16:
                {
                  rawSize_ = input.readInt32();
                  bitField0_ |= 0x00000002;
                  break;
                } // case 16
              case 26:
                {
                  zlibData_ = input.readBytes();
                  bitField0_ |= 0x00000004;
                  break;
                } // case 26
              case 34:
                {
                  lzmaData_ = input.readBytes();
                  bitField0_ |= 0x00000008;
                  break;
                } // case 34
              case 42:
                {
                  oBSOLETEBzip2Data_ = input.readBytes();
                  bitField0_ |= 0x00000010;
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
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private com.google.protobuf.ByteString raw_ = com.google.protobuf.ByteString.EMPTY;
      /**
       *
       *
       * <pre>
       * No compression
       * </pre>
       *
       * <code>optional bytes raw = 1;</code>
       *
       * @return Whether the raw field is set.
       */
      @Override
      public boolean hasRaw() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       *
       *
       * <pre>
       * No compression
       * </pre>
       *
       * <code>optional bytes raw = 1;</code>
       *
       * @return The raw.
       */
      @Override
      public com.google.protobuf.ByteString getRaw() {
        return raw_;
      }
      /**
       *
       *
       * <pre>
       * No compression
       * </pre>
       *
       * <code>optional bytes raw = 1;</code>
       *
       * @param value The raw to set.
       * @return This builder for chaining.
       */
      public Builder setRaw(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        raw_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       *
       *
       * <pre>
       * No compression
       * </pre>
       *
       * <code>optional bytes raw = 1;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearRaw() {
        bitField0_ = (bitField0_ & ~0x00000001);
        raw_ = getDefaultInstance().getRaw();
        onChanged();
        return this;
      }

      private int rawSize_;
      /**
       *
       *
       * <pre>
       * When compressed, the uncompressed size
       * </pre>
       *
       * <code>optional int32 raw_size = 2;</code>
       *
       * @return Whether the rawSize field is set.
       */
      @Override
      public boolean hasRawSize() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       *
       *
       * <pre>
       * When compressed, the uncompressed size
       * </pre>
       *
       * <code>optional int32 raw_size = 2;</code>
       *
       * @return The rawSize.
       */
      @Override
      public int getRawSize() {
        return rawSize_;
      }
      /**
       *
       *
       * <pre>
       * When compressed, the uncompressed size
       * </pre>
       *
       * <code>optional int32 raw_size = 2;</code>
       *
       * @param value The rawSize to set.
       * @return This builder for chaining.
       */
      public Builder setRawSize(int value) {

        rawSize_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      /**
       *
       *
       * <pre>
       * When compressed, the uncompressed size
       * </pre>
       *
       * <code>optional int32 raw_size = 2;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearRawSize() {
        bitField0_ = (bitField0_ & ~0x00000002);
        rawSize_ = 0;
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString zlibData_ = com.google.protobuf.ByteString.EMPTY;
      /**
       *
       *
       * <pre>
       * Possible compressed versions of the data.
       * </pre>
       *
       * <code>optional bytes zlib_data = 3;</code>
       *
       * @return Whether the zlibData field is set.
       */
      @Override
      public boolean hasZlibData() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       *
       *
       * <pre>
       * Possible compressed versions of the data.
       * </pre>
       *
       * <code>optional bytes zlib_data = 3;</code>
       *
       * @return The zlibData.
       */
      @Override
      public com.google.protobuf.ByteString getZlibData() {
        return zlibData_;
      }
      /**
       *
       *
       * <pre>
       * Possible compressed versions of the data.
       * </pre>
       *
       * <code>optional bytes zlib_data = 3;</code>
       *
       * @param value The zlibData to set.
       * @return This builder for chaining.
       */
      public Builder setZlibData(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        zlibData_ = value;
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }
      /**
       *
       *
       * <pre>
       * Possible compressed versions of the data.
       * </pre>
       *
       * <code>optional bytes zlib_data = 3;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearZlibData() {
        bitField0_ = (bitField0_ & ~0x00000004);
        zlibData_ = getDefaultInstance().getZlibData();
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString lzmaData_ = com.google.protobuf.ByteString.EMPTY;
      /**
       *
       *
       * <pre>
       * PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
       * </pre>
       *
       * <code>optional bytes lzma_data = 4;</code>
       *
       * @return Whether the lzmaData field is set.
       */
      @Override
      public boolean hasLzmaData() {
        return ((bitField0_ & 0x00000008) != 0);
      }
      /**
       *
       *
       * <pre>
       * PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
       * </pre>
       *
       * <code>optional bytes lzma_data = 4;</code>
       *
       * @return The lzmaData.
       */
      @Override
      public com.google.protobuf.ByteString getLzmaData() {
        return lzmaData_;
      }
      /**
       *
       *
       * <pre>
       * PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
       * </pre>
       *
       * <code>optional bytes lzma_data = 4;</code>
       *
       * @param value The lzmaData to set.
       * @return This builder for chaining.
       */
      public Builder setLzmaData(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        lzmaData_ = value;
        bitField0_ |= 0x00000008;
        onChanged();
        return this;
      }
      /**
       *
       *
       * <pre>
       * PROPOSED feature for LZMA compressed data. SUPPORT IS NOT REQUIRED.
       * </pre>
       *
       * <code>optional bytes lzma_data = 4;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearLzmaData() {
        bitField0_ = (bitField0_ & ~0x00000008);
        lzmaData_ = getDefaultInstance().getLzmaData();
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString oBSOLETEBzip2Data_ =
          com.google.protobuf.ByteString.EMPTY;
      /**
       *
       *
       * <pre>
       * Formerly used for bzip2 compressed data. Depreciated in 2010.
       * </pre>
       *
       * <code>optional bytes OBSOLETE_bzip2_data = 5 [deprecated = true];</code>
       *
       * @deprecated Blob.OBSOLETE_bzip2_data is deprecated. See
       *     main/java/org/apache/sedona/proto/fileformat.proto;l=32
       * @return Whether the oBSOLETEBzip2Data field is set.
       */
      @Override
      @Deprecated
      public boolean hasOBSOLETEBzip2Data() {
        return ((bitField0_ & 0x00000010) != 0);
      }
      /**
       *
       *
       * <pre>
       * Formerly used for bzip2 compressed data. Depreciated in 2010.
       * </pre>
       *
       * <code>optional bytes OBSOLETE_bzip2_data = 5 [deprecated = true];</code>
       *
       * @deprecated Blob.OBSOLETE_bzip2_data is deprecated. See
       *     main/java/org/apache/sedona/proto/fileformat.proto;l=32
       * @return The oBSOLETEBzip2Data.
       */
      @Override
      @Deprecated
      public com.google.protobuf.ByteString getOBSOLETEBzip2Data() {
        return oBSOLETEBzip2Data_;
      }
      /**
       *
       *
       * <pre>
       * Formerly used for bzip2 compressed data. Depreciated in 2010.
       * </pre>
       *
       * <code>optional bytes OBSOLETE_bzip2_data = 5 [deprecated = true];</code>
       *
       * @param value The oBSOLETEBzip2Data to set.
       * @return This builder for chaining.
       */
      @Deprecated
      public Builder setOBSOLETEBzip2Data(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        oBSOLETEBzip2Data_ = value;
        bitField0_ |= 0x00000010;
        onChanged();
        return this;
      }
      /**
       *
       *
       * <pre>
       * Formerly used for bzip2 compressed data. Depreciated in 2010.
       * </pre>
       *
       * <code>optional bytes OBSOLETE_bzip2_data = 5 [deprecated = true];</code>
       *
       * @return This builder for chaining.
       */
      @Deprecated
      public Builder clearOBSOLETEBzip2Data() {
        bitField0_ = (bitField0_ & ~0x00000010);
        oBSOLETEBzip2Data_ = getDefaultInstance().getOBSOLETEBzip2Data();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:Blob)
    }

    // @@protoc_insertion_point(class_scope:Blob)
    private static final Fileformat.Blob DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Fileformat.Blob();
    }

    public static Fileformat.Blob getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<Blob> PARSER =
        new com.google.protobuf.AbstractParser<Blob>() {
          @Override
          public Blob parsePartialFrom(
              com.google.protobuf.CodedInputStream input,
              com.google.protobuf.ExtensionRegistryLite extensionRegistry)
              throws com.google.protobuf.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (com.google.protobuf.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new com.google.protobuf.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static com.google.protobuf.Parser<Blob> parser() {
      return PARSER;
    }

    @Override
    public com.google.protobuf.Parser<Blob> getParserForType() {
      return PARSER;
    }

    @Override
    public Fileformat.Blob getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface BlobHeaderOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:BlobHeader)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required string type = 1;</code>
     *
     * @return Whether the type field is set.
     */
    boolean hasType();
    /**
     * <code>required string type = 1;</code>
     *
     * @return The type.
     */
    String getType();
    /**
     * <code>required string type = 1;</code>
     *
     * @return The bytes for type.
     */
    com.google.protobuf.ByteString getTypeBytes();

    /**
     * <code>optional bytes indexdata = 2;</code>
     *
     * @return Whether the indexdata field is set.
     */
    boolean hasIndexdata();
    /**
     * <code>optional bytes indexdata = 2;</code>
     *
     * @return The indexdata.
     */
    com.google.protobuf.ByteString getIndexdata();

    /**
     * <code>required int32 datasize = 3;</code>
     *
     * @return Whether the datasize field is set.
     */
    boolean hasDatasize();
    /**
     * <code>required int32 datasize = 3;</code>
     *
     * @return The datasize.
     */
    int getDatasize();
  }
  /** Protobuf type {@code BlobHeader} */
  public static final class BlobHeader extends com.google.protobuf.GeneratedMessage
      implements
      // @@protoc_insertion_point(message_implements:BlobHeader)
      BlobHeaderOrBuilder {
    private static final long serialVersionUID = 0L;

    static {
      com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
          com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC,
          /* major= */ 4,
          /* minor= */ 27,
          /* patch= */ 0,
          /* suffix= */ "",
          BlobHeader.class.getName());
    }
    // Use BlobHeader.newBuilder() to construct.
    private BlobHeader(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }

    private BlobHeader() {
      type_ = "";
      indexdata_ = com.google.protobuf.ByteString.EMPTY;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return Fileformat.internal_static_org_apache_sedona_osm_build_BlobHeader_descriptor;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return Fileformat.internal_static_org_apache_sedona_osm_build_BlobHeader_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Fileformat.BlobHeader.class, Fileformat.BlobHeader.Builder.class);
    }

    private int bitField0_;
    public static final int TYPE_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private volatile Object type_ = "";
    /**
     * <code>required string type = 1;</code>
     *
     * @return Whether the type field is set.
     */
    @Override
    public boolean hasType() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required string type = 1;</code>
     *
     * @return The type.
     */
    @Override
    public String getType() {
      Object ref = type_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          type_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string type = 1;</code>
     *
     * @return The bytes for type.
     */
    @Override
    public com.google.protobuf.ByteString getTypeBytes() {
      Object ref = type_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b =
            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
        type_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int INDEXDATA_FIELD_NUMBER = 2;
    private com.google.protobuf.ByteString indexdata_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>optional bytes indexdata = 2;</code>
     *
     * @return Whether the indexdata field is set.
     */
    @Override
    public boolean hasIndexdata() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional bytes indexdata = 2;</code>
     *
     * @return The indexdata.
     */
    @Override
    public com.google.protobuf.ByteString getIndexdata() {
      return indexdata_;
    }

    public static final int DATASIZE_FIELD_NUMBER = 3;
    private int datasize_ = 0;
    /**
     * <code>required int32 datasize = 3;</code>
     *
     * @return Whether the datasize field is set.
     */
    @Override
    public boolean hasDatasize() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>required int32 datasize = 3;</code>
     *
     * @return The datasize.
     */
    @Override
    public int getDatasize() {
      return datasize_;
    }

    private byte memoizedIsInitialized = -1;

    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasDatasize()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(com.google.protobuf.CodedOutputStream output) throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        com.google.protobuf.GeneratedMessage.writeString(output, 1, type_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeBytes(2, indexdata_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeInt32(3, datasize_);
      }
      getUnknownFields().writeTo(output);
    }

    @Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.GeneratedMessage.computeStringSize(1, type_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream.computeBytesSize(2, indexdata_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream.computeInt32Size(3, datasize_);
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
      if (!(obj instanceof Fileformat.BlobHeader)) {
        return super.equals(obj);
      }
      Fileformat.BlobHeader other = (Fileformat.BlobHeader) obj;

      if (hasType() != other.hasType()) return false;
      if (hasType()) {
        if (!getType().equals(other.getType())) return false;
      }
      if (hasIndexdata() != other.hasIndexdata()) return false;
      if (hasIndexdata()) {
        if (!getIndexdata().equals(other.getIndexdata())) return false;
      }
      if (hasDatasize() != other.hasDatasize()) return false;
      if (hasDatasize()) {
        if (getDatasize() != other.getDatasize()) return false;
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
      if (hasType()) {
        hash = (37 * hash) + TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getType().hashCode();
      }
      if (hasIndexdata()) {
        hash = (37 * hash) + INDEXDATA_FIELD_NUMBER;
        hash = (53 * hash) + getIndexdata().hashCode();
      }
      if (hasDatasize()) {
        hash = (37 * hash) + DATASIZE_FIELD_NUMBER;
        hash = (53 * hash) + getDatasize();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static Fileformat.BlobHeader parseFrom(java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Fileformat.BlobHeader parseFrom(
        java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Fileformat.BlobHeader parseFrom(com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Fileformat.BlobHeader parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Fileformat.BlobHeader parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static Fileformat.BlobHeader parseFrom(
        byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static Fileformat.BlobHeader parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Fileformat.BlobHeader parseFrom(
        java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Fileformat.BlobHeader parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseDelimitedWithIOException(PARSER, input);
    }

    public static Fileformat.BlobHeader parseDelimitedFrom(
        java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static Fileformat.BlobHeader parseFrom(com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseWithIOException(PARSER, input);
    }

    public static Fileformat.BlobHeader parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    @Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(Fileformat.BlobHeader prototype) {
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
    /** Protobuf type {@code BlobHeader} */
    public static final class Builder extends com.google.protobuf.GeneratedMessage.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:BlobHeader)
        Fileformat.BlobHeaderOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return Fileformat.internal_static_org_apache_sedona_osm_build_BlobHeader_descriptor;
      }

      @Override
      protected FieldAccessorTable internalGetFieldAccessorTable() {
        return Fileformat.internal_static_org_apache_sedona_osm_build_BlobHeader_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Fileformat.BlobHeader.class, Fileformat.BlobHeader.Builder.class);
      }

      // Construct using Fileformat.BlobHeader.newBuilder()
      private Builder() {}

      private Builder(BuilderParent parent) {
        super(parent);
      }

      @Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        type_ = "";
        indexdata_ = com.google.protobuf.ByteString.EMPTY;
        datasize_ = 0;
        return this;
      }

      @Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return Fileformat.internal_static_org_apache_sedona_osm_build_BlobHeader_descriptor;
      }

      @Override
      public Fileformat.BlobHeader getDefaultInstanceForType() {
        return Fileformat.BlobHeader.getDefaultInstance();
      }

      @Override
      public Fileformat.BlobHeader build() {
        Fileformat.BlobHeader result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
      public Fileformat.BlobHeader buildPartial() {
        Fileformat.BlobHeader result = new Fileformat.BlobHeader(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(Fileformat.BlobHeader result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.type_ = type_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.indexdata_ = indexdata_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.datasize_ = datasize_;
          to_bitField0_ |= 0x00000004;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof Fileformat.BlobHeader) {
          return mergeFrom((Fileformat.BlobHeader) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Fileformat.BlobHeader other) {
        if (other == Fileformat.BlobHeader.getDefaultInstance()) return this;
        if (other.hasType()) {
          type_ = other.type_;
          bitField0_ |= 0x00000001;
          onChanged();
        }
        if (other.hasIndexdata()) {
          setIndexdata(other.getIndexdata());
        }
        if (other.hasDatasize()) {
          setDatasize(other.getDatasize());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @Override
      public final boolean isInitialized() {
        if (!hasType()) {
          return false;
        }
        if (!hasDatasize()) {
          return false;
        }
        return true;
      }

      @Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
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
                  type_ = input.readBytes();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
              case 18:
                {
                  indexdata_ = input.readBytes();
                  bitField0_ |= 0x00000002;
                  break;
                } // case 18
              case 24:
                {
                  datasize_ = input.readInt32();
                  bitField0_ |= 0x00000004;
                  break;
                } // case 24
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private Object type_ = "";
      /**
       * <code>required string type = 1;</code>
       *
       * @return Whether the type field is set.
       */
      public boolean hasType() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required string type = 1;</code>
       *
       * @return The type.
       */
      public String getType() {
        Object ref = type_;
        if (!(ref instanceof String)) {
          com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
          String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            type_ = s;
          }
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>required string type = 1;</code>
       *
       * @return The bytes for type.
       */
      public com.google.protobuf.ByteString getTypeBytes() {
        Object ref = type_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b =
              com.google.protobuf.ByteString.copyFromUtf8((String) ref);
          type_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string type = 1;</code>
       *
       * @param value The type to set.
       * @return This builder for chaining.
       */
      public Builder setType(String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        type_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       * <code>required string type = 1;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearType() {
        type_ = getDefaultInstance().getType();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }
      /**
       * <code>required string type = 1;</code>
       *
       * @param value The bytes for type to set.
       * @return This builder for chaining.
       */
      public Builder setTypeBytes(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        type_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString indexdata_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes indexdata = 2;</code>
       *
       * @return Whether the indexdata field is set.
       */
      @Override
      public boolean hasIndexdata() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional bytes indexdata = 2;</code>
       *
       * @return The indexdata.
       */
      @Override
      public com.google.protobuf.ByteString getIndexdata() {
        return indexdata_;
      }
      /**
       * <code>optional bytes indexdata = 2;</code>
       *
       * @param value The indexdata to set.
       * @return This builder for chaining.
       */
      public Builder setIndexdata(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        indexdata_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes indexdata = 2;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearIndexdata() {
        bitField0_ = (bitField0_ & ~0x00000002);
        indexdata_ = getDefaultInstance().getIndexdata();
        onChanged();
        return this;
      }

      private int datasize_;
      /**
       * <code>required int32 datasize = 3;</code>
       *
       * @return Whether the datasize field is set.
       */
      @Override
      public boolean hasDatasize() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <code>required int32 datasize = 3;</code>
       *
       * @return The datasize.
       */
      @Override
      public int getDatasize() {
        return datasize_;
      }
      /**
       * <code>required int32 datasize = 3;</code>
       *
       * @param value The datasize to set.
       * @return This builder for chaining.
       */
      public Builder setDatasize(int value) {

        datasize_ = value;
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }
      /**
       * <code>required int32 datasize = 3;</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearDatasize() {
        bitField0_ = (bitField0_ & ~0x00000004);
        datasize_ = 0;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:BlobHeader)
    }

    // @@protoc_insertion_point(class_scope:BlobHeader)
    private static final Fileformat.BlobHeader DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new Fileformat.BlobHeader();
    }

    public static Fileformat.BlobHeader getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<BlobHeader> PARSER =
        new com.google.protobuf.AbstractParser<BlobHeader>() {
          @Override
          public BlobHeader parsePartialFrom(
              com.google.protobuf.CodedInputStream input,
              com.google.protobuf.ExtensionRegistryLite extensionRegistry)
              throws com.google.protobuf.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (com.google.protobuf.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new com.google.protobuf.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static com.google.protobuf.Parser<BlobHeader> parser() {
      return PARSER;
    }

    @Override
    public com.google.protobuf.Parser<BlobHeader> getParserForType() {
      return PARSER;
    }

    @Override
    public Fileformat.BlobHeader getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_Blob_descriptor;
  private static final com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_Blob_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_org_apache_sedona_osm_build_BlobHeader_descriptor;
  private static final com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_org_apache_sedona_osm_build_BlobHeader_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
    return descriptor;
  }

  private static com.google.protobuf.Descriptors.FileDescriptor descriptor;

  static {
    String[] descriptorData = {
      "\n2main/java/org/apache/sedona/proto/file"
          + "format.proto\022\033org.apache.sedona.osm.buil"
          + "d\"l\n\004Blob\022\013\n\003raw\030\001 \001(\014\022\020\n\010raw_size\030\002 \001(\005"
          + "\022\021\n\tzlib_data\030\003 \001(\014\022\021\n\tlzma_data\030\004 \001(\014\022\037"
          + "\n\023OBSOLETE_bzip2_data\030\005 \001(\014B\002\030\001\"?\n\nBlobH"
          + "eader\022\014\n\004type\030\001 \002(\t\022\021\n\tindexdata\030\002 \001(\014\022\020"
          + "\n\010datasize\030\003 \002(\005B\002H\003"
    };
    descriptor =
        com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
            descriptorData, new com.google.protobuf.Descriptors.FileDescriptor[] {});
    internal_static_org_apache_sedona_osm_build_Blob_descriptor =
        getDescriptor().getMessageTypes().get(0);
    internal_static_org_apache_sedona_osm_build_Blob_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_Blob_descriptor,
            new String[] {
              "Raw", "RawSize", "ZlibData", "LzmaData", "OBSOLETEBzip2Data",
            });
    internal_static_org_apache_sedona_osm_build_BlobHeader_descriptor =
        getDescriptor().getMessageTypes().get(1);
    internal_static_org_apache_sedona_osm_build_BlobHeader_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessage.FieldAccessorTable(
            internal_static_org_apache_sedona_osm_build_BlobHeader_descriptor,
            new String[] {
              "Type", "Indexdata", "Datasize",
            });
    descriptor.resolveAllFeaturesImmutable();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
