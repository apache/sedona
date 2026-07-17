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
package org.apache.spark.sql.sedona_sql.io.netcdfmetadata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import java.util.concurrent.CompletableFuture
import scala.util.control.NonFatal

/**
 * A ucar `RandomAccessFile` backed by a Hadoop `FSDataInputStream`. cdm-core performs all file
 * I/O through this interface; each buffer fill becomes a positioned read on the underlying
 * stream, which object stores such as S3A serve as HTTP range requests. Only the byte ranges
 * cdm-core actually asks for are transferred — the file is never downloaded in full.
 *
 * The known file length and modification time are supplied by Spark's file listing, so opening a
 * file issues no extra filesystem metadata calls.
 */
class HadoopRandomAccessFile(
    path: Path,
    configuration: Configuration,
    fileSize: Long,
    fileModificationTime: Long,
    bufferSize: Int = HadoopRandomAccessFile.DEFAULT_BUFFER_SIZE)
    extends ucar.unidata.io.RandomAccessFile(bufferSize) {

  private val in: FSDataInputStream = {
    val fs = path.getFileSystem(configuration)
    openWithRandomReadPolicy(fs).getOrElse(fs.open(path))
  }

  /**
   * Open via `FileSystem.openFile` with a random-access read policy hint, so object store
   * connectors (S3A, ABFS) serve each read as a small range request instead of draining a
   * sequential stream on every backward seek. Passing the known file length skips an extra HEAD
   * request. Unknown options are ignored by `opt`, so the hints are safe on filesystems that do
   * not recognize them.
   *
   * `openFile` exists since Hadoop 3.3.0 but Sedona compiles against an older Hadoop API, so it
   * is invoked reflectively; on older runtimes this returns None and the caller falls back to
   * plain `FileSystem.open`, which still performs positioned reads.
   */
  private def openWithRandomReadPolicy(fs: FileSystem): Option[FSDataInputStream] = {
    try {
      val builderClass = Class.forName("org.apache.hadoop.fs.FutureDataInputStreamBuilder")
      val optMethod = builderClass.getMethod("opt", classOf[String], classOf[String])
      val buildMethod = builderClass.getMethod("build")
      var builder = classOf[FileSystem].getMethod("openFile", classOf[Path]).invoke(fs, path)
      builder = optMethod.invoke(builder, "fs.option.openfile.read.policy", "random")
      builder = optMethod.invoke(builder, "fs.s3a.experimental.input.fadvise", "random")
      builder = optMethod.invoke(builder, "fs.option.openfile.length", fileSize.toString)
      val future =
        buildMethod.invoke(builder).asInstanceOf[CompletableFuture[FSDataInputStream]]
      Some(future.get())
    } catch {
      case NonFatal(_) => None
    }
  }

  this.location = path.toString

  override def length(): Long = fileSize

  override def getLastModified: Long = fileModificationTime

  override protected def read_(pos: Long, b: Array[Byte], offset: Int, len: Int): Int = {
    // Positioned read; loop because a single read may return fewer bytes than requested.
    var total = 0
    while (total < len) {
      val n = in.read(pos + total, b, offset + total, len - total)
      if (n < 0) {
        return if (total == 0) -1 else total
      }
      total += n
    }
    total
  }

  override def readToByteChannel(dest: WritableByteChannel, offset: Long, nbytes: Long): Long = {
    val chunkSize = Math.min(nbytes, 64L * 1024).toInt
    if (chunkSize <= 0) return 0L
    val chunk = new Array[Byte](chunkSize)
    var done = 0L
    while (done < nbytes) {
      val toRead = Math.min(chunk.length.toLong, nbytes - done).toInt
      val n = read_(offset + done, chunk, 0, toRead)
      if (n <= 0) return done
      dest.write(ByteBuffer.wrap(chunk, 0, n))
      done += n
    }
    done
  }

  override def flush(): Unit = {}

  override def close(): Unit = {
    super.close()
    in.close()
  }
}

object HadoopRandomAccessFile {

  /**
   * Larger than ucar's 8 KiB default: header parsing reads mostly sequentially, and a bigger
   * buffer keeps the number of remote range requests low without transferring much extra data.
   */
  val DEFAULT_BUFFER_SIZE: Int = 32 * 1024
}
