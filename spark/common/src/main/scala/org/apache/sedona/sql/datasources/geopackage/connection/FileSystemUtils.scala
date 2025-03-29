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
package org.apache.sedona.sql.datasources.geopackage.connection

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File

object FileSystemUtils {

  def copyToLocal(options: Configuration, file: Path): (File, Boolean) = {
    if (isLocalFileSystem(options, file)) {
      return (new File(file.toUri.getPath), false)
    }

    val fs = file.getFileSystem(options)
    val tempFile = File.createTempFile(java.util.UUID.randomUUID.toString, ".gpkg")

    fs.copyToLocalFile(file, new Path(tempFile.getAbsolutePath))

    (tempFile, true)
  }

  private def isLocalFileSystem(conf: Configuration, path: Path): Boolean = {
    FileSystem.get(path.toUri, conf).isInstanceOf[org.apache.hadoop.fs.LocalFileSystem]
  }

}
