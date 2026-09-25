#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM alpine:3.22.1

# Keep the same upstream MinIO release without depending on its withdrawn registry image.
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64) arch=amd64; sha256=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f ;; \
      aarch64) arch=arm64; sha256=5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d ;; \
      *) echo "Unsupported MinIO test image architecture: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    wget -O /usr/bin/minio \
      "https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-${arch}.RELEASE.2025-09-07T16-13-09Z"; \
    echo "${sha256}  /usr/bin/minio" | sha256sum -c -; \
    chmod +x /usr/bin/minio

EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
