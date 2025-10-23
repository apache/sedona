#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# check_makefiles_for_tabs.sh

# Iterate over all files passed as arguments by pre-commit
for makefile in "$@"; do
  # Check if the file exists and is a regular file
  if [[ -f "$makefile" ]]; then
    if grep -P '^\s' "$makefile" | grep -vP '^\t' > /dev/null; then
      echo "Error: File '$makefile' contains spaces at the beginning of lines instead of tabs."
      exit 1
    fi
  fi
done
exit 0
