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

package org.apache.sedona.core.io.avro;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.utils.AvroUtils;
import org.apache.sedona.core.utils.SedonaUtils;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SchemaUtils {
    public static class SchemaParser {
        private static Schema.Parser parser = null;
        private static Map<String, String> dataTypes = null;
        
        private static Schema.Parser getParser() {
            if (SedonaUtils.isNull(parser)) {
                synchronized (SchemaParser.class) {
                    if (SedonaUtils.isNull(parser)) {
                        parser = new Schema.Parser();
                        dataTypes = new HashMap<>();
                    }
                }
            }
            return parser;
        }
        
        public static String parseJson(String namespace, String name, JSONObject json) throws SedonaException {
            Schema.Parser parser = getParser();
            String dataType = AvroUtils.getNestedNamespace(namespace, name);
            if (dataTypes.containsKey(dataType) && !dataTypes.get(dataType).equals(json.toJSONString())) {
                throw new SedonaException(dataType+ " already Defined with different Schema");
            }else if(dataTypes.containsKey(dataType)){
                return dataType;
            }
            parser.parse(json.toJSONString());
            dataTypes.put(dataType,json.toJSONString());
            return dataType;
        }
        
        public static Schema getSchema(String namespace, String name) throws SedonaException {
            return getSchema(AvroUtils.getNestedNamespace(namespace,name));
        }
        
        public static Schema getSchema(String dataType) throws SedonaException {
            if(!dataTypes.containsKey(dataType)){
                throw new SedonaException(dataType+ "not defined");
            }
            return getParser().getTypes().get(dataType);
        }
        
        
    }
}
