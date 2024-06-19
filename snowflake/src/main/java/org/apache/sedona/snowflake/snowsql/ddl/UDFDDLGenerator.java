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
package org.apache.sedona.snowflake.snowsql.ddl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.sedona.snowflake.snowsql.UDFs;
import org.apache.sedona.snowflake.snowsql.UDFsV2;
import org.apache.sedona.snowflake.snowsql.annotations.UDFAnnotations;

public class UDFDDLGenerator {

  public static Method[] udfMethods() {
    return UDFs.class.getDeclaredMethods();
  }

  public static Method[] udfV2Methods() {
    return UDFsV2.class.getDeclaredMethods();
  }

  public static String buildUDFDDL(
      Method method,
      Map<String, String> configs,
      String stageName,
      boolean isNativeApp,
      String appRoleName) {
    if (!method.isAnnotationPresent(UDFAnnotations.ParamMeta.class)) {
      throw new RuntimeException("Missing ParamMeta annotation for method: " + method.getName());
    }
    String[] argNames = method.getAnnotation(UDFAnnotations.ParamMeta.class).argNames();
    Parameter[] argTypesRaw = method.getParameters();
    String argTypesCustom[] = method.getAnnotation(UDFAnnotations.ParamMeta.class).argTypes();
    // generate return type
    String returnType =
        Constants.snowflakeTypeMap.get(
            method.getAnnotation(UDFAnnotations.ParamMeta.class).returnTypes().isEmpty()
                ? method.getReturnType().getTypeName()
                : method.getAnnotation(UDFAnnotations.ParamMeta.class).returnTypes());
    if (returnType == null) {
      throw new RuntimeException("Unsupported type: " + method.getReturnType().getTypeName());
    }
    String handlerName = method.getDeclaringClass().getName() + "." + method.getName();
    // check some function attributes
    String null_input_conf =
        method.isAnnotationPresent(UDFAnnotations.CallOnNull.class)
            ? "CALLED ON NULL INPUT"
            : "RETURNS NULL ON NULL INPUT";
    String immutable_conf =
        method.isAnnotationPresent(UDFAnnotations.Volatile.class) ? "VOLATILE" : "IMMUTABLE";
    return formatUDFDDL(
        method.getName(),
        configs.getOrDefault("schema", "sedona"),
        argTypesRaw,
        argNames,
        argTypesCustom,
        returnType,
        stageName,
        handlerName,
        configs.get(Constants.SEDONA_VERSION),
        configs.get(Constants.GEOTOOLS_VERSION),
        null_input_conf,
        immutable_conf,
        isNativeApp,
        appRoleName);
  }

  public static List<String> buildAll(
      Map<String, String> configs, String stageName, boolean isNativeApp, String appRoleName) {
    List<String> ddlList = new ArrayList<>();
    for (Method method : udfMethods()) {
      if (method.getModifiers() == (Modifier.PUBLIC | Modifier.STATIC)) {
        ddlList.add(buildUDFDDL(method, configs, stageName, isNativeApp, appRoleName));
      }
    }
    for (Method method : udfV2Methods()) {
      if (method.getModifiers() == (Modifier.PUBLIC | Modifier.STATIC)) {
        ddlList.add(buildUDFDDL(method, configs, stageName, isNativeApp, appRoleName));
      }
    }
    // Replace Geometry with GEOGRAPHY and generate DDL for UDFsV2 again
    Constants.snowflakeTypeMap.replace("Geometry", "GEOGRAPHY");
    for (Method method : udfV2Methods()) {
      if (method.getModifiers() == (Modifier.PUBLIC | Modifier.STATIC)) {
        ddlList.add(buildUDFDDL(method, configs, stageName, isNativeApp, appRoleName));
      }
    }
    return ddlList;
  }

  public static String formatUDFDDL(
      String functionName,
      String schemaName,
      Parameter[] argTypesRaw,
      String[] argNames,
      String[] argTypesCustom,
      String returnType,
      String stageName,
      String handlerName,
      String sedona_version,
      String geotools_version,
      String null_input_conf,
      String immutable_conf,
      boolean isNativeApp,
      String appRoleName) {
    String ddlTemplate =
        String.join(
            "\n",
            new BufferedReader(
                    new InputStreamReader(
                        Objects.requireNonNull(
                            DDLGenerator.class
                                .getClassLoader()
                                .getResourceAsStream("UDFTemplate.txt"))))
                .lines()
                .collect(Collectors.toList()));
    String ddl =
        ddlTemplate
            .replace("{KW_FUNCTION_NAME}", functionName)
            .replace("{KW_SCHEMA_NAME}", schemaName)
            .replace("{KW_ARG_SPEC}", ArgSpecBuilder.args(argTypesRaw, argNames, argTypesCustom))
            .replace("{KW_RETURN_TYPE}", returnType)
            .replace("{KW_STAGE_NAME}", stageName)
            .replace("{KW_HANDLER_NAME}", handlerName)
            .replace("{KW_SEDONA_VERSION}", sedona_version)
            .replace("{KW_GEOTOOLS_VERSION}", geotools_version)
            .replace("{KW_NULL_INPUT_CONF}", null_input_conf)
            .replace("{KW_IMMUTABLE_CONF}", immutable_conf);
    if (isNativeApp) {
      ddl += "\n";
      ddl +=
          "GRANT USAGE ON FUNCTION "
              + schemaName
              + "."
              + functionName
              + "("
              + ArgSpecBuilder.argTypes(argTypesRaw, argTypesCustom)
              + ") TO APPLICATION ROLE "
              + appRoleName
              + ";";
    }
    return ddl;
  }
}
