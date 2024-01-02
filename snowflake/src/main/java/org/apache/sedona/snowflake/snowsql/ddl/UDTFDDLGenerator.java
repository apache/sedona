/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.snowflake.snowsql.ddl;

import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.apache.sedona.snowflake.snowsql.udtfs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.stream.Collectors;

public class UDTFDDLGenerator {
    public static final Class[] udtfClz = {
            ST_MinimumBoundingRadius.class,
            ST_Intersection_Aggr.class,
            ST_SubDivideExplode.class,
            ST_Envelope_Aggr.class,
            ST_Union_Aggr.class,
            ST_Collect.class,
            ST_Dump.class
    };

    public static String formatUDTFDDL(
            String functionName,
            String schemaName,
            Parameter[] argTypes,
            String[] argNames,
            String returnType,
            String stageName,
            String handlerName,
            String sedona_version,
            String geotools_version,
            String null_input_conf,
            String immutable_conf,
            boolean isNativeApp,
            String appRoleName
    ) {
        String ddlTemplate = new BufferedReader(
                new InputStreamReader(
                        Objects.requireNonNull(DDLGenerator.class.getClassLoader().getResourceAsStream("UDTFTemplate.txt"))
                )
        ).lines().collect(Collectors.joining("\n"));
        String ddl = ddlTemplate.replace(
                "{KW_FUNCTION_NAME}", functionName
        ).replace(
                "{KW_SCHEMA_NAME}", schemaName
        ).replace(
                "{KW_ARG_SPEC}", ArgSpecBuilder.args(argTypes, argNames)
        ).replace(
                "{KW_RETURN_TYPE}", returnType
        ).replace(
                "{KW_STAGE_NAME}", stageName
        ).replace(
                "{KW_HANDLER_NAME}", handlerName
        ).replace(
                "{KW_SEDONA_VERSION}", sedona_version
        ).replace(
                "{KW_GEOTOOLS_VERSION}", geotools_version
        ).replace(
                "{KW_NULL_INPUT_CONF}", null_input_conf
        ).replace(
                "{KW_IMMUTABLE_CONF}", immutable_conf
        );
        if (isNativeApp) {
            ddl += "\n";
            ddl += "GRANT USAGE ON FUNCTION " + schemaName + "." + functionName + "(" + ArgSpecBuilder.argTypes(argTypes) + ") TO APPLICATION ROLE " + appRoleName + ";";
        }
        return ddl;
    }

    public static String buildUDTFDDL(Class c, Map<String, String> configs, String stageName, boolean isNativeApp, String appRoleName) {
        UDTFAnnotations.TabularFunc funcProps = (UDTFAnnotations.TabularFunc) c.getAnnotation(UDTFAnnotations.TabularFunc.class);
        // get return types
        Class outputRowClass = Arrays.stream(c.getDeclaredClasses()).filter(
                cls -> cls.getName().endsWith("OutputRow")
        ).findFirst().get();
        String returnTypes = Arrays.stream(outputRowClass.getFields()).map(
                field -> field.getName() + " " + Constants.snowflakeTypeMap.get(field.getType().getTypeName())
        ).collect(Collectors.joining(", "));
        Method processMethod = Arrays.stream(c.getDeclaredMethods()).filter(m -> m.getName().equals("process")).findFirst().get();
        Parameter[] paramTypes = processMethod.getParameters();
        String[] argNames = funcProps.argNames();
        String handlerName = c.getPackage().getName() + "." + c.getSimpleName();
        String null_input_conf = c.isAnnotationPresent(UDTFAnnotations.CallOnNull.class) ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT";
        String immutable_conf = c.isAnnotationPresent(UDTFAnnotations.Volatile.class) ? "VOLATILE" : "IMMUTABLE";
        return formatUDTFDDL(
                funcProps.name(),
                configs.getOrDefault("schema", "sedona"),
                paramTypes,
                argNames,
                returnTypes,
                stageName,
                handlerName,
                configs.get(Constants.SEDONA_VERSION),
                configs.get(Constants.GEOTOOLS_VERSION),
                null_input_conf,
                immutable_conf,
                isNativeApp,
                appRoleName
        );
    }

    public static List<String> buildAll(Map<String, String> configs, String stageName, boolean isNativeApp, String appRoleName) {
        List<String> ddlList = new ArrayList<>();
        for (Class c : udtfClz) {
            ddlList.add(buildUDTFDDL(c, configs, stageName, isNativeApp, appRoleName));
        }
        return ddlList;
    }


}
