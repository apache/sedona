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

import java.lang.reflect.Parameter;

public class ArgSpecBuilder
{
    /**
     * Build argument spec for a function. Format: argName1 argType1, argName2 argType2, ...
     * @param paramTypes
     * @param argNames
     * @return
     */
    public static String args(Parameter[] paramTypes, String[] argNames){
        StringBuilder argTypesBuilder = new StringBuilder();
        for (int it = 0; it < paramTypes.length; it++) {
            argTypesBuilder.append(String.format(
                    "%s %s",
                    argNames[it],
                    Constants.snowflakeTypeMap.get(paramTypes[it].getType().getTypeName())
            ));
            if (it + 1 != paramTypes.length) {
                argTypesBuilder.append(", ");
            }
        }
        String argSpec = argTypesBuilder.toString();
        return argSpec;
    }

    /**
     * Build argument spec for a function. Format: argType1, argType2, ...
     * @param paramTypes
     * @return
     */
    public static String argTypes(Parameter[] paramTypes){
        StringBuilder argTypesBuilder = new StringBuilder();
        for (int it = 0; it < paramTypes.length; it++) {
            argTypesBuilder.append(String.format(
                    "%s",
                    Constants.snowflakeTypeMap.get(paramTypes[it].getType().getTypeName())
            ));
            if (it + 1 != paramTypes.length) {
                argTypesBuilder.append(", ");
            }
        }
        String argSpec = argTypesBuilder.toString();
        return argSpec;
    }
}
