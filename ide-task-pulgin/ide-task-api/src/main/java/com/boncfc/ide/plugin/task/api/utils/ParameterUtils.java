/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.boncfc.ide.plugin.task.api.utils;

import com.boncfc.ide.plugin.task.api.model.ParamDataType;
import com.boncfc.ide.plugin.task.api.parse.PlaceholderUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.boncfc.ide.plugin.task.api.TaskConstants.*;

/**
 * parameter parse utils
 */
public class ParameterUtils {

    private static final Pattern DATE_PARSE_PATTERN = Pattern.compile("\\$\\[([^\\$\\]]+)]");

    private static final Pattern DATE_START_PATTERN = Pattern.compile("^[0-9]");

    private static final char PARAM_REPLACE_CHAR = '?';

    private ParameterUtils() {
        throw new UnsupportedOperationException("Construct ParameterUtils");
    }


    /**
     * set in parameter
     *
     * @param index    index
     * @param stmt     preparedstatement
     * @param dataType data type
     * @param value    value
     * @throws Exception errors
     */
    public static void setInParameter(int index, PreparedStatement stmt, ParamDataType dataType,
                                      String value) throws Exception {
        if(dataType.equals(ParamDataType.STRING)) {
            stmt.setString(index, value);
        } else if(dataType.equals(ParamDataType.INT)) {
            stmt.setInt(index, Integer.parseInt(value));
        } else if (dataType.equals(ParamDataType.FLOAT)) {
            stmt.setFloat(index, Float.parseFloat(value));
        } else if (dataType.equals(ParamDataType.BOOLEAN)) {
            stmt.setBoolean(index, Boolean.parseBoolean(value));
        } else {
            stmt.setString(index, value);
        }
    }

}
