package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.TablePrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.*;


public class ParamChecker {

    private static void throwNotExistException(String key) {
        throw new IllegalArgumentException(String.format(OTSErrorMessage.MISSING_PARAMTER_ERROR, key));
    }

    private static void throwStringLengthZeroException(String key) {
        throw new IllegalArgumentException(String.format(OTSErrorMessage.PARAMTER_STRING_IS_EMPTY_ERROR, key));
    }

    private static void throwEmptyListException(String key) {
        throw new IllegalArgumentException(String.format(OTSErrorMessage.PARAMETER_LIST_IS_EMPTY_ERROR, key));
    }

    private static void throwNotListException(String key, Throwable t) {
        throw new IllegalArgumentException(String.format(OTSErrorMessage.PARAMETER_IS_NOT_ARRAY_ERROR, key), t);
    }

    public static String checkStringAndGet(Configuration param, String key) {
        String value = param.getString(key);
        value = value != null ? value.trim() : null;
        if (null == value) {
            throwNotExistException(key);
        } else if (value.length() == 0) {
            throwStringLengthZeroException(key);
        }
        return value;
    }

    public static List<Object> checkListAndGet(Configuration param, String key, boolean isCheckEmpty) {
        List<Object> value = null;
        try {
            value = param.getList(key);
        } catch (ClassCastException e) {
            throwNotListException(key, e);
        }
        if (null == value) {
            throwNotExistException(key);
        } else if (isCheckEmpty && value.isEmpty()) {
            throwEmptyListException(key);
        }
        return value;
    }
    
    public static void checkPrimaryKey(TableMeta meta, List<TablePrimaryKeySchema> pk) {
        Map<String, PrimaryKeyType> pkNameAndTypeMapping = meta.getPrimaryKeyMap();
        // 个数是否相等
        if (pkNameAndTypeMapping.size() != pk.size()) {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.INPUT_PK_COUNT_NOT_EQUAL_META_ERROR, pk.size(), pkNameAndTypeMapping.size()));
        }
        
        // 名字类型是否相等
        for (TablePrimaryKeySchema col : pk) {
            PrimaryKeyType type = pkNameAndTypeMapping.get(col.getName());
            if (type == null) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_COLUMN_MISSING_ERROR, col.getName()));
            }
            if (type != col.getType()) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.INPUT_PK_TYPE_NOT_MATCH_META_ERROR, col.getName(), type, col.getType()));
            }
        }
    }
    
    public static void checkAttribute(List<OTSAttrColumn> attr) {
        // 检查重复列
        Set<String> names = new HashSet<String>();
        for (OTSAttrColumn col : attr) {
            if (names.contains(col.getName())) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.ATTR_REPEAT_COLUMN_ERROR, col.getName()));
            } else {
                names.add(col.getName());
            }
        }
    }
}
