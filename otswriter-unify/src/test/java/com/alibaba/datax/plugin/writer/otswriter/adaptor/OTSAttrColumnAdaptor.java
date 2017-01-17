package com.alibaba.datax.plugin.writer.otswriter.adaptor;

import java.lang.reflect.Type;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OTSAttrColumnAdaptor implements JsonDeserializer<OTSAttrColumn>, JsonSerializer<OTSAttrColumn> {
    private final static String SRC_NAME = "srcName";
    private final static String NAME = "name";
    private final static String TYPE = "type";

    @Override
    public JsonElement serialize(OTSAttrColumn src, Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject json = new JsonObject();
        json.add(SRC_NAME, new JsonPrimitive(src.getSrcName()));
        json.add(NAME, new JsonPrimitive(src.getName()));
        
        switch (src.getType()) {
        case STRING:
            json.add(TYPE, new JsonPrimitive(OTSConst.TYPE_STRING));
            break;
        case INTEGER:
            json.add(TYPE, new JsonPrimitive(OTSConst.TYPE_INTEGER));
            break;
        case DOUBLE:
            json.add(TYPE, new JsonPrimitive(OTSConst.TYPE_DOUBLE));
            break;
        case BOOLEAN:
            json.add(TYPE, new JsonPrimitive(OTSConst.TYPE_BOOLEAN));
            break;
        case BINARY:
            json.add(TYPE, new JsonPrimitive(OTSConst.TYPE_BINARY));
            break;
        default:
            throw new IllegalArgumentException(String.format(OTSErrorMessage.ATTR_TYPE_ERROR,  src.getType()));
        }
        return json;
    }

    @Override
    public OTSAttrColumn deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        JsonObject obj = json.getAsJsonObject();
        String srcName = obj.getAsJsonPrimitive(SRC_NAME).getAsString();
        String name = obj.getAsJsonPrimitive(NAME).getAsString();
        String type = obj.getAsJsonPrimitive(TYPE).getAsString();
        
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return new OTSAttrColumn(srcName, name, ColumnType.STRING);
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return new OTSAttrColumn(srcName, name, ColumnType.INTEGER);
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_DOUBLE)) {
            return new OTSAttrColumn(srcName, name, ColumnType.DOUBLE);
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BOOLEAN)) {
            return new OTSAttrColumn(srcName, name, ColumnType.BOOLEAN);
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_BINARY)) {
            return new OTSAttrColumn(srcName, name, ColumnType.BINARY);
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.ATTR_TYPE_ERROR,  type));
        }
    }
}
