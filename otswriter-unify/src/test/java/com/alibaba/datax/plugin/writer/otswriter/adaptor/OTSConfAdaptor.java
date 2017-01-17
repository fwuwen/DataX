package com.alibaba.datax.plugin.writer.otswriter.adaptor;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alibaba.datax.plugin.writer.otswriter.model.*;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OTSConfAdaptor implements JsonDeserializer<OTSConf>, JsonSerializer<OTSConf>{
    private final static String ENDPOINT = "endpoint";
    private final static String ACCESS_ID = "accessId";
    private final static String ACCESS_KEY = "accessKey";
    private final static String INSTANCE_NAME = "instanceName";
    private final static String TABLE_NAME = "table";
    private final static String PRIMARY_KEY = "primaryKey";
    private final static String ATTRIBUTE = "column";
    private final static String OPERATION = "writeMode";
    private final static String MODE = "mode";
    private final static String TIMESTAMP = "defaultTimestampInMillionSecond";
    
    @Override
    public JsonElement serialize(OTSConf src, Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject json = new JsonObject();
        json.add(ENDPOINT, new JsonPrimitive(src.getEndpoint()));
        json.add(ACCESS_ID, new JsonPrimitive(src.getAccessId()));
        json.add(ACCESS_KEY, new JsonPrimitive(src.getAccessKey()));
        json.add(INSTANCE_NAME, new JsonPrimitive(src.getInstanceName()));
        json.add(TABLE_NAME, new JsonPrimitive(src.getTableName()));
        json.add(TIMESTAMP, new JsonPrimitive(src.getTimestamp()));
        
        JsonArray jsonPrimaryKeyArray = new JsonArray();
        for (TablePrimaryKeySchema column : src.getPrimaryKeyColumn()) {
            jsonPrimaryKeyArray.add(context.serialize(column));
        }
        json.add(PRIMARY_KEY, jsonPrimaryKeyArray);
        
        JsonArray jsonAttributeArray = new JsonArray();
        for (OTSAttrColumn column : src.getAttributeColumn()) {
            jsonAttributeArray.add(context.serialize(column));
        }
        json.add(ATTRIBUTE, jsonAttributeArray);
        
        json.add(OPERATION, new JsonPrimitive(src.getOperation() == OTSOpType.PUT_ROW ? OTSConst.OTS_OP_TYPE_PUT : OTSConst.OTS_OP_TYPE_UPDATE));
        json.add(MODE, new JsonPrimitive(src.getMode() == OTSMode.NORMAL? OTSConst.OTS_MODE_NORMAL : OTSConst.OTS_MODE_MULTI_VERSION));
        return json;
    }

    @Override
    public OTSConf deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        JsonObject obj = json.getAsJsonObject();
        OTSConf conf = new OTSConf();
        conf.setEndpoint(obj.getAsJsonPrimitive(ENDPOINT).getAsString());
        conf.setAccessId(obj.getAsJsonPrimitive(ACCESS_ID).getAsString());
        conf.setAccessKey(obj.getAsJsonPrimitive(ACCESS_KEY).getAsString());
        conf.setInstanceName(obj.getAsJsonPrimitive(INSTANCE_NAME).getAsString());
        conf.setTableName(obj.getAsJsonPrimitive(TABLE_NAME).getAsString());
        conf.setTimestamp(obj.getAsJsonPrimitive(TIMESTAMP).getAsLong());
        
        JsonArray primaryKey = obj.getAsJsonArray(PRIMARY_KEY);
        List<TablePrimaryKeySchema> primaryKeyColumn = new ArrayList<TablePrimaryKeySchema>();
        for (Iterator<JsonElement> iter = primaryKey.iterator(); iter.hasNext();) {
            TablePrimaryKeySchema pk = (TablePrimaryKeySchema) context.deserialize(iter.next(), TablePrimaryKeySchema.class);
            primaryKeyColumn.add(pk);
        }
        conf.setPrimaryKeyColumn(primaryKeyColumn);
        
        JsonArray attribute = obj.getAsJsonArray(ATTRIBUTE);
        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        for (Iterator<JsonElement> iter = attribute.iterator(); iter.hasNext();) {
            OTSAttrColumn attr = (OTSAttrColumn) context.deserialize(iter.next(), OTSAttrColumn.class);
            attributeColumn.add(attr);
        }
        conf.setAttributeColumn(attributeColumn);
        String op = obj.getAsJsonPrimitive(OPERATION).getAsString();
        if (op.equalsIgnoreCase(OTSConst.OTS_OP_TYPE_PUT)) {
            conf.setOperation(OTSOpType.PUT_ROW);
        } else {
            conf.setOperation(OTSOpType.UPDATE_ROW);
        }
        String mode = obj.getAsJsonPrimitive(MODE).getAsString();
        if (mode.equalsIgnoreCase(OTSConst.OTS_MODE_NORMAL)) {
            conf.setMode(OTSMode.NORMAL);
        } else {
            conf.setMode(OTSMode.MULTI_VERSION);
        }
        return conf;
    }
}
