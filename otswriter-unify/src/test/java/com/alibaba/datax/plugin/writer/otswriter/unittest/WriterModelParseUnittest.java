package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.utils.WriterModelParser;
import com.google.gson.Gson;

public class WriterModelParseUnittest {
    
    private static final Logger LOG = LoggerFactory.getLogger(WriterModelParseUnittest.class);

    /**
     * 测试解析OTSPKColumn的正确性
     * 输入：非法的Column字符串
     * 期望：函数解析数据异常，异常消息符合预期
     */
    @Test
    public void testParseOTSPKColumn() {
        
        // 备注：key是错误的对象，value是对应的解析异常预期错误消息
        Map<Map<String, Object>, String> input = new LinkedHashMap<Map<String, Object>, String>();
        
        {
            // 缺少type，{"name":"xx"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            input.put(t, "The 'type' fileds is missing in json map of 'primaryKey'.");
        }
        {
            // 缺少name，{"type":"int"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("type", "int");
            input.put(t, "The 'name' fileds is missing in json map of 'primaryKey'.");
        }
        {
            // 缺少name和type, {}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            input.put(t, "The 'type' fileds is missing in json map of 'primaryKey'.");
        }
        {
            // 包括name和type，但是多了一个value, {"name":"xx", "type":"string", "value":""}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            t.put("type", "string");
            t.put("value", "");
            input.put(t, "The only support 'name' and 'type' fileds in json map of 'primaryKey'.");
        }
        {
            // 错误的type值, {"name":"xx", "type":"hello"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            t.put("type", "hello");
            input.put(t, "Primary key type only support 'string', 'int' and 'binary', not support 'hello'.");
        }
        {
            // 空字符串name，{"name":"", "type":"int"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "");
            t.put("type", "int");
            input.put(t, "The name of item can not be a empty string in 'primaryKey'.");
            
        }
        {
            // 传入了类型为Double的Column, {"name":"xx", "type":"double"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            t.put("type", "double");
            input.put(t, "Primary key type only support 'string', 'int' and 'binary', not support 'double'.");
        }
        {
            // 传入了类型为Bool的Column, {"name":"xx", "type":"bool"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            t.put("type", "bool");
            input.put(t, "Primary key type only support 'string', 'int' and 'binary', not support 'bool'.");
        }

        Gson g = new Gson();
        for (Entry<Map<String, Object>, String> e : input.entrySet()) {
            LOG.info("Map: {}, Expect: {}", g.toJson(e.getKey()), e.getValue());
            try {
                WriterModelParser.parseOTSPKColumn(e.getKey());
                fail();
            } catch (Exception ee) {
                assertEquals(e.getValue(), ee.getMessage());
            }
        }
    }
    
    /**
     * 测试解析OTSAttrColumn的正确性
     * 输入：非法的Column字符串
     * 期望：函数解析数据异常，异常消息符合预期
     */
    @Test
    public void testParseOTSAttrColumnForNormal() {
        
        // 备注：key是错误的对象，value是对应的解析异常预期错误消息
        Map<Map<String, Object>, String> input = new LinkedHashMap<Map<String, Object>, String>();
        {
            // 缺少type，{"name":"xx"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            input.put(t, "The 'type' fileds is missing in json map of 'column'.");
        }
        {
            // 缺少name，{"type":"int"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("type", "int");
            input.put(t, "The 'name' fileds is missing in json map of 'column'.");
        }
        {
            // 缺少name和type, {}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            input.put(t, "The 'type' fileds is missing in json map of 'column'.");
        }
        {
            // 包括name和type，但是多了一个value, {"name":"xx", "type":"string", "value":""}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            t.put("type", "string");
            t.put("value", "");
            input.put(t, "The only support 'name' and 'type' fileds in json map of 'column'.");
        }
        {
            // 错误的type值, {"name":"xx", "type":"hello"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            t.put("type", "hello");
            input.put(t, "Column type only support 'string','int','double','bool' and 'binary', not support 'hello'.");
        }
        {
            // 空字符串name，{"name":"", "type":"int"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "");
            t.put("type", "int");
            input.put(t, "The name of item can not be a empty string in 'column'.");
        }

        Gson g = new Gson();
        for (Entry<Map<String, Object>, String> e : input.entrySet()) {
            LOG.info("Map: {}, Expect: {}", g.toJson(e.getKey()), e.getValue());
            try {
                WriterModelParser.parseOTSAttrColumn(e.getKey(), OTSMode.NORMAL);
                fail();
            } catch (Exception ee) {
                assertEquals(e.getValue(), ee.getMessage());
            }
        }
    }
    
    /**
     * 验证在多版本模式下的参数解析
     */
    @Test
    public void testParseOTSAttrColumnForMultiVersion() {
        Map<Map<String, Object>, String> input = new LinkedHashMap<Map<String, Object>, String>();
        {
            // 缺少type，{"name":"xx"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("name", "xx");
            input.put(t, "The 'type' fileds is missing in json map of 'column'.");
        }
        {
            // 缺少name，{"type":"int"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("type", "int");
            input.put(t, "The 'name' fileds is missing in json map of 'column'.");
        }
        {
            // 缺少name和type, {}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            input.put(t, "The 'type' fileds is missing in json map of 'column'.");
        }
        {
            // 包括name和type，但是多了一个value, {"name":"xx", "type":"string", "value":""}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("srcName", "pp");
            t.put("name", "xx");
            t.put("type", "string");
            t.put("value", "");
            input.put(t, "The only support 'srcName', 'name' and 'type' fileds in json map of 'column'.");
        }
        {
            // 错误的type值, {"name":"xx", "type":"hello"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("srcName", "pp");
            t.put("name", "xx");
            t.put("type", "hello");
            input.put(t, "Column type only support 'string','int','double','bool' and 'binary', not support 'hello'.");
        }
        {
            // 空字符串name，{"name":"", "type":"int"}
            Map<String, Object> t = new LinkedHashMap<String, Object>();
            t.put("srcName", "pp");
            t.put("name", "");
            t.put("type", "int");
            input.put(t, "The name of item can not be a empty string in 'column'.");
        }

        Gson g = new Gson();
        for (Entry<Map<String, Object>, String> e : input.entrySet()) {
            LOG.info("Map: {}, Expect: {}", g.toJson(e.getKey()), e.getValue());
            try {
                WriterModelParser.parseOTSAttrColumn(e.getKey(), OTSMode.MULTI_VERSION);
                fail();
            } catch (Exception ee) {
                assertEquals(e.getValue(), ee.getMessage());
            }
        }
    }
}
