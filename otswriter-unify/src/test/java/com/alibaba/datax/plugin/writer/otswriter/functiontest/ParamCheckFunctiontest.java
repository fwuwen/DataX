package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.alibaba.datax.plugin.writer.otswriter.model.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterMasterProxy;
import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSHelper;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;

public class ParamCheckFunctiontest extends BaseTest{
    private static String tableName = "ParamCheckFunctiontest";
    private static Configuration p = Utils.loadConf();
    private static SyncClientInterface ots = Utils.getOTSClient();
    
    @BeforeClass
    public static void setBeforeClass() throws Exception {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("UID", PrimaryKeyType.STRING);
        
        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @AfterClass
    public static void setAfterClass() {
        ots.shutdown();
    }
    
    @Before
    public void setup() {}
    
    @After
    public void teardown() {}
    
    private String linesToJson(Map<String, String> lines) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        Set<Entry<String, String>> entrys = lines.entrySet();
        int i = 0;
        for (Entry<String, String> e : entrys) {
            if (i == (entrys.size() - 1)) {
                sb.append(e.getValue());
            } else {
                sb.append(e.getValue() + ",");
            }
            i++;
        }
        sb.append("}");
        return sb.toString();
    }
    
    private Map<String, String> getMultiVersionLines() {
        Map<String, String> lines = new LinkedHashMap<String, String>();
        lines.put("endpoint",     "\"endpoint\":\""+ p.getString("endpoint") +"\"");
        lines.put("accessId",     "\"accessId\":\""+ p.getString("accessid") +"\"");
        lines.put("accessKey",    "\"accessKey\":\""+ p.getString("accesskey") +"\"");
        lines.put("instanceName", "\"instanceName\":\""+ p.getString("instance-name") +"\"");
        lines.put("table",        "\"table\":\""+ tableName +"\"");
        lines.put("primaryKey",   "\"primaryKey\":[\"Uid\", \"Pid\", \"Mid\", \"UID\"]");
        lines.put("columnNamePrefixFilter", "\"columnNamePrefixFilter\": \"CF:\"");
        lines.put("mode",         "\"mode\":\"multiVersion\"");
        return lines;
    }
    
    private Map<String, String> getNormalLines() {
        Map<String, String> lines = new LinkedHashMap<String, String>();
        lines.put("endpoint",     "\"endpoint\":\""+ p.getString("endpoint") +"\"");
        lines.put("accessId",     "\"accessId\":\""+ p.getString("accessid") +"\"");
        lines.put("accessKey",    "\"accessKey\":\""+ p.getString("accesskey") +"\"");
        lines.put("instanceName", "\"instanceName\":\""+ p.getString("instance-name") +"\"");
        lines.put("table",        "\"table\":\""+ tableName +"\"");
        lines.put("primaryKey",   "\"primaryKey\":[\"Uid\", \"Pid\", \"Mid\", \"UID\"]");
        lines.put("column",       "\"column\":    [{\"name\":\"attr_0\", \"type\":\"String\"}]");
        lines.put("writeMode",    "\"writeMode\":\"putRow\"");
        lines.put("defaultTimestampInMillisecond", "\"defaultTimestampInMillisecond\":1450002101");
        lines.put("mode",         "\"mode\":\"normal\"");
        return lines;
    }
    
    /**
     * 测试目的：测试正常逻辑的检测是否符合预期。
     * 测试内容：构造一个合法的配置文件，期望配置文件解析正确，且参数符合预期
     * @throws Exception 
     */
    @Test
    public void testCase1() throws Exception {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        {
            Map<String, String> lines = this.getMultiVersionLines();
            String json = this.linesToJson(lines);
            Configuration configuration = Configuration.from(json);
            proxy.init(configuration);
            OTSConf conf = proxy.getOTSConf();
            
            assertEquals(p.getString("endpoint"), conf.getEndpoint());
            assertEquals(p.getString("accessid"), conf.getAccessId());
            assertEquals(p.getString("accesskey"), conf.getAccessKey());
            assertEquals(p.getString("instance-name"), conf.getInstanceName());
            assertEquals(tableName, conf.getTableName());
            assertEquals(OTSOpType.UPDATE_ROW, conf.getOperation());
            
            List<TablePrimaryKeySchema> pk = conf.getPrimaryKeyColumn();
            assertEquals(4, pk.size());
            assertEquals("Uid", pk.get(0).getName());
            assertEquals(PrimaryKeyType.STRING, pk.get(0).getType());
            assertEquals("Pid", pk.get(1).getName());
            assertEquals(PrimaryKeyType.INTEGER, pk.get(1).getType());
            assertEquals("Mid", pk.get(2).getName());
            assertEquals(PrimaryKeyType.INTEGER, pk.get(2).getType());
            assertEquals("UID", pk.get(3).getName());
            assertEquals(PrimaryKeyType.STRING, pk.get(3).getType());
            
            
            assertEquals("CF:", conf.getColumnNamePrefixFilter());
            
            assertEquals(-1, conf.getTimestamp());
            assertEquals(OTSMode.MULTI_VERSION, conf.getMode());
            
            assertEquals(18,  conf.getRetry());
            assertEquals(100, conf.getSleepInMillisecond());
            assertEquals(100, conf.getBatchWriteCount());
            assertEquals(5,  conf.getConcurrencyWrite());
            assertEquals(1,   conf.getIoThreadCount());
            assertEquals(10000, conf.getSocketTimeout());
            assertEquals(10000, conf.getConnectTimeoutInMillisecond());
            assertEquals(1024*1024, conf.getRestrictConf().getRequestTotalSizeLimitation());
        }
        
        {
            Map<String, String> lines = this.getNormalLines();
            String json = this.linesToJson(lines);
            Configuration configuration = Configuration.from(json);
            proxy.init(configuration);
            OTSConf conf = proxy.getOTSConf();
            
            assertEquals(p.getString("endpoint"), conf.getEndpoint());
            assertEquals(p.getString("accessid"), conf.getAccessId());
            assertEquals(p.getString("accesskey"), conf.getAccessKey());
            assertEquals(p.getString("instance-name"), conf.getInstanceName());
            assertEquals(tableName, conf.getTableName());
            assertEquals(OTSOpType.PUT_ROW, conf.getOperation());
            
            List<TablePrimaryKeySchema> pk = conf.getPrimaryKeyColumn();
            assertEquals(4, pk.size());
            assertEquals("Uid", pk.get(0).getName());
            assertEquals(PrimaryKeyType.STRING, pk.get(0).getType());
            assertEquals("Pid", pk.get(1).getName());
            assertEquals(PrimaryKeyType.INTEGER, pk.get(1).getType());
            assertEquals("Mid", pk.get(2).getName());
            assertEquals(PrimaryKeyType.INTEGER, pk.get(2).getType());
            assertEquals("UID", pk.get(3).getName());
            assertEquals(PrimaryKeyType.STRING, pk.get(3).getType());
            
            List<OTSAttrColumn> attr = conf.getAttributeColumn();
            assertEquals(1, attr.size());
            assertEquals("attr_0", attr.get(0).getName());
            assertEquals(ColumnType.STRING, attr.get(0).getType());
            
            assertEquals(1450002101, conf.getTimestamp());
            assertEquals(OTSMode.NORMAL, conf.getMode());
            
            assertEquals(18,  conf.getRetry());
            assertEquals(100, conf.getSleepInMillisecond());
            assertEquals(100, conf.getBatchWriteCount());
            assertEquals(5,  conf.getConcurrencyWrite());
            assertEquals(1,   conf.getIoThreadCount());
            assertEquals(10000, conf.getSocketTimeout());
            assertEquals(10000, conf.getConnectTimeoutInMillisecond());
            assertEquals(1024*1024, conf.getRestrictConf().getRequestTotalSizeLimitation());
        }
    }
    
    private void nullParamTest(String key,String expactError) {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.remove(key);
        String json = this.linesToJson(lines);
        
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals(expactError, e.getMessage());
        }
    }
    
    /**
     * 测试目的：测试必填参数的存在性检查是否符合预期。
     * 测试内容：分别不构造endpoint、accessId、accessKey、instanceName、table、primaryKey、column、writeMode、mode，期望解析出错，且错误消息符合预期
     * @throws Exception 
     */
    @Test
    public void testCase2() throws Exception {
        nullParamTest("endpoint",     "The param 'endpoint' is not exist.");
        nullParamTest("accessId",     "The param 'accessId' is not exist.");
        nullParamTest("accessKey",    "The param 'accessKey' is not exist.");
        nullParamTest("instanceName", "The param 'instanceName' is not exist.");
        nullParamTest("table",        "The param 'table' is not exist.");
        nullParamTest("primaryKey",   "The param 'primaryKey' is not exist.");
        nullParamTest("column",       "The param 'column' is not exist.");
        nullParamTest("writeMode",    "The param 'writeMode' is not exist.");
        nullParamTest("mode",         "The param 'mode' is not exist.");
    }
    
    private void emptyStringParamTest(String key,String expactError) {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put(key, "\""+ key +"\":\"\"");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals(expactError, e.getMessage());
        }
    }
    
    private void emptyArrayParamTest(String key,String expactError) {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put(key, "\""+ key +"\":[]");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals(expactError, e.getMessage());
        }
    }
    
    /**
     * 测试目的：测试参数值是否为空得检查是否符合预期。
     * 测试内容：分别构造endpoint、accessId、accessKey、instanceName、table、primaryKey、column、writeMode、mode的值为空（空字符串或者空数组），期望解析出>错，且错误消息符合预期
     */
    @Test
    public void testCase3() {
        emptyStringParamTest("endpoint",     "The param length of 'endpoint' is zero.");
        emptyStringParamTest("accessId",     "The param length of 'accessId' is zero.");
        emptyStringParamTest("accessKey",    "The param length of 'accessKey' is zero.");
        emptyStringParamTest("instanceName", "The param length of 'instanceName' is zero.");
        emptyStringParamTest("table",        "The param length of 'table' is zero.");
        emptyStringParamTest("writeMode",    "The param length of 'writeMode' is zero.");
        emptyStringParamTest("mode",         "The param length of 'mode' is zero.");
        
        emptyArrayParamTest("primaryKey",    "The param 'primaryKey' is a empty json array.");
        emptyArrayParamTest("column",        "The param 'column' is a empty json array.");
    }
    
    /**
     * 测试目的：测试primaryKey和column中有重复列名检查是否符合预期。
     * 测试内容：分别在primaryKey和column都构造同名的列，期望解析出错，且错误消息符合预期
     */
    @Test
    public void testCase4() {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        
        // Uid重复
        lines.put("primaryKey", "\"primaryKey\":[\"Uid\",\"Pid\",\"Mid\",\"UID\"]");
        lines.put("column", "\"column\":[{\"name\":\"Uid\", \"type\":\"String\"}]");
        
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals("Duplicate item in 'column' and 'primaryKey', column name : Uid .", e.getMessage());
        }
    }
    
    private void vaildWriteModeTest(String value, OTSOpType expect) throws Exception {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("writeMode", "\"writeMode\":\""+ value +"\"");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        proxy.init(configuration);
        
        OTSConf conf = proxy.getOTSConf();
        assertEquals(expect, conf.getOperation());
    }
    
    private void invalidWriteModeTest(String value, String expect) {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("writeMode", "\"writeMode\":\""+ value +"\"");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals(expect, e.getMessage());
        }
    }
    
    /**
     * 测试目的：测试writeMode值解析是否符合预期。
     * 测试内容：分别构造writeMode的值为：PutRow、UpdateRow、putrow、updaterow、PUTROW、
     * UPDATEROW，期望解析正常，值符合预期。分别构造writeMode的值为put、PUT、put-row、update，
     * 期望解析出错，错误消息符合预期
     * @throws Exception 
     */
    @Test
    public void testCase5() throws Exception {
        vaildWriteModeTest("PutRow", OTSOpType.PUT_ROW);
        vaildWriteModeTest("putrow", OTSOpType.PUT_ROW);
        vaildWriteModeTest("PUTROW", OTSOpType.PUT_ROW);
        vaildWriteModeTest("UpdateRow", OTSOpType.UPDATE_ROW);
        vaildWriteModeTest("updaterow", OTSOpType.UPDATE_ROW);
        vaildWriteModeTest("UPDATEROW", OTSOpType.UPDATE_ROW);
        
        invalidWriteModeTest("put", "The 'writeMode' only support 'PutRow' and 'UpdateRow' not 'put'.");
        invalidWriteModeTest("PUT", "The 'writeMode' only support 'PutRow' and 'UpdateRow' not 'PUT'.");
        invalidWriteModeTest("put-row", "The 'writeMode' only support 'PutRow' and 'UpdateRow' not 'put-row'.");
        invalidWriteModeTest("update", "The 'writeMode' only support 'PutRow' and 'UpdateRow' not 'update'.");
    }
    
    private void validModeTestByMulti(String value, OTSMode expect) throws Exception {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getMultiVersionLines();
        lines.put("mode", "\"mode\":\""+ value +"\"");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        proxy.init(configuration);
        
        OTSConf conf = proxy.getOTSConf();
        assertEquals(expect, conf.getMode());
    }
    
    private void validModeTestByNormal(String value, OTSMode expect) throws Exception {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("mode", "\"mode\":\""+ value +"\"");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        proxy.init(configuration);
        
        OTSConf conf = proxy.getOTSConf();
        assertEquals(expect, conf.getMode());
    }
    
    private void invalidModeTest(String value, String expect) {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("mode", "\"mode\":\""+ value +"\"");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals(expect, e.getMessage());
        }
    }
    
    /**
     * 测试目的：测试mode值解析是否符合预期。
     * 测试内容：分别构造mode的值为：multiversion、MultiVersion、multiVERSION、MULTIVERSION、
     * normal、NORMAL、Normal，期望解析正常，值符合预期。分别构造mode的值为multi、version、normalVersion，
     * 期望解析错误，且错误消息符合预期
     * @throws Exception 
     */
    @Test
    public void testCase6() throws Exception {
        validModeTestByMulti("multiversion", OTSMode.MULTI_VERSION);
        validModeTestByMulti("MultiVersion", OTSMode.MULTI_VERSION);
        validModeTestByMulti("multiVERSION", OTSMode.MULTI_VERSION);
        validModeTestByMulti("MULTIVERSION", OTSMode.MULTI_VERSION);
        validModeTestByNormal("normal", OTSMode.NORMAL);
        validModeTestByNormal("NORMAL", OTSMode.NORMAL);
        validModeTestByNormal("Normal", OTSMode.NORMAL);
        
        invalidModeTest("multi", "The 'mode' only support 'normal' and 'multiVersion' not 'multi'.");
        invalidModeTest("version", "The 'mode' only support 'normal' and 'multiVersion' not 'version'.");
        invalidModeTest("normalVersion", "The 'mode' only support 'normal' and 'multiVersion' not 'normalVersion'.");
    }

    /**
     * 测试目的：测试默认值是否符合预期。
     * 测试内容：分别为默认参数构造特定的值，期望值符合预期。
     * @throws Exception 
     */
    @Test
    public void testCase7() throws Exception {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put(OTSConst.RETRY, "\"maxRetryTime\": 11");
        lines.put(OTSConst.SLEEP_IN_MILLISECOND, "\"retrySleepInMillisecond\": 12");
        lines.put(OTSConst.BATCH_WRITE_COUNT, "\"batchWriteCount\": 13");
        lines.put(OTSConst.CONCURRENCY_WRITE, "\"concurrencyWrite\": 14");
        lines.put(OTSConst.IO_THREAD_COUNT, "\"ioThreadCount\": 15");
        lines.put(OTSConst.SOCKET_TIMEOUTIN_MILLISECOND, "\"socketTimeoutInMillisecond\": 17");
        lines.put(OTSConst.CONNECT_TIMEOUT_IN_MILLISECOND, "\"connectTimeoutInMillisecond\": 18");
        lines.put(OTSConst.REQUEST_TOTAL_SIZE_LIMITATION, "\"requestTotalSizeLimitation\": 19");
        String json = this.linesToJson(lines);
        Configuration p = Configuration.from(json);
        proxy.init(p);
        OTSConf conf = proxy.getOTSConf();
        assertEquals(11, conf.getRetry());
        assertEquals(12, conf.getSleepInMillisecond());
        assertEquals(13, conf.getBatchWriteCount());
        assertEquals(14, conf.getConcurrencyWrite());
        assertEquals(15, conf.getIoThreadCount());
        assertEquals(17, conf.getSocketTimeout());
        assertEquals(18, conf.getConnectTimeoutInMillisecond());
        assertEquals(19, conf.getRestrictConf().getRequestTotalSizeLimitation());
    }
    
    private void invalidPrimaryKey(String value, String expect) {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("primaryKey", "\"primaryKey\":["+ value +"]");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals(expect, e.getMessage());
        }
    }
    
    /**
     * 测试目的：测试primaryKey的值合法性检查是否符合预期。测试内容：分别构造
     * {\"type\":\"String\"}、
     * {\"name\":\"Uid\"}、
     * {}、
     * {\"name\":\"Uid\",\"type\":\"String\", \"value\":\"\"}、
     * {\"name\":\"Uid\", \"type\":\"Integer\"}、
     * {\"name\":\"\", \"type\":\"String\"}、
     * {\"name\":\"UID\",\"type\":\"String\"}{\"name\":\"UID\",\"type\":\"String\"}、
     * {\"name\":\"UID\",\"type\":\"Bool\"}、
     * {\"name\":\"UID\",\"type\":\"double\"}、
     * {\"name\":\"Uid\",\"type\":\"string\"}、
     * {\"name\":\"Uid\",\"type\":\"string\"}{\"name\":\"Pid\",\"type\":\"int\"}{\"name\":\"Mid\",\"type\":\"int\"}{\"name\":\"UID\",\"type\":\"string\"}{\"name\":\"xx\",\"type\":\"string\"}
     * 100个PK列
     * 期望primaryKey解析出错，错误消息符合预期。
     */
    @Test
    public void testCase8() {
        invalidPrimaryKey("{\"type\":\"String\"}", "The item is not string in 'primaryKey'.");
        invalidPrimaryKey("{\"name\":\"Uid\"}", "The item is not string in 'primaryKey'.");
        invalidPrimaryKey("{}", "The item is not string in 'primaryKey'.");
        invalidPrimaryKey("{\"name\":\"Uid\",\"type\":\"String\", \"value\":\"\"}", "The item is not string in 'primaryKey'.");
        invalidPrimaryKey("\"\"", "Can not find the pk('') at ots in 'primaryKey'.");
        invalidPrimaryKey("\"UID\"", "The count of 'primaryKey' not equal meta, input count : 1, primary key count : 4 in meta.");
        invalidPrimaryKey("\"UID\",\"UID\", \"Pid\", \"Mid\"", "Duplicate item in 'column' and 'primaryKey', column name : UID .");
        invalidPrimaryKey(
                  "\"Uid\","
                + "\"Pid\","
                + "\"Mid\","
                + "\"UID\","
                + "\"xx\"", "Can not find the pk('xx') at ots in 'primaryKey'.");
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                sb.append("\"Uid"+ i +"\"");
                if (i < 99) {
                    sb.append(",");
                }
            }
            invalidPrimaryKey(sb.toString(), "Can not find the pk('Uid0') at ots in 'primaryKey'.");
        }
    }
    
    private void invalidColumnByNormal(String value, String expect) {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("column", "\"column\":["+ value +"]");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals(expect, e.getMessage());
        }
    }
    
    
    
    private void validColumnByNormal(String value, List<OTSAttrColumn> expect) throws Exception {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("column", "\"column\":["+ value +"]");
        String json = this.linesToJson(lines);
        Configuration configuration = Configuration.from(json);
        
        proxy.init(configuration);
        
        List<OTSAttrColumn> src = proxy.getOTSConf().getAttributeColumn();
        
        assertEquals(expect.size(), src.size());
        for (int i = 0; i < expect.size(); i++) {
            OTSAttrColumn ec = expect.get(i);
            OTSAttrColumn sc = src.get(i);
            
            assertEquals(ec.getSrcName(), sc.getSrcName());
            assertEquals(ec.getName(), sc.getName());
            assertEquals(ec.getType(), sc.getType());
        }
    }

    /**
     * ## 普通模式下的参数测试
     * 测试目的：测试column的值合法性检查是否符合预期。测试内容：分别构造
     * {\"type\":\"String\"}、
     * {\"name\":\"Uid\"}、
     * {}、
     * {\"name\":\"Uid\",\"type\":\"String\", \"value\":\"\"}、
     * {\"name\":\"Uid\", \"type\":\"Integer\"}、
     * {\"name\":\"\", \"type\":\"String\"}，
     * 129个Column
     * 1000个Column
     * 期望column解析出错，错误消息符合预期。
     * @throws Exception 
     */
    @Test
    public void testCase9() throws Exception {
        invalidColumnByNormal("{\"type\":\"String\"}", "The 'name' fileds is missing in json map of 'column'.");
        invalidColumnByNormal("{\"name\":\"Uid\"}", "The 'type' fileds is missing in json map of 'column'.");
        invalidColumnByNormal("{}", "The 'type' fileds is missing in json map of 'column'.");
        invalidColumnByNormal("{\"name\":\"Uid\",\"type\":\"String\", \"value\":\"\"}", "The only support 'name' and 'type' fileds in json map of 'column'.");
        invalidColumnByNormal("{\"name\":\"Uid\", \"type\":\"Integer\"}", "Column type only support 'string','int','double','bool' and 'binary', not support 'Integer'.");
        invalidColumnByNormal("{\"name\":\"\", \"type\":\"String\"}", "The name of item can not be a empty string in 'column'.");
        
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 129; i++) {
                sb.append("{\"name\":\"Attr_"+ i +"\",\"type\":\"string\"}");
                if (i < 128) {
                    sb.append(",");
                }
            }
            invalidColumnByNormal(sb.toString(), "The input count(129) of column more than max(128).");
        }
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append("{\"name\":\"Attr_"+ i +"\",\"type\":\"string\"}");
                if (i < 999) {
                    sb.append(",");
                }
            }
            invalidColumnByNormal(sb.toString(), "The input count(1000) of column more than max(128).");
        }
        {
            List<OTSAttrColumn> expect = new ArrayList<OTSAttrColumn>();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1; i++) {
                sb.append("{\"name\":\"Attr_"+ i +"\",\"type\":\"string\"}");
                if (i < 0) {
                    sb.append(",");
                }
                expect.add(new OTSAttrColumn("Attr_"+ i, ColumnType.STRING));
            }
            validColumnByNormal(sb.toString(), expect);
        }
        {
            List<OTSAttrColumn> expect = new ArrayList<OTSAttrColumn>();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                sb.append("{\"name\":\"Attr_"+ i +"\",\"type\":\"string\"}");
                if (i < 7) {
                    sb.append(",");
                }
                expect.add(new OTSAttrColumn("Attr_"+ i, ColumnType.STRING));
            }
            validColumnByNormal(sb.toString(), expect);
        }
        {
            List<OTSAttrColumn> expect = new ArrayList<OTSAttrColumn>();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 128; i++) {
                sb.append("{\"name\":\"Attr_"+ i +"\",\"type\":\"string\"}");
                if (i < 127) {
                    sb.append(",");
                }
                expect.add(new OTSAttrColumn("Attr_"+ i, ColumnType.STRING));
            }
            validColumnByNormal(sb.toString(), expect);
        }
    }
    
    /**
     * 测试目的：测试column的值合法性检查是否符合预期。
     * 测试内容：构造128列属性列，期望解析配置正确。构造129列属性列，期望解析配置出错，且错误消息符合预期。
     * @throws Exception 
     */
    @Test
    public void testCase12() throws Exception {
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 128; i++) {
                sb.append("{\"name\":\""+ getColumnName(i) +"\", \"type\":\"String\"}");
                if (i != 128) {
                    sb.append(",");
                }
            }
            OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
            Map<String, String> lines = this.getNormalLines();
            lines.put("column",    "\"column\":["+ sb.toString() +"]");
            String json = this.linesToJson(lines);
            Configuration configuration = Configuration.from(json);
            proxy.init(configuration);
        }
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 129; i++) {
                sb.append("{\"name\":\""+ getColumnName(i) +"\", \"type\":\"String\"}");
                if (i != 128) {
                    sb.append(",");
                }
            }
            OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
            Map<String, String> lines = this.getNormalLines();
            lines.put("column",    "\"column\":["+ sb.toString() +"]");
            String json = this.linesToJson(lines);
            Configuration configuration = Configuration.from(json);
            try {
                proxy.init(configuration);
                fail();
            } catch (Exception e) {
                assertEquals("The input count(129) of column more than max(128).", e.getMessage());
            }
        }
    }
    
    /**
     * 测试目的：测试tableName长度超过预期的行为是否符合预期。
     * 测试内容：构造一个长度为max的表，输入插件中，期望解析正常。输入一个max+1的表名，期望解析错误。
     * @throws Exception 
     */
    @Test
    public void testCase13() throws Exception {
        int max = 255;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < max; i++) {
            sb.append("a");
        }
        {
            TableMeta tableMeta = new TableMeta(sb.toString());
            tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("UID", PrimaryKeyType.STRING);
            OTSHelper.createTableSafe(ots, tableMeta);
        }
        {
            TableMeta tableMeta = new TableMeta(sb.toString() + "a");
            tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("UID", PrimaryKeyType.STRING);
            try {
                OTSHelper.createTableSafe(ots, tableMeta);
                fail();
            } catch (Exception e) {
                assertEquals("Invalid table name: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.", e.getMessage());
            }
        }
    }
    
    /**
     * 测试目的：测试参数非法的参数检测是否符合预期。
     * 测试内容：构造一个不是以http开头的endpoint ，期望解析出错
     */
    @Test
    public void testCase14() {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("endpoint", "\"endpoint\":\"10.230.202.12\"");
        String json = this.linesToJson(lines);
        
        Configuration configuration = Configuration.from(json);
        try {
            proxy.init(configuration);
            fail();
        } catch (Exception e) {
            assertEquals("the endpoint must start with \"http://\" or \"https://\".", e.getMessage());
        }
    }
    
    /**
     * 测试目的：测试accessid和accesskey两边有空字符串的解析情况。
     * 测试内容：分别构造两边带空字符串的accessid和accesskey，期望程序解析正确。
     * @throws Exception 
     */
    @Test
    public void testCase15() throws Exception {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        Map<String, String> lines = this.getNormalLines();
        lines.put("accessId",     "\"accessId\":\" "+ p.getString("accessid") +"\"   ");
        lines.put("accessKey",    "\"accessKey\":\"    "+ p.getString("accesskey") +"\" ");
        String json = this.linesToJson(lines);
        
        Configuration configuration = Configuration.from(json);
        proxy.init(configuration);
    }
    
    /**
     * 测试目的：测试instanceName版本不匹配程序的解析情况。
     * 测试内容：分别构造一个Public和Legacy的instanceName，期望程序解析出错
     */
    @Test
    public void testCase16() {
        {
            OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
            Map<String, String> lines = this.getNormalLines();
            lines.put("instanceName", "\"instanceName\":\""+ p.getString("legacy-instance-name") +"\"");
            String json = this.linesToJson(lines);
            
            Configuration configuration = Configuration.from(json);
            try {
                proxy.init(configuration);
                fail();
            } catch (Exception e) {
                assertEquals("The instance does not support API version 2014-12-31, please check your SDK.", e.getMessage());
            }
        }
        {
            OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
            Map<String, String> lines = this.getNormalLines();
            lines.put("instanceName", "\"instanceName\":\""+ p.getString("public-instance-name") +"\"");
            String json = this.linesToJson(lines);
            
            Configuration configuration = Configuration.from(json);
            try {
                proxy.init(configuration);
                fail();
            } catch (Exception e) {
                assertEquals("The instance does not support API version 2014-12-31, please check your SDK.", e.getMessage());
            }
        }
    }
    
    /**
     * 测试目的：测试table解析情况。
     * 测试内容：构造两边带空字符串的table，期望程序解析正确。在表前添加instanceId，期望解析出错。
     * @throws Exception 
     */
    @Test
    public void testCase17() throws Exception {
        {
            OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
            Map<String, String> lines = this.getNormalLines();
            lines.put("table",     "\"table\":\" "+ tableName +"\"   ");
            String json = this.linesToJson(lines);
            
            Configuration configuration = Configuration.from(json);
            proxy.init(configuration);
        }
        {
            OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
            Map<String, String> lines = this.getNormalLines();
            lines.put("table", "\"table\":\""+ p.getString("instance-id") + "#" + tableName +"\"");
            String json = this.linesToJson(lines);
            
            Configuration configuration = Configuration.from(json);
            try {
                proxy.init(configuration);
                fail();
            } catch (Exception e) {
                assertEquals("Invalid table name: '"+ p.getString("instance-id") +"#"+ tableName +"'.", e.getMessage());
            }
        }
    }
    
    /**
     * 用户配置的PK和表中得PK顺序不一致
     * @throws Exception 
     */
    @Test
    public void testCase19() throws Exception {
        OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        
        StringBuilder sb = new StringBuilder();
        sb.append("\"UID\",");
        sb.append("\"Mid\",");
        sb.append("\"Uid\",");
        sb.append("\"Pid\"");
        
        String value = sb.toString();
        
        {
            Map<String, String> lines = this.getMultiVersionLines();
            lines.put("primaryKey", "\"primaryKey\":["+ value +"]");
            String json = this.linesToJson(lines);
            Configuration configuration = Configuration.from(json);
            proxy.init(configuration);
            OTSConf conf = proxy.getOTSConf();
            
            assertEquals(p.getString("endpoint"), conf.getEndpoint());
            assertEquals(p.getString("accessid"), conf.getAccessId());
            assertEquals(p.getString("accesskey"), conf.getAccessKey());
            assertEquals(p.getString("instance-name"), conf.getInstanceName());
            assertEquals(tableName, conf.getTableName());
            assertEquals(OTSOpType.UPDATE_ROW, conf.getOperation());
            
            List<TablePrimaryKeySchema> pk = conf.getPrimaryKeyColumn();
            assertEquals(4, pk.size());
            assertEquals("Uid", pk.get(2).getName());
            assertEquals(PrimaryKeyType.STRING, pk.get(2).getType());
            assertEquals("Pid", pk.get(3).getName());
            assertEquals(PrimaryKeyType.INTEGER, pk.get(3).getType());
            assertEquals("Mid", pk.get(1).getName());
            assertEquals(PrimaryKeyType.INTEGER, pk.get(1).getType());
            assertEquals("UID", pk.get(0).getName());
            assertEquals(PrimaryKeyType.STRING, pk.get(0).getType());
            
            Map<TablePrimaryKeySchema, Integer> m = Common.getPkColumnMapping(conf.getEncodePkColumnMapping());
            Set<TablePrimaryKeySchema> kks = m.keySet();
            TablePrimaryKeySchema[] pks = kks.toArray(new TablePrimaryKeySchema[kks.size()]);
            
            assertEquals(4, pks.length);
            assertEquals("Uid", pks[0].getName());
            assertEquals(PrimaryKeyType.STRING, pks[0].getType());
            assertEquals(2, m.get(pks[0]).intValue());
            
            assertEquals("Pid", pks[1].getName());
            assertEquals(PrimaryKeyType.INTEGER, pks[1].getType());
            assertEquals(3, m.get(pks[1]).intValue());
            
            assertEquals("Mid", pks[2].getName());
            assertEquals(PrimaryKeyType.INTEGER, pks[2].getType());
            assertEquals(1, m.get(pks[2]).intValue());
            
            assertEquals("UID", pks[3].getName());
            assertEquals(PrimaryKeyType.STRING, pks[3].getType());
            assertEquals(0, m.get(pks[3]).intValue());
            
            assertEquals(-1, conf.getTimestamp());
            assertEquals(OTSMode.MULTI_VERSION, conf.getMode());
            
            assertEquals(18,  conf.getRetry());
            assertEquals(100, conf.getSleepInMillisecond());
            assertEquals(100, conf.getBatchWriteCount());
            assertEquals(5,  conf.getConcurrencyWrite());
            assertEquals(1,   conf.getIoThreadCount());
            assertEquals(10000, conf.getSocketTimeout());
            assertEquals(10000, conf.getConnectTimeoutInMillisecond());
            assertEquals(1024*1024, conf.getRestrictConf().getRequestTotalSizeLimitation());
        }
        
        {
            Map<String, String> lines = this.getNormalLines();
            lines.put("primaryKey", "\"primaryKey\":["+ value +"]");
            String json = this.linesToJson(lines);
            Configuration configuration = Configuration.from(json);
            proxy.init(configuration);
            OTSConf conf = proxy.getOTSConf();
            
            assertEquals(p.getString("endpoint"), conf.getEndpoint());
            assertEquals(p.getString("accessid"), conf.getAccessId());
            assertEquals(p.getString("accesskey"), conf.getAccessKey());
            assertEquals(p.getString("instance-name"), conf.getInstanceName());
            assertEquals(tableName, conf.getTableName());
            assertEquals(OTSOpType.PUT_ROW, conf.getOperation());
            
            List<TablePrimaryKeySchema> pk = conf.getPrimaryKeyColumn();
            assertEquals(4, pk.size());
            assertEquals("Uid", pk.get(2).getName());
            assertEquals(PrimaryKeyType.STRING, pk.get(2).getType());
            assertEquals("Pid", pk.get(3).getName());
            assertEquals(PrimaryKeyType.INTEGER, pk.get(3).getType());
            assertEquals("Mid", pk.get(1).getName());
            assertEquals(PrimaryKeyType.INTEGER, pk.get(1).getType());
            assertEquals("UID", pk.get(0).getName());
            assertEquals(PrimaryKeyType.STRING, pk.get(0).getType());
            
            Map<TablePrimaryKeySchema, Integer> m = Common.getPkColumnMapping(conf.getEncodePkColumnMapping());
            Set<TablePrimaryKeySchema> kks = m.keySet();
            TablePrimaryKeySchema[] pks = kks.toArray(new TablePrimaryKeySchema[kks.size()]);
            
            assertEquals(4, pks.length);
            assertEquals("Uid", pks[0].getName());
            assertEquals(PrimaryKeyType.STRING, pks[0].getType());
            assertEquals(2, m.get(pks[0]).intValue());
            
            assertEquals("Pid", pks[1].getName());
            assertEquals(PrimaryKeyType.INTEGER, pks[1].getType());
            assertEquals(3, m.get(pks[1]).intValue());
            
            assertEquals("Mid", pks[2].getName());
            assertEquals(PrimaryKeyType.INTEGER, pks[2].getType());
            assertEquals(1, m.get(pks[2]).intValue());
            
            assertEquals("UID", pks[3].getName());
            assertEquals(PrimaryKeyType.STRING, pks[3].getType());
            assertEquals(0, m.get(pks[3]).intValue());
            
            List<OTSAttrColumn> attr = conf.getAttributeColumn();
            assertEquals(1, attr.size());
            assertEquals("attr_0", attr.get(0).getName());
            assertEquals(ColumnType.STRING, attr.get(0).getType());
            
            assertEquals(1450002101, conf.getTimestamp());
            assertEquals(OTSMode.NORMAL, conf.getMode());
            
            assertEquals(18,  conf.getRetry());
            assertEquals(100, conf.getSleepInMillisecond());
            assertEquals(100, conf.getBatchWriteCount());
            assertEquals(5,  conf.getConcurrencyWrite());
            assertEquals(1,   conf.getIoThreadCount());
            assertEquals(10000, conf.getSocketTimeout());
            assertEquals(10000, conf.getConnectTimeoutInMillisecond());
            assertEquals(1024*1024, conf.getRestrictConf().getRequestTotalSizeLimitation());
        }
    }
}
