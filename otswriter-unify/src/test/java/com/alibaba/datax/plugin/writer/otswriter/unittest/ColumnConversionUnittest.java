package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.plugin.writer.otswriter.model.TablePrimaryKeySchema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.common.Person;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.utils.ColumnConversion;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.google.gson.Gson;

/**
 * Column的数据转换测试
 * @author redchen
 *
 */
public class ColumnConversionUnittest {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnConversionUnittest.class);
    
    class PKItem {
        private Column src;
        private TablePrimaryKeySchema type;
        private PrimaryKeyValue expect;
        
        // 备注：第一个元素表示需要被转换的Column，第二个元素表示期望转义的类型，第三个元素表示期望的值
        public PKItem(Column src, TablePrimaryKeySchema type, PrimaryKeyValue expect) {
            this.src = src;
            this.type = type;
            this.expect = expect;
        }

        public Column getSrc() {
            return src;
        }

        public TablePrimaryKeySchema getType() {
            return type;
        }

        public PrimaryKeyValue getExpect() {
            return expect;
        }
    }
    
    class AttrItem {
        private Column src;
        private OTSAttrColumn type;
        private ColumnValue expect;
        
        // 备注：第一个元素表示需要被转换的Column，第二个元素表示期望转义的类型，第三个元素表示期望的值
        public AttrItem(Column src, OTSAttrColumn type, ColumnValue expect) {
            this.src = src;
            this.type = type;
            this.expect = expect;
        }

        public Column getSrc() {
            return src;
        }

        public OTSAttrColumn getType() {
            return type;
        }

        public ColumnValue getExpect() {
            return expect;
        }
    }
    
    private boolean pkEqual(TablePrimaryKeySchema type, PrimaryKeyValue col1, PrimaryKeyValue col2) {
        switch (type.getType()) {
        case INTEGER:
            return col1.asLong() == col2.asLong() ;
        case STRING:
            return col1.asString().equals(col2.asString());
        default:
            throw new RuntimeException("Not support.");
        }
    }
    
    private boolean bytesEqual(byte[] b1, byte[] b2) {
        if (b1.length != b2.length) {
            return false;
        }
        for (int i = 0; i < b1.length; i++) {
            if (b1[i] != b2[i]) {
                return false;
            }
        }
        return true;
    }
    
    private boolean attrEqual(OTSAttrColumn type, ColumnValue col1, ColumnValue col2) {
        switch (type.getType()) {
        case BINARY:
            return bytesEqual(col1.asBinary(), col2.asBinary());
        case BOOLEAN:
            return col1.asBoolean() == col2.asBoolean() ;
        case DOUBLE:
            return col1.asDouble() == col2.asDouble() ;
        case INTEGER:
            return col1.asLong() == col2.asLong() ;
        case STRING:
            return col1.asString().equals(col2.asString());
        default:
            throw new RuntimeException("Not support.");
        }
    }
    
    /**
     * 输入：传入合法的Column，和预期的PK Type
     * 期望：函数能正在转换Column，且最终的值正确
     * @throws UnsupportedEncodingException 
     * @throws OTSCriticalException 
     */
    @Test
    public void testColumnToPrimaryKeyValueValid() throws UnsupportedEncodingException, OTSCriticalException {

        List<PKItem> input = new ArrayList<PKItem>();
        // string->string
        // English, 中文, mētēr, にほんご, 한국어
        input.add(new PKItem(new StringColumn("English"), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("English")));
        input.add(new PKItem(new StringColumn("中文"), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("中文")));
        input.add(new PKItem(new StringColumn("mētēr"), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("mētēr")));
        input.add(new PKItem(new StringColumn("にほんご"), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("にほんご")));
        input.add(new PKItem(new StringColumn("한국어"), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("한국어")));
        
        // int -> string
        // 122211
        input.add(new PKItem(new LongColumn(122211), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("122211")));
        
        // double -> string
        // -12.01, 0, 0.0, 109.0
        input.add(new PKItem(new DoubleColumn(-12.01), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("-12.01")));
        input.add(new PKItem(new DoubleColumn(0), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("0")));
        input.add(new PKItem(new DoubleColumn(0.0), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("0.0")));
        input.add(new PKItem(new DoubleColumn(109.0), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("109.0")));
        
        // bool -> string
        // true, false
        input.add(new PKItem(new BoolColumn(true), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("true")));
        input.add(new PKItem(new BoolColumn(false), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("false")));
        
        // binary -> string
        input.add(new PKItem(new BytesColumn("hello".getBytes("UTF-8")), new TablePrimaryKeySchema("name", PrimaryKeyType.STRING), PrimaryKeyValue.fromString("hello")));
        
        // string->int
        // 90, -12.2, 0, 8973277.009012, 100E10
        input.add(new PKItem(new StringColumn("90"), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(90)));
        input.add(new PKItem(new StringColumn("-12.2"), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(-12)));
        input.add(new PKItem(new StringColumn("0"), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(0)));
        input.add(new PKItem(new StringColumn("8973277.009012"), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(8973277)));
        input.add(new PKItem(new StringColumn("100E2"), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(10000)));
        
        // int -> int
        // Long.min, Long.max, -99, 0, 123
        input.add(new PKItem(new LongColumn(Long.MIN_VALUE), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(Long.MIN_VALUE)));
        input.add(new PKItem(new LongColumn(Long.MAX_VALUE), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(Long.MAX_VALUE)));
        input.add(new PKItem(new LongColumn(-99), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(-99)));
        input.add(new PKItem(new LongColumn(0), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(0)));
        input.add(new PKItem(new LongColumn(123), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(123)));
        
        // double -> int
        // -100.01, 0, 281
        input.add(new PKItem(new DoubleColumn(-100.01), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(-100)));
        input.add(new PKItem(new DoubleColumn(0), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(0)));
        input.add(new PKItem(new DoubleColumn(281), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(281)));
        
        // bool -> int
        input.add(new PKItem(new BoolColumn(true), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(1)));
        input.add(new PKItem(new BoolColumn(false), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), PrimaryKeyValue.fromLong(0)));
        Gson g = new Gson();
        for (PKItem item : input) {
            LOG.info("Item: {}", g.toJson(item));
            assertTrue(pkEqual(item.getType(), item.getExpect(), ColumnConversion.columnToPrimaryKeyValue(item.getSrc(), item.getType())));
        }
    }
    
    /**
     * 输入：传入非法的Column，和预期的PK Type
     * 期望：在Column转换过程中抛出异常
     * @throws UnsupportedEncodingException 
     */
    @Test
    public void testColumnToPrimaryKeyValueInvalid() throws UnsupportedEncodingException {
        Map<PKItem, String> input = new LinkedHashMap<PKItem, String>();
        
        // string->int， 非数值型的字符串
        // hello, 0x5f, 100L, 102E2
        input.put(new PKItem(new StringColumn("hello"), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), null) , "Column coversion error, src type : STRING, src value: hello, expect type: INTEGER .");
        input.put(new PKItem(new StringColumn("0x5f"), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), null) , "Column coversion error, src type : STRING, src value: 0x5f, expect type: INTEGER .");
        input.put(new PKItem(new StringColumn("100L"), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), null) , "Column coversion error, src type : STRING, src value: 100L, expect type: INTEGER .");

        // binary -> int
        input.put(new PKItem(new BytesColumn("world".getBytes()), new TablePrimaryKeySchema("name", PrimaryKeyType.INTEGER), null) , "Column coversion error, src type : BYTES, src value: world, expect type: INTEGER .");
        Gson g = new Gson();
        for (Entry<PKItem, String> item : input.entrySet()) {
            LOG.info("Item: {}, Expect:{}", g.toJson(item.getKey()), item.getValue());
            try {
                ColumnConversion.columnToPrimaryKeyValue(item.getKey().getSrc(), item.getKey().getType());
                assertTrue(false);
            } catch (Exception e) {
                assertEquals(item.getValue(), e.getMessage());
            }
        }
    }
    
    /**
     * 输入：传入合法的Column，和预期的Column Type
     * 期望：函数能正在转换Column，且最终的值正确
     * @throws UnsupportedEncodingException 
     * @throws OTSCriticalException 
     */
    @Test
    public void testColumnToColumnValueValid() throws UnsupportedEncodingException, OTSCriticalException {
        
        List<AttrItem> input = new ArrayList<AttrItem>();
        // string->string
        // English, 中文, mētēr, にほんご, 한국어
        input.add(new AttrItem(new StringColumn("English"), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("English")));
        input.add(new AttrItem(new StringColumn("中文"), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("中文")));
        input.add(new AttrItem(new StringColumn("mētēr"), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("mētēr")));
        input.add(new AttrItem(new StringColumn("にほんご"), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("にほんご")));
        input.add(new AttrItem(new StringColumn("한국어"), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("한국어")));
        
        // int -> string
        // 122211
        input.add(new AttrItem(new LongColumn(122211), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("122211")));
        
        // double -> string
        // -12.01, 0, 0.0, 109.0
        input.add(new AttrItem(new DoubleColumn(-12.01), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("-12.01")));
        input.add(new AttrItem(new DoubleColumn(0), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("0")));
        input.add(new AttrItem(new DoubleColumn(0.0), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("0.0")));
        input.add(new AttrItem(new DoubleColumn(109.0), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("109.0")));
        
        // bool -> string
        // true, false
        input.add(new AttrItem(new BoolColumn(true), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("true")));
        input.add(new AttrItem(new BoolColumn(false), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("false")));
        
        // binary -> string
        input.add(new AttrItem(new BytesColumn("hello".getBytes("UTF-8")), new OTSAttrColumn("", ColumnType.STRING), ColumnValue.fromString("hello")));
        
        // string->int
        // 90, -12.2, 0, 8973277.009012
        input.add(new AttrItem(new StringColumn("90"), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(90)));
        input.add(new AttrItem(new StringColumn("-12.2"), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(-12)));
        input.add(new AttrItem(new StringColumn("0"), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(0)));
        input.add(new AttrItem(new StringColumn("8973277.009012"), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(8973277)));
        
        // int -> int
        // Long.min, Long.max, -99, 0, 123
        input.add(new AttrItem(new LongColumn(Long.MIN_VALUE), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(Long.MIN_VALUE)));
        input.add(new AttrItem(new LongColumn(Long.MAX_VALUE), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(Long.MAX_VALUE)));
        input.add(new AttrItem(new LongColumn(-99), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(-99)));
        input.add(new AttrItem(new LongColumn(0), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(0)));
        input.add(new AttrItem(new LongColumn(123), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(123)));
        
        // double -> int
        // -100.01, 0, 281
        input.add(new AttrItem(new DoubleColumn(-100.01), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(-100)));
        input.add(new AttrItem(new DoubleColumn(0), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(0)));
        input.add(new AttrItem(new DoubleColumn(281), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(281)));
        
        // bool -> int
        input.add(new AttrItem(new BoolColumn(true), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(1)));
        input.add(new AttrItem(new BoolColumn(false), new OTSAttrColumn("", ColumnType.INTEGER), ColumnValue.fromLong(0)));
        
        // string -> double
        // -1942.010, 0.0000, 9999.1212121
        input.add(new AttrItem(new StringColumn("-1942.010"), new OTSAttrColumn("", ColumnType.DOUBLE), ColumnValue.fromDouble(-1942.01)));
        input.add(new AttrItem(new StringColumn("0.0000"), new OTSAttrColumn("", ColumnType.DOUBLE), ColumnValue.fromDouble(0)));
        input.add(new AttrItem(new StringColumn("9999.1212121"), new OTSAttrColumn("", ColumnType.DOUBLE), ColumnValue.fromDouble(9999.1212121)));
        
        // int -> double
        // -3131, 0 ,1231231
        input.add(new AttrItem(new LongColumn(-3131), new OTSAttrColumn("", ColumnType.DOUBLE), ColumnValue.fromDouble(-3131)));
        input.add(new AttrItem(new LongColumn(0), new OTSAttrColumn("", ColumnType.DOUBLE), ColumnValue.fromDouble(0)));
        input.add(new AttrItem(new LongColumn(1231231), new OTSAttrColumn("", ColumnType.DOUBLE), ColumnValue.fromDouble(1231231)));
        
        // bool -> double
        // true, false
        input.add(new AttrItem(new BoolColumn(true), new OTSAttrColumn("", ColumnType.DOUBLE), ColumnValue.fromDouble(1)));
        input.add(new AttrItem(new BoolColumn(false), new OTSAttrColumn("", ColumnType.DOUBLE), ColumnValue.fromDouble(0)));
        
        
        // string -> bool
        // true, TRUE, trUe, false , FALSE, FALSESS
        input.add(new AttrItem(new StringColumn("true"), new OTSAttrColumn("", ColumnType.BOOLEAN), ColumnValue.fromBoolean(true)));
        input.add(new AttrItem(new StringColumn("TRUE"), new OTSAttrColumn("", ColumnType.BOOLEAN), ColumnValue.fromBoolean(true)));
        input.add(new AttrItem(new StringColumn("trUe"), new OTSAttrColumn("", ColumnType.BOOLEAN), ColumnValue.fromBoolean(true)));
        input.add(new AttrItem(new StringColumn("false"), new OTSAttrColumn("", ColumnType.BOOLEAN), ColumnValue.fromBoolean(false)));
        input.add(new AttrItem(new StringColumn("FALSE"), new OTSAttrColumn("", ColumnType.BOOLEAN), ColumnValue.fromBoolean(false)));
        
        // int -> bool
        // -10, 0, 10
        input.add(new AttrItem(new LongColumn(-10), new OTSAttrColumn("", ColumnType.BOOLEAN), ColumnValue.fromBoolean(true)));
        input.add(new AttrItem(new LongColumn(0), new OTSAttrColumn("", ColumnType.BOOLEAN), ColumnValue.fromBoolean(false)));
        input.add(new AttrItem(new LongColumn(10), new OTSAttrColumn("", ColumnType.BOOLEAN), ColumnValue.fromBoolean(true)));
        
        // string -> binary
        // hello, world
        input.add(new AttrItem(new StringColumn("hello"), new OTSAttrColumn("", ColumnType.BINARY), ColumnValue.fromBinary("hello".getBytes())));
        input.add(new AttrItem(new StringColumn("world"), new OTSAttrColumn("", ColumnType.BINARY), ColumnValue.fromBinary("world".getBytes())));
        
        // binary -> binary
        // Person
        Person person = new Person();
        person.setName("陈万红");
        person.setAge(25);
        person.setHeight(178);
        person.setMale(true);
        input.add(new AttrItem(new BytesColumn(Person.toByte(person)), new OTSAttrColumn("", ColumnType.BINARY), ColumnValue.fromBinary(Person.toByte(person))));
        
        Gson g = new Gson();
        for (AttrItem item : input) {
            LOG.info("Item:{}", g.toJson(item));
            assertTrue(attrEqual(item.getType(), item.getExpect(), ColumnConversion.columnToColumnValue(item.getSrc(), item.getType())));
        }
    }
    
    /**
     * 输入：传入非法的Column，和预期的Column Type
     * 期望：在Column转换过程中抛出异常
     * @throws UnsupportedEncodingException 
     */
    @Test
    public void testColumnToColumnValueInvalid() throws UnsupportedEncodingException {
        Map<AttrItem, String> input = new LinkedHashMap<AttrItem, String>();
        
        // string->int， 非数值型的字符串
        // 0x4f, hello, 100L
        input.put(new AttrItem(new StringColumn("0x4f"), new OTSAttrColumn("", ColumnType.INTEGER), null), "Column coversion error, src type : STRING, src value: 0x4f, expect type: INTEGER .");
        input.put(new AttrItem(new StringColumn("hello"), new OTSAttrColumn("", ColumnType.INTEGER), null), "Column coversion error, src type : STRING, src value: hello, expect type: INTEGER .");
        input.put(new AttrItem(new StringColumn("100L"), new OTSAttrColumn("", ColumnType.INTEGER), null), "Column coversion error, src type : STRING, src value: 100L, expect type: INTEGER .");
        
        // binary -> int
        // hello
        input.put(new AttrItem(new BytesColumn("hello".getBytes("UTF-8")), new OTSAttrColumn("", ColumnType.INTEGER), null), "Column coversion error, src type : BYTES, src value: hello, expect type: INTEGER .");
        
        // string -> double
        // 0x4f, hello, 100L
        input.put(new AttrItem(new StringColumn("0x4f"), new OTSAttrColumn("", ColumnType.DOUBLE), null), "Column coversion error, src type : STRING, src value: 0x4f, expect type: DOUBLE .");
        input.put(new AttrItem(new StringColumn("hello"), new OTSAttrColumn("", ColumnType.DOUBLE), null), "Column coversion error, src type : STRING, src value: hello, expect type: DOUBLE .");
        input.put(new AttrItem(new StringColumn("100L"), new OTSAttrColumn("", ColumnType.DOUBLE), null), "Column coversion error, src type : STRING, src value: 100L, expect type: DOUBLE .");
        
        // binary -> double
        // world
        input.put(new AttrItem(new BytesColumn("world".getBytes("UTF-8")), new OTSAttrColumn("", ColumnType.DOUBLE), null), "Column coversion error, src type : BYTES, src value: world, expect type: DOUBLE .");
        
        // string -> bool
        // TRU, TRUEE, fal, falsss
        input.put(new AttrItem(new StringColumn("TRU"), new OTSAttrColumn("", ColumnType.BOOLEAN), null), "Column coversion error, src type : STRING, src value: TRU, expect type: BOOLEAN .");
        input.put(new AttrItem(new StringColumn("TRUEE"), new OTSAttrColumn("", ColumnType.BOOLEAN), null), "Column coversion error, src type : STRING, src value: TRUEE, expect type: BOOLEAN .");
        input.put(new AttrItem(new StringColumn("fal"), new OTSAttrColumn("", ColumnType.BOOLEAN), null), "Column coversion error, src type : STRING, src value: fal, expect type: BOOLEAN .");
        input.put(new AttrItem(new StringColumn("falsss"), new OTSAttrColumn("", ColumnType.BOOLEAN), null), "Column coversion error, src type : STRING, src value: falsss, expect type: BOOLEAN .");
        
        // double -> bool
        // 0.0, 1.0, -1.0
        input.put(new AttrItem(new DoubleColumn(0.0), new OTSAttrColumn("", ColumnType.BOOLEAN), null), "Column coversion error, src type : DOUBLE, src value: 0.0, expect type: BOOLEAN .");
        input.put(new AttrItem(new DoubleColumn(1.0), new OTSAttrColumn("", ColumnType.BOOLEAN), null), "Column coversion error, src type : DOUBLE, src value: 1.0, expect type: BOOLEAN .");
        input.put(new AttrItem(new DoubleColumn(-1.0), new OTSAttrColumn("", ColumnType.BOOLEAN), null), "Column coversion error, src type : DOUBLE, src value: -1.0, expect type: BOOLEAN .");
        
        // binary -> bool
        // 杭州
        input.put(new AttrItem(new BytesColumn("杭州".getBytes("UTF-8")), new OTSAttrColumn("", ColumnType.BOOLEAN), null), "Column coversion error, src type : BYTES, src value: 杭州, expect type: BOOLEAN .");
        
        Gson g = new Gson();
        for (Entry<AttrItem, String> item : input.entrySet()) {
            LOG.info("Item: {}, Expect: {}", g.toJson(item.getKey()), item.getValue());
            try {
                ColumnConversion.columnToColumnValue(item.getKey().getSrc(), item.getKey().getType());
                assertTrue(false);
            } catch (Exception e) {
                assertEquals(item.getValue(), e.getMessage());
            }
        }

    }
}
