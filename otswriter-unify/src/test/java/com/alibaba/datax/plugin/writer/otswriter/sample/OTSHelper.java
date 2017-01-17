package com.alibaba.datax.plugin.writer.otswriter.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.PartitionRange;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.google.gson.Gson;

/**
 * 建表，删表，读取数据
 * @author redchen
 *
 */
class TableJson {
    private String tableName;
    private int maxVersion;
    private int timeToLive;
    private List<PrimaryKeySchema> pks;
    private List<PartitionRange> partitions;
    
    public List<PartitionRange> getPartitions() {
        return partitions;
    }
    public void setPartitions(List<PartitionRange> partitions) {
        this.partitions = partitions;
    }
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public int getMaxVersion() {
        return maxVersion;
    }
    public void setMaxVersion(int maxVersion) {
        this.maxVersion = maxVersion;
    }
    public int getTimeToLive() {
        return timeToLive;
    }
    public void setTimeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
    }
    public List<PrimaryKeySchema> getPks() {
        return pks;
    }
    public void setPks(List<PrimaryKeySchema> pks) {
        this.pks = pks;
    }
}

class GetJson {
    private String tableName;
    private int maxVersion;
    private PrimaryKey pk;
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public int getMaxVersion() {
        return maxVersion;
    }
    public void setMaxVersion(int maxVersion) {
        this.maxVersion = maxVersion;
    }
    public PrimaryKey getPk() {
        return pk;
    }
    public void setPk(PrimaryKey pk) {
        this.pk = pk;
    }
}

class GetRangeJson {
    private String tableName;
    private int maxVersion;
    private Direction direction;
    private PrimaryKey begin;
    private PrimaryKey end;
    
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public int getMaxVersion() {
        return maxVersion;
    }
    public void setMaxVersion(int maxVersion) {
        this.maxVersion = maxVersion;
    }
    public Direction getDirection() {
        return direction;
    }
    public void setDirection(Direction direction) {
        this.direction = direction;
    }
    public PrimaryKey getBegin() {
        return begin;
    }
    public void setBegin(PrimaryKey begin) {
        this.begin = begin;
    }
    public PrimaryKey getEnd() {
        return end;
    }
    public void setEnd(PrimaryKey end) {
        this.end = end;
    }
}

public class OTSHelper {
    
    private SyncClientInterface ots = null;
    
    public OTSHelper(SyncClientInterface ots) {
        this.ots = ots;
    }
    
    public void createTable(TableJson tj) {
        TableMeta meta = new TableMeta(tj.getTableName());
        
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(tj.getMaxVersion());
        tableOptions.setTimeToLive(tj.getTimeToLive());
        
        CreateTableRequest createTableRequest = new CreateTableRequest(meta, tableOptions);
        ots.createTable(createTableRequest);
    }
    
    public void deleteTable(String tabkeName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest(tabkeName);
        ots.deleteTable(deleteTableRequest);
    }
    
    public void get(GetJson gj) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(gj.getTableName(), gj.getPk());
        rowQueryCriteria.setMaxVersions(gj.getMaxVersion());
        GetRowRequest getRowRequest = new GetRowRequest();
        getRowRequest.setRowQueryCriteria(rowQueryCriteria);
        Gson g = new Gson();
        System.out.println(g.toJson(ots.getRow(getRowRequest).getRow()));
    }
    
    public void getRange(GetRangeJson gj) {
        List<Row> results = new ArrayList<Row>();
        PrimaryKey token =  gj.getBegin();
        do {
            RangeRowQueryCriteria cur = new RangeRowQueryCriteria(gj.getTableName());
            cur.setDirection(Direction.FORWARD);
            cur.setInclusiveStartPrimaryKey(token);
            cur.setExclusiveEndPrimaryKey(gj.getEnd());
            cur.setMaxVersions(gj.getMaxVersion());
            
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(cur);
            
            GetRangeResponse result = ots.getRange(request);
            token = result.getNextStartPrimaryKey();
            results.addAll(result.getRows());
        } while (token != null);
        Gson g = new Gson();
        System.out.println(g.toJson(results));
    }
    
    public static void usage() {
        //System.err.println("./ots endpoint accessid accesskey instance_name create table.json");
        //System.err.println("./ots endpoint accessid accesskey instance_name delete table_name");
        //System.err.println("./ots endpoint accessid accesskey instance_name get param.json");
    }
    
    public static Configuration loadConf() {
        String path = "src/test/resources/conf.json";
        InputStream f;
        try {
            f = new FileInputStream(path);
            Configuration p = Configuration.from(f);
            return p;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    } 
    
    private static String getFileContent(String path) throws IOException{
        File filename = new File(path); 
        InputStreamReader reader = new InputStreamReader(new FileInputStream(filename)); 
        BufferedReader br = new BufferedReader(reader); 
        String line = null;  
        StringBuilder sb = new StringBuilder();
        while ((line = br.readLine()) != null) {  
            sb.append(line);
        }
        br.close();
        return sb.toString();
    }
    
    public static TableJson getTableJson(String path) throws IOException {
        String content = getFileContent(path);
        
        Gson g = new Gson();
        TableJson gj = g.fromJson(content, TableJson.class);
        //System.err.println(g.toJson(gj));
        return gj;
    }
    
    public static GetJson getGetJson(String path) throws IOException {
        String content = getFileContent(path);
        
        Gson g = new Gson();
        GetJson gj = g.fromJson(content, GetJson.class);
        //System.err.println(g.toJson(gj));
        return gj;
    }
    
    public static GetRangeJson getGetRangeJson(String path) throws IOException {
        String content = getFileContent(path);
        
        Gson g = new Gson();
        GetRangeJson gj = g.fromJson(content, GetRangeJson.class);
        //System.err.println(g.toJson(gj));
        return gj;
    }
    
    /**
     * ./ots endpoint accessid accesskey instance_name create table.json 
     * ./ots endpoint accessid accesskey instance_name delete table_name
     * ./ots endpoint accessid accesskey instance_name get param.json
     * ./ots endpoint accessid accesskey instance_name getrange param.json
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 6) {
            usage();
            System.exit(-1);
        }
        String endpoint = args[0];
        String accessid = args[1];
        String accesskey = args[2];
        String instance_name = args[3];
        String op = args[4];
        String param = args[5];
        SyncClientInterface oo = new SyncClient(endpoint, accessid, accesskey, instance_name);
        OTSHelper ots = new OTSHelper(oo);
        if (op.equals("create")) {
            TableJson tj = getTableJson(param);
            try {ots.createTable(tj);} finally {oo.shutdown();}
        } else if (op.equals("delete")) {
            
            try {ots.deleteTable(param);} finally {oo.shutdown();}
        } else if (op.equals("get")) {
            GetJson gj = getGetJson(param);
            try {ots.get(gj);} finally {oo.shutdown();}
        } else if (op.equals("getrange")) {
            GetRangeJson gj = getGetRangeJson(param);
            try {ots.getRange(gj);} finally {oo.shutdown();}
        } else {
            usage();
            System.exit(-1);
        }
    }
}
