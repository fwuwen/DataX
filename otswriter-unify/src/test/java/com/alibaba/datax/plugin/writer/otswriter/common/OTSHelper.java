package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.TablePrimaryKeySchema;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;

public class OTSHelper {
    
    /**
     * 创建表。如果表已经存在，则删除原来的表，在重新新建
     * @param ots
     * @param meta
     * @throws Exception
     */
    public static void createTableSafe(SyncClientInterface ots, TableMeta meta) throws Exception {
        {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(meta.getTableName());
            try {
                ots.deleteTable(deleteTableRequest);
            } catch (TableStoreException e) {
                if (!ErrorCode.OBJECT_NOT_EXIST.equals(e.getErrorCode())) {
                    throw e;
                }
            }
        }
        {
            TableOptions tableOptions = new TableOptions();
            tableOptions.setMaxVersions(Integer.MAX_VALUE);
            tableOptions.setTimeToLive(-1);
            
            CreateTableRequest createTableRequest = new CreateTableRequest(meta, tableOptions);
            createTableRequest.setTableOptions(tableOptions);
            ots.createTable(createTableRequest);
        }
        Thread.sleep(5 * 1000);
    }
    
    public static void createTableSafe(SyncClientInterface ots, String tableName, Map<String, PrimaryKeyType> pk) throws Exception {
        TableMeta meta = new TableMeta(tableName);
        for (Entry<String, PrimaryKeyType> s : pk.entrySet()) {
            meta.addPrimaryKeyColumn(s.getKey(), s.getValue());
        }
        createTableSafe(ots, meta);
    }
    
    public static void createTableSafe(SyncClientInterface ots, String tableName, Map<String, PrimaryKeyType> pk, int readeCU, int writeCU) throws Exception {
        throw new RuntimeException("Unimplements");
    }
    
    public static void prepareData(
            SyncClientInterface ots,
            String tableName, 
            Map<String, PrimaryKeyType> pk,
            Map<String, ColumnType> attr, 
            long begin, 
            long rowCount, 
            double nullPercent) throws Exception {
        throw new RuntimeException("Unimplements");
    }
    
    public static List<Row> getAllData(SyncClientInterface ots, OTSConf conf) throws ClientException, TableStoreException {
        List<Row> results = new ArrayList<Row>();
        List<PrimaryKeyColumn> begin  = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end  = new ArrayList<PrimaryKeyColumn>();
        
        List<String> cc = new ArrayList<String>();
        
        for (TablePrimaryKeySchema col : conf.getPrimaryKeyColumn()) {
            begin.add(new PrimaryKeyColumn(col.getName(), PrimaryKeyValue.INF_MIN));
            end.add(new PrimaryKeyColumn(col.getName(), PrimaryKeyValue.INF_MAX));
        }
        for (OTSAttrColumn col : conf.getAttributeColumn()) {
            cc.add(col.getName());
        }
        
        PrimaryKey token =  new PrimaryKey(begin);
        do {
            RangeRowQueryCriteria cur = new RangeRowQueryCriteria(conf.getTableName());
            cur.setDirection(Direction.FORWARD);
            cur.addColumnsToGet(cc);
            cur.setInclusiveStartPrimaryKey(token);
            cur.setExclusiveEndPrimaryKey(new PrimaryKey(end));
            cur.setMaxVersions(Integer.MAX_VALUE);
            
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(cur);
            
            GetRangeResponse result = ots.getRange(request);
            token = result.getNextStartPrimaryKey();
            results.addAll(result.getRows());
        } while (token != null);
        return results;
    }
}
