package com.aliyun.openservices.ots.internal.model;

import com.alicloud.openservices.tablestore.model.*;

import java.util.List;

public class MockGetRangeResult extends GetRangeResponse {
    
    private List<Row> rows = null;
    
    public MockGetRangeResult (List<Row> rows) {
        super(new Response("requestId"), new ConsumedCapacity(new CapacityUnit(0, 0)));
        this.rows = rows;
    }
    
    public GetRangeResponse toGetRangeResult() {
        GetRangeResponse result = new GetRangeResponse(new Response("requestId"), new ConsumedCapacity(new CapacityUnit(0, 0)));
        result.setRows(rows);
        return result;
    }
}
