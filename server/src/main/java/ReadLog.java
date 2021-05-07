import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.pusher.rest.Pusher;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ReadLog {

    private static String PRODUCT_TABLE_NAME = "request_info";

    public static void main(String[] args) throws IOException {
        final Map<String, Long> tableMap = new HashMap<String, Long>();

        Pusher pusher = new Pusher("1199661", "83bd9336fa76445546d6", "317308eca71d2d3fd37c");
        pusher.setCluster("ap1");
        pusher.setEncrypted(true);

        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "new_schema", "root", "password");

        client.registerEventListener(event -> {
            EventData data = event.getData();

            System.out.println(event);

            if(data instanceof TableMapEventData) {
                TableMapEventData tableData = (TableMapEventData)data;
                tableMap.put(tableData.getTable(), tableData.getTableId());
            } else if(data instanceof WriteRowsEventData) {
                WriteRowsEventData eventData = (WriteRowsEventData)data;
                if(eventData.getTableId() == tableMap.get(PRODUCT_TABLE_NAME)) {
                    for(Object[] product: eventData.getRows()) {
                        pusher.trigger(PRODUCT_TABLE_NAME, "insert", getProductMap(product));
                    }
                }
            } else if(data instanceof UpdateRowsEventData) {
                UpdateRowsEventData eventData = (UpdateRowsEventData)data;
                if(eventData.getTableId() == tableMap.get(PRODUCT_TABLE_NAME)) {
                    for(Map.Entry<Serializable[], Serializable[]> row : eventData.getRows()) {
                        pusher.trigger(PRODUCT_TABLE_NAME, "update", getProductMap(row.getValue()));
                    }
                }
            } else if(data instanceof DeleteRowsEventData) {
                DeleteRowsEventData eventData = (DeleteRowsEventData)data;
                if(eventData.getTableId() == tableMap.get(PRODUCT_TABLE_NAME)) {
                    for(Object[] product: eventData.getRows()) {
                        pusher.trigger(PRODUCT_TABLE_NAME, "delete", product[0]);
                    }
                }
            }
        });

        client.connect();

    }

    static Map<String, Object> getProductMap(Object[] product) {
        System.out.println("Get:" +  product.toString());
        Map<String, Object> map = new HashMap<>();
        map.put("id", product[0]);
        map.put("name", product[1]);
        map.put("price", product[2]);

        return map;
    }
}
