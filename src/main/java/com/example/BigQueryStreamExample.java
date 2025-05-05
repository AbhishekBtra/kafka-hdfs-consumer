package com.example;

import com.google.cloud.bigquery.*;
import java.util.*;

public class BigQueryStreamExample {

    public static void sendRows(Map<String, String> rowContent) {
        // Initialize client
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        // Table identifier
        //String project="planar-effect-325211";
        String datasetName = "sample";
        String tableName = "hadoop_csv_test";


        TableId tableId = TableId.of(datasetName, tableName);

        // Convert row to InsertAllRequest.RowToInsert
        InsertAllRequest.RowToInsert row = InsertAllRequest.RowToInsert.of(rowContent);

        // Prepare insert request
        InsertAllRequest request = InsertAllRequest.newBuilder(tableId)
                .addRow(row)
                .build();

        // Execute the insert
        InsertAllResponse response = bigquery.insertAll(request);

        // Check for insert errors
        if (response.hasErrors()) {
            System.out.println("Insert errors occurred:");
            response.getInsertErrors().forEach((key, err) -> System.out.println(err));
        } else {
            System.out.println("Insert successful.");
        }
    }
}
