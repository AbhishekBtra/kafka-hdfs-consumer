package com.example;

import java.util.Random;
//import java.util.random.RandomGeneratorFactory;
import java.nio.charset.StandardCharsets;

import com.google.cloud.storage.*;

public class StreamToGCS {
    
    //function to send records to GCS bucket
    public static void sendRowsToGcs(String records) throws StorageException{

        String bucketName = "buck81";
        Random rn = new Random();
        String blobName = "metadata_"+rn.nextInt();

        //get storage
        Storage storage = StorageOptions.getDefaultInstance().getService();
        //prepare blob
        BlobInfo bI = BlobInfo.newBuilder(bucketName,blobName).setContentType("text-plain").build();
        //BlobId bId = BlobId.of(bucketName, blobName);
        //create blob
        storage.create(bI,records.getBytes(StandardCharsets.UTF_8));
    }
}
