package com.example;

import java.util.Random;
//import java.util.random.RandomGeneratorFactory;
import java.nio.charset.StandardCharsets;

import com.google.cloud.storage.*;

public class StreamToGCS {
    
    public static void sendRowsToGcs(String records) throws StorageException{

        String bucketName = "buck81";
        Random rn = new Random();
        String blobName = "metadata_"+rn.nextInt();


        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobInfo bI = BlobInfo.newBuilder(bucketName,blobName).setContentType("text-plain").build();
        BlobId bId = BlobId.of(bucketName, blobName);
        storage.create(bI,records.getBytes(StandardCharsets.UTF_8));
    }
}
