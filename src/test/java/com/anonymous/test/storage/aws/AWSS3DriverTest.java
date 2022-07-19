package com.anonymous.test.storage.aws;

import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.Assert.*;

public class AWSS3DriverTest {

    @Test
    public void getObjectMetadata() {

        S3Client s3Client = S3Client.builder().region(Region.AP_EAST_1).build();
        AWSS3Driver.putObjectFromString(s3Client, "flush-test-1111-from-data", "testmeta", "metadatavalue");
        System.out.println(AWSS3Driver.getObjectSize(s3Client, "flush-test-1111-from-data", "testmeta"));


    }
}