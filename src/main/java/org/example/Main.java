package org.example;


import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import java.io.File;
import java.io.IOException;
import org.apache.parquet.schema.MessageTypeParser;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class Main {

    private static TrackingByteBufferAllocator allocator;
//    @Before
    private static void initAllocator() {
        allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
    }

//    @After
    public static void closeAllocator() {
        allocator.close();
    }

//    @Test
    public static void test() throws Exception{
        Configuration conf = new Configuration();
        Path targetPath=Paths.get("./test.parquet");
        if(Files.exists(targetPath)){
            Files.delete(targetPath);
        }
        MessageType schema=parseMessageType("message test{ "
                +" required int32 test_field;"
                +"} ");
        GroupWriteSupport.setSchema(schema,conf);
        SimpleGroupFactory f=new SimpleGroupFactory(schema);
        ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(targetPath))
            .withAllocator(allocator)
            .withCompressionCodec(UNCOMPRESSED)
            .withRowGroupSize(268435456)
            .withPageSize(1024)
            .withValidation(false)
            .withWriterVersion(WriterVersion.PARQUET_1_0)
            .withConf(conf)
            .build();

        for(int i=0;i<1000;i++){
            writer.write(f.newGroup().append("test_field",i));
        }

        writer.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");
        initAllocator();
        test();
        closeAllocator();
    }
}