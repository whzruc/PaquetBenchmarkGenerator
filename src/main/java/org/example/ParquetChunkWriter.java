package org.example;



import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.schema.PrimitiveType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class ParquetChunkWriter implements Tool {
    private Configuration conf;
    private TrackingByteBufferAllocator allocator;


    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        // ClassLoader
        ClassLoader classLoader=ParquetChunkWriter.class.getClassLoader();
        URL benchmarkResource=classLoader.getResource("benchmark.json");
        URL propertiesResource=classLoader.getResource("config.properties");

        // read benchmark.json
        List<BenchmarkInfo> benchmarkInfos = readBenchmarkInfosFromJson(benchmarkResource.getPath());

        String targetSchemaName="test";
        BenchmarkInfo foundBenchmarkInfo = findBenchmarkInfoByName(benchmarkInfos, targetSchemaName);
        // read config.properties
        Properties properties = readProperties(propertiesResource.getPath());

        long rowGroupSize = Long.parseLong(properties.getProperty("RowGroupSize"));
        String outputPath = properties.getProperty("OutPutPath");
        CompressionCodecName compressionCodecName = CompressionCodecName.fromConf(
                properties.getProperty("Compression"));
        String encoding = properties.getProperty("Encoding");

        if(foundBenchmarkInfo!=null) {
            BenchmarkInfo benchmarkInfo=foundBenchmarkInfo;
            String benchmarkName = benchmarkInfo.getBenchmarkName();
            String sourcePath = benchmarkInfo.getBenchamrkSourcePaths();
            Map<String, List<String>> schema = benchmarkInfo.getSchema();
            Map<String, Integer> numRows = benchmarkInfo.getNumRows();

            // allocator
            allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());

            for (String tableName : schema.keySet()) {
                System.out.println("parse tableName:" + tableName);
                // *.csv *.tbl *.tsv
                String inputFilePath = getInputFilePath(sourcePath, tableName);
                long startTime = System.currentTimeMillis();
                List<String> columnNames=benchmarkInfo.getColumnNames().get(tableName);
                List<String> columnTyps=benchmarkInfo.getSchema().get(tableName);
                Integer maxNumRows=benchmarkInfo.getNumRows().get(tableName);

                int counts = 0;
                int totalCounts=0;
                int number = 0;
                String[] columnNamesString = new String[columnNames.size()];
                String messageTypeString="message "+tableName+" {";
                for (int i = 0; i < columnNames.size(); i++) {
                    columnNamesString[i] = columnNames.get(i);
                    /**
                     * Notice: the types is Trino-SQL(For example)
                     * We should convert them to Parquet Logical Types
                     */
                    messageTypeString+="optional "+ParquetLocgicalTypes(columnTyps.get(i),columnNames.get(i))+"; ";
                }
                messageTypeString+="}";
//                System.out.println(messageTypeString);
//                System.out.println(Arrays.toString(columnNamesString));
//                System.out.println(benchmarkInfo.getDelimiter());
                System.out.println(inputFilePath);
                MessageType schemaMessage=parseMessageType(messageTypeString);


                try (
                FileInputStream fis = new FileInputStream(inputFilePath);
                InputStreamReader isr = new InputStreamReader(fis);
                BufferedReader reader = new BufferedReader(isr)) {
                    boolean initParquetWriter=true;
                    GroupWriteSupport.setSchema(schemaMessage,conf);
                    SimpleGroupFactory f = new SimpleGroupFactory(schemaMessage);

                    Group group=f.newGroup();
                    ParquetWriter<Group> writer = null;
                    String line;
                    while((line=reader.readLine())!=null){
                        String[] str=line.split(benchmarkInfo.getDelimiter());
                        counts++;
                        totalCounts++;
                        for(int i=0;i<columnNames.size();i++){
                            convertValue(group,columnNames.get(i),columnTyps.get(i),str[i]);
                        }

                        if(initParquetWriter||counts==maxNumRows){
                            // build ParquetWriter
                            Path outputFilePath= Paths.get(benchmarkInfo.getOutputPath()+"/"+tableName+"/"+tableName+"_"+number+".parquet");
                            if(Files.exists(outputFilePath)){
                                Files.delete(outputFilePath);
                            }
                            if(!Files.exists(Paths.get(benchmarkInfo.getOutputPath()))){
                                Files.createDirectories(Paths.get(benchmarkInfo.getOutputPath()));
                            }
                            // Create Parent Path
                            Path parentPath = Paths.get(benchmarkInfo.getOutputPath()+"/"+tableName);
                            if (!Files.exists(parentPath)) {
                                Files.createDirectories(parentPath);
                            }
                            number++;
                            counts=0;
                            initParquetWriter=false;
                            System.out.println(outputFilePath);
                            if(writer!=null){
                                writer.close();
                            }
                            writer=ExampleParquetWriter.builder(new LocalOutputFile(outputFilePath))
                                    .withAllocator(allocator)
                                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                                    .withDictionaryEncoding(false)
                                    .withRowGroupSize(256*1024*1024)
                                    .withPageSize(1024)
                                    .withValidation(false)
                                    .withWriterVersion(WriterVersion.PARQUET_2_0)
                                    .withConf(conf)
                                    .build();
                            writer.write(group);
                            group=f.newGroup();
                        }
                        else{
                            writer.write(group);
                            group=f.newGroup();
                        }
                    }
                    writer.close();
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                System.out.println("Finish convert table " + tableName + " parse counts: " + totalCounts);
                long endTime = System.currentTimeMillis();
                long elapsedTime = endTime - startTime;
                System.out.println("Execution time of the program is " + elapsedTime);
            }

            allocator.close();
        }
        return 0;
    }

    private String ParquetLocgicalTypes(String columnType, String columnName) {
        ColumnInfo columnInfo=parseColumnType(columnType);

        switch (columnInfo.columnType){
            case "boolean":
                return "boolean "+columnName;
            case "decimal":
                return "binary "+columnName+" (DECIMAL("+columnInfo.param1+","+columnInfo.param2+"))";
//            case "char":
//            case "varchar":
//                return "fixed_len_byte_array("+columnInfo.param1+") "+columnName;
            case "integer":
            case "int32":
                return "int32 "+columnName;
            case "bigint":
            case "int64":
                return "int64 "+columnName;
            case "float":
                return "float "+columnName;
            case "double":
                return "double "+columnName;
            case "char":
            case "varchar":
            case "string":
                return "binary "+columnName+" (STRING)";
            case "timestamp":
                return "int64 "+columnName+" (TIMESTAMP_MILLIS)";
            case "date":
                return "int32 "+columnName+" (DATE)";
            default:
                return columnType;
        }
    }

    private void convertValue(Group group,String columnName,String columnType, String valueStr) {
        ColumnInfo columnInfo=parseColumnType(columnType);
//        System.out.println(columnInfo.toString());
        switch (columnInfo.columnType) {
            case "date":
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    Date date = sdf.parse(valueStr);
                    long timestamp = date.getTime();
                    // data truncation may occur
                    int intValue = (int) (timestamp & 0xFFFFFFFFL);
//                    System.out.println("convert int: " + intValue);
                    group.append(columnName,intValue);
                } catch (java.text.ParseException e) {
                    e.printStackTrace();
                }
                break;
            case "int32":
            case "integer":
                group.append(columnName, Integer.parseInt(valueStr));
                break;
            case "timestamp":
                SimpleDateFormat sdf_timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try{
                    Date date=sdf_timestamp.parse(valueStr);
                    long timestamp=date.getTime();
                    group.append(columnName,timestamp);
                } catch (java.text.ParseException e) {
                    e.printStackTrace();
                }
                break;
            case "int64":
            case "bigint":
                group.append(columnName, Long.parseLong(valueStr));
                break;
            case "float":
                group.append(columnName,Float.parseFloat(valueStr));
                break;
            case "double":
                group.append(columnName,Double.parseDouble(valueStr));
                break;
            case "boolean":
                group.append(columnName,Boolean.parseBoolean(valueStr));
                break;
            case "binary":
            case "decimal":
                group.append(columnName, Binary.fromString(valueStr));
                break;
            case "fixed_len_byte_array":
            case "char":
            case "varchar":
                group.append(columnName,valueStr);
                break;
            default:
                group.append(columnName,valueStr);
        }
    }
    private BenchmarkInfo findBenchmarkInfoByName(List<BenchmarkInfo> benchmarkInfos, String targetName) {
        for (BenchmarkInfo benchmarkInfo : benchmarkInfos) {
            if (benchmarkInfo.getBenchmarkName().equals(targetName)) {
                return benchmarkInfo;
            }
        }
        return null;
    }

    private List<BenchmarkInfo> readBenchmarkInfosFromJson(String jsonFilePath) throws IOException {
        String jsonContent = Files.lines(Paths.get(jsonFilePath)).collect(Collectors.joining());
        JSONArray jsonArray = new JSONArray(jsonContent);
        List<BenchmarkInfo> benchmarkInfos = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String benchmarkName = jsonObject.getString("benchmarkName");
            String benchamrkSourcePaths = jsonObject.getString("benchamrkSourcePaths");
            String outputPath=jsonObject.getString("outputPath");
            String delimiter=jsonObject.getString("delimiter");

            // schema types
            JSONObject schemaJson = jsonObject.getJSONObject("schema");
            Map<String, List<String>> schema = new HashMap<>();
            for (String key : schemaJson.keySet()) {
                schema.put(key, Arrays.asList(schemaJson.getJSONArray(key).toList().toArray(new String[0])));
            }
            // columnNames
            JSONObject columnNamesJson =jsonObject.getJSONObject("columnNames");
            Map<String,List<String>> columnNames=new HashMap<>();
            for(String key: columnNamesJson.keySet()){
                columnNames.put(key,Arrays.asList(columnNamesJson.getJSONArray(key).toList().toArray(new String[0])));
            }
            JSONObject numRowsJson = jsonObject.getJSONObject("numRows");
            Map<String, Integer> numRows = new HashMap<>();
            for (String key : numRowsJson.keySet()) {
                numRows.put(key, numRowsJson.getInt(key));
            }
            benchmarkInfos.add(new BenchmarkInfo(benchmarkName, benchamrkSourcePaths, schema,columnNames, numRows,delimiter,outputPath));
        }
        return benchmarkInfos;
    }

    private Properties readProperties(String propertiesFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(propertiesFilePath));
        return properties;
    }
    public static String getInputFilePath(String sourcePath, String tableName) {
        String csvFilePath = sourcePath + File.separator + tableName + File.separator + tableName + ".csv";
        String tblFilePath = sourcePath + File.separator + tableName + File.separator + tableName + ".tbl";
        String tsvFilePath = sourcePath + File.separator + tableName + File.separator + tableName + ".tsv";

        int existingFileCount = 0;
        String inputFilePath = null;

        if (Files.exists(Paths.get(csvFilePath))) {
            inputFilePath = csvFilePath;
            existingFileCount++;
        }
        if (Files.exists(Paths.get(tblFilePath))) {
            inputFilePath = tblFilePath;
            existingFileCount++;
        }
        if (Files.exists(Paths.get(tsvFilePath))) {
            inputFilePath = tsvFilePath;
            existingFileCount++;
        }

        if (existingFileCount > 1) {
            throw new IllegalStateException("Not allow " + tableName + " CSV、TBL、TSV files exist at the same time");
        }

        if (inputFilePath == null) {
            throw new IllegalArgumentException("In" + sourcePath + "/" + tableName + "all available files(*.csv,*.tbl,*.tsv) don't exist");
        }

        return inputFilePath;
    }

    static class ColumnInfo {
        String columnType;
        String param1;
        String param2;

        public ColumnInfo(String columnType, String param1, String param2) {
            this.columnType = columnType;
            this.param1 = param1;
            this.param2 = param2;
        }

        @Override
        public String toString() {
            return "ColumnInfo{" +
                    "columnType='" + columnType + '\'' +
                    ", param1='" + param1 + '\'' +
                    ", param2='" + param2 + '\'' +
                    '}';
        }
    }
    private  ColumnInfo parseColumnType(String columnType) {
        String columnTypeName = columnType;
        String param1 = "";
        String param2 = "";

        Pattern pattern = Pattern.compile("\\((.*?)\\)");
        Matcher matcher = pattern.matcher(columnType);
        if (matcher.find()) {
            String[] params = matcher.group(1).split(",");
            if (params.length > 0) {
                param1 = params[0].trim();
            }
            if (params.length > 1) {
                param2 = params[1].trim();
            }
            columnTypeName = columnType.substring(0, columnType.indexOf('(')).trim();
        }

        return new ColumnInfo(columnTypeName, param1, param2);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ParquetChunkWriter writer = new ParquetChunkWriter();
        writer.setConf(conf);
        int result = ToolRunner.run(conf, writer, args);
        System.exit(result);
    }
}