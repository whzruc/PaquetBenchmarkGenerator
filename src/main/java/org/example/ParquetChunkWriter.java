package org.example;



import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class ParquetChunkWriter implements Tool {
    private Configuration conf;


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


        // 读取benchmark.json文件获取benchmark信息
        List<BenchmarkInfo> benchmarkInfos = readBenchmarkInfosFromJson(benchmarkResource.getPath());

        String targetSchemaName="test";
        BenchmarkInfo foundBenchmarkInfo = findBenchmarkInfoByName(benchmarkInfos, targetSchemaName);
//        Optional<BenchmarkInfo> foundBenchmarkInfoOptional = benchmarkInfos.stream()
//                .filter(benchmarkInfo -> benchmarkInfo.getBenchmarkName().equals(targetSchemaName))
//                .findFirst();
//        if (foundBenchmarkInfoOptional.isEmpty()) {
//            System.out.println("未找到指定benchmarkName的BenchmarkInfo");
//        }

        // 读取config.properties文件获取写入参数
        Properties properties = readProperties(propertiesResource.getPath());

        long rowGroupSize = Long.parseLong(properties.getProperty("RowGroupSize"));
        String outputPath = properties.getProperty("OutPutPath");
        CompressionCodecName compressionCodecName = CompressionCodecName.fromConf(
                properties.getProperty("Compression"));
        String encoding = properties.getProperty("Encoding");


//        foundBenchmarkInfoOptional.ifPresent(benchmarkInfo -> {
        if(foundBenchmarkInfo!=null) {
            BenchmarkInfo benchmarkInfo=foundBenchmarkInfo;
            String benchmarkName = benchmarkInfo.getBenchmarkName();
            String sourcePath = benchmarkInfo.getBenchamrkSourcePaths();
            Map<String, List<String>> schema = benchmarkInfo.getSchema();
            Map<String, Integer> numRows = benchmarkInfo.getNumRows();
            for (String tableName : schema.keySet()) {
                System.out.println("parse tableName:" + tableName);
                // *.csv *.tbl *.tsv
                String inputFilePath = getInputFilePath(sourcePath, tableName);
                long startTime = System.currentTimeMillis();
                List<String> columnNames;

                int counts = 0;
                columnNames = benchmarkInfo.getColumnNames().get(tableName);
                String[] columnNamesString = new String[columnNames.size()];
                for (int i = 0; i < columnNames.size(); i++) {
                    columnNamesString[i] = columnNames.get(i);
                }
                System.out.println(Arrays.toString(columnNamesString));
                System.out.println(benchmarkInfo.getDelimiter());
                System.out.println(inputFilePath);
                try (Reader reader = new FileReader(inputFilePath)) {
                    Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                            .setHeader(columnNamesString)
                            // use different delimiter
                            .setDelimiter(benchmarkInfo.getDelimiter())
                            .build().parse(reader);

                    for (CSVRecord record : records) {
                        counts++;
                    }

                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                System.out.println("Finish convert table " + tableName + " parse counts: " + counts);
                long endTime = System.currentTimeMillis();
                long elapsedTime = endTime - startTime;
                System.out.println("Execution time of the program is " + elapsedTime);
            }
        }
//        });


//            String benchmarkName = benchmarkInfo.getBenchmarkName();
//            String sourcePath = benchmarkInfo.getBenchamrkSourcePaths();
//            Map<String, List<String>> schema = benchmarkInfo.getSchema();
//            Map<String, Integer> numRows = benchmarkInfo.getNumRows();
//
//            for (String tableName : schema.keySet()) {
//                // *.csv *.tbl *.tsv
//                String inputFilePath=getInputFilePath(sourcePath,tableName);
//                long startTime=System.currentTimeMillis();
//
//                columnNames=benchmarkInfo.getColumnNames();
//                try(Reader reader=new FileReader(inputFilePath)){
//                    Iterable<CSVRecord> records=CSVFormat.DEFAULT.builder()
//                            .setHeader()
//                            // use different delimiter
//                            .setDelimiter(benchmarkInfo.getDelimiter())
//                            .build().parse(reader);
//                    for(CSVRecord record:records){
//
//                    }
//
//                }
//                System.out.println("Finish convert table "+tableName);
//                long endTime=System.currentTimeMillis();
//                long elapsedTime=endTime-startTime;
//                System.out.println("Execution time of the program is "+elapsedTime);
//
//                // 解析CSV文件头部获取列名（假设CSV文件有头部）
//                CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader();
//                try (CSVParser csvParser = CSVParser.parse(String.valueOf(new File(inputFilePath)), csvFormat)) {
//                    List<String> headerList = csvParser.getHeaderNames();
//
//                    // 构建Parquet文件的模式（schema）信息
//                    MessageType.Builder messageTypeBuilder = MessageTypeParser.parseMessageType("message " + tableName + " {")
//                            .toBuilder();
//                    for (String header : headerList) {
//                        // 简单示例，都当作字符串类型处理，实际需完善类型映射
//                        messageTypeBuilder.addField(header, org.apache.parquet.schema.Types.primitive("UTF8"));
//                    }
//                    MessageType messageType = messageTypeBuilder.named(tableName);
//                    GroupWriteSupport.setSchema(messageType, conf);
//
//                    // 构建Parquet文件输出路径（根据需求调整命名等）
//                    String parquetFilePath = outputPath + File.separator + benchmarkName + "_" + tableName + ".parquet";
//
//                    // 创建ParquetWriter
//                    ExampleParquetWriter.Builder<Group> builder = ExampleParquetWriter.builder(new LocalOutputFile(new Path(parquetFilePath)))
//                            .withRowGroupSize(rowGroupSize)
//                            .withCompressionCodec(compressionCodecName)
//                            .withWriterVersion(WriterVersion.PARQUET_1_0)
//                            .withConf(conf)
//                            .withParquetProperties(new ParquetProperties(encoding));
//
//                    // 分块读取和写入
//                    int rowIndex = 0;
//                    int blockIndex = 0;
//                    int fileIndex = 0;
//                    try (ExampleParquetWriter<Group> writer = builder.build()) {
//                        for (CSVRecord record : csvParser) {
//                            if (rowIndex % numRows.get(tableName) == 0 && rowIndex > 0) {
//                                blockIndex++;
//                            }
//                            if (rowIndex % (numRows.get(tableName) * rowGroupSize) == 0 && rowIndex > 0) {
//                                fileIndex++;
//                                blockIndex = 0;
//                                writer.close();
//                                // 重新构建ParquetWriter，创建新文件用于写入
//                                builder = ExampleParquetWriter.builder(new LocalOutputFile(new Path(parquetFilePath + "." + fileIndex)))
//                                        .withRowGroupSize(rowGroupSize)
//                                        .withCompressionCodec(compressionCodecName)
//                                        .withWriterVersion(WriterVersion.PARQUET_1_0)
//                                        .withConf(conf)
//                                        .withParquetProperties(new ParquetProperties(encoding));
//                                writer = builder.build();
//                            }
//                            // 构建写入Parquet的记录（需根据实际完善）
//                            Group group = createGroupFromRecord(record, headerList, rowIndex, blockIndex, fileIndex);
//                            writer.write(group);
//                            rowIndex++;
//                        }
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }


        return 0;
    }
    private BenchmarkInfo findBenchmarkInfoByName(List<BenchmarkInfo> benchmarkInfos, String targetName) {
        for (BenchmarkInfo benchmarkInfo : benchmarkInfos) {
            if (benchmarkInfo.getBenchmarkName().equals(targetName)) {
                return benchmarkInfo;
            }
        }
        return null; // 如果没找到则返回null
    }
    private Group createGroupFromRecord(CSVRecord record, List<String> headerList, int rowIndex, int blockIndex, int fileIndex) {
        // 这里要根据实际Parquet写入数据结构和逻辑完善
        // 简单示例，创建一个Group对象并填充数据，假设使用相关Parquet写入API
        // 此处代码需替换为符合你实际情况的逻辑
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

            benchmarkInfos.add(new BenchmarkInfo(benchmarkName, benchamrkSourcePaths, schema,columnNames, numRows,delimiter));
        }
        return benchmarkInfos;
    }

    private Properties readProperties(String propertiesFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(propertiesFilePath));
        return properties;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ParquetChunkWriter writer = new ParquetChunkWriter();
        writer.setConf(conf);
        int result = ToolRunner.run(conf, writer, args);
        System.exit(result);
    }
}