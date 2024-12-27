package org.example;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public class MultiThreadFileProcessing {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadFileProcessing.class);
    private Map<String, List<String>> benchmarkInfoColumnNames;
    private Map<String, List<String>> benchmarkInfoSchema;
    private Map<String, Integer> benchmarkInfoNumRows;
    private List<String> inputFilePaths;
    private String benchmarkOutputPath;
    private String delimiter;
    private Configuration conf;

    private Properties properties;
    private Map<String, AtomicInteger> number = new HashMap<>();
    private ReentrantLock lock = new ReentrantLock();

    public MultiThreadFileProcessing(Map<String, List<String>> benchmarkInfoColumnNames,
                                     Map<String, List<String>> benchmarkInfoSchema,
                                     Map<String, Integer> benchmarkInfoNumRows,
                                     List<String> inputFilePaths,
                                     String outputPath,
                                     String delimiter,
                                     Configuration conf,
                                     Properties props
    ) {
        this.benchmarkInfoColumnNames = benchmarkInfoColumnNames;
        this.benchmarkInfoSchema = benchmarkInfoSchema;
        this.benchmarkInfoNumRows = benchmarkInfoNumRows;
        this.inputFilePaths = inputFilePaths;
        this.benchmarkOutputPath = outputPath;
        this.delimiter = delimiter;
        this.conf = conf;
        this.properties = props;
        for (String columnName : benchmarkInfoSchema.keySet()) {
            number.put(columnName, new AtomicInteger(0));
        }


    }

    private class FileReadAndProcessTask implements Callable<Void> {
        private boolean initParquetWriter;
        private String inputFilePath;
        private ThreadLocal<Group> threadLocalGroup;
        private ThreadLocal<ParquetWriter<Group>> threadLocalParquetWriter;
        private TrackingByteBufferAllocator allocator;
        private int counts = 0;
        private int totalCounts = 0;

        private void convertValue(Group group, String columnName, String columnType, String valueStr) {
            ParquetChunkWriter.ColumnInfo columnInfo = parseColumnType(columnType);
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
                        group.append(columnName, intValue);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    break;
                case "int32":
                case "integer":
                    group.append(columnName, Integer.parseInt(valueStr));
                    break;
                case "timestamp":
                    SimpleDateFormat sdf_timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    try {
                        Date date = sdf_timestamp.parse(valueStr);
                        long timestamp = date.getTime();
                        group.append(columnName, timestamp);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    break;
                case "int64":
                case "bigint":
                    group.append(columnName, Long.parseLong(valueStr));
                    break;
                case "float":
                    group.append(columnName, Float.parseFloat(valueStr));
                    break;
                case "double":
                    group.append(columnName, Double.parseDouble(valueStr));
                    break;
                case "boolean":
                    group.append(columnName, Boolean.parseBoolean(valueStr));
                    break;
                case "binary":
                case "decimal":
                    group.append(columnName, Binary.fromString(valueStr));
                    break;
                case "fixed_len_byte_array":
                case "char":
                case "varchar":
                    group.append(columnName, valueStr);
                    break;
                default:
                    group.append(columnName, valueStr);
            }
        }

        public FileReadAndProcessTask(String inputFilePath) {
            this.inputFilePath = inputFilePath;
            this.threadLocalGroup = new ThreadLocal<>();
            this.threadLocalParquetWriter = new ThreadLocal<>();
            this.threadLocalParquetWriter.set(null);
            this.initParquetWriter = true;
        }

        private String ParquetLocgicalTypes(String columnType, String columnName) {
            ParquetChunkWriter.ColumnInfo columnInfo = parseColumnType(columnType);

            switch (columnInfo.columnType) {
                case "boolean":
                    return "boolean " + columnName;
                case "decimal":
                    return "binary " + columnName + " (DECIMAL(" + columnInfo.param1 + "," + columnInfo.param2 + "))";
                case "integer":
                case "int32":
                    return "int32 " + columnName;
                case "bigint":
                case "int64":
                    return "int64 " + columnName;
                case "float":
                    return "float " + columnName;
                case "double":
                    return "double " + columnName;
                case "char":
                case "varchar":
                case "string":
                    return "binary " + columnName + " (STRING)";
                case "timestamp":
                    return "int64 " + columnName + " (TIMESTAMP_MILLIS)";
                case "date":
                    return "int32 " + columnName + " (DATE)";
                default:
                    return columnType;
            }
        }

        private ParquetChunkWriter.ColumnInfo parseColumnType(String columnType) {
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

            return new ParquetChunkWriter.ColumnInfo(columnTypeName, param1, param2);
        }

        @Override
        public Void call() throws Exception {
            String tableName = getTableNameFromPath(inputFilePath);
            List<String> columnNames = benchmarkInfoColumnNames.get(tableName);
            List<String> columnTyps = benchmarkInfoSchema.get(tableName);
            Integer maxNumRows = benchmarkInfoNumRows.get(tableName);
            long rowGroupSize = Long.parseLong(properties.getProperty("RowGroupSize"));
            String outputPath = benchmarkOutputPath;
            CompressionCodecName compressionCodecName = CompressionCodecName.fromConf(
                    properties.getProperty("Compression"));
            String[] columnNamesString = new String[columnNames.size()];
            String messageTypeString = "message " + tableName + " {";
            for (int i = 0; i < columnNames.size(); i++) {
                columnNamesString[i] = columnNames.get(i);
                messageTypeString += "optional " + ParquetLocgicalTypes(columnTyps.get(i), columnNames.get(i)) + "; ";
            }
            messageTypeString += "}";
//                System.out.println(messageTypeString);
//                System.out.println(Arrays.toString(columnNamesString));
//                System.out.println(benchmarkInfo.getDelimiter());
//            System.out.println(inputFilePath);
            MessageType schemaMessage = parseMessageType(messageTypeString);
            initThreadLocalGroup(schemaMessage);

            try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFilePath))) {

                GroupWriteSupport.setSchema(schemaMessage, conf);
                SimpleGroupFactory f = new SimpleGroupFactory(schemaMessage);
                Group group = f.newGroup();
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] str = line.split(delimiter);
                    counts++;
                    totalCounts++;
                    // build ParquetWriter
                    for (int i = 0; i < columnNames.size(); i++) {
                        convertValue(threadLocalGroup.get(), columnNames.get(i), columnTyps.get(i), str[i]);
                    }
                    if (initParquetWriter || counts == maxNumRows) {
                        Path outputFilePath = null;
                        lock.lock();
                        outputFilePath = Paths.get(outputPath + "/" + tableName + "/" + tableName + "_" + number.get(tableName) + ".parquet");
                        number.get(tableName).incrementAndGet();
                        lock.unlock();
                        if (Files.exists(outputFilePath)) {
                            Files.delete(outputFilePath);
                        }
                        if (!Files.exists(Paths.get(outputPath))) {
                            Files.createDirectories(Paths.get(outputPath));
                        }
                        // Create Parent Path
                        Path parentPath = Paths.get(outputPath + "/" + tableName);
                        if (!Files.exists(parentPath)) {
                            Files.createDirectories(parentPath);
                        }
                        counts = 0;
                        initParquetWriter = false;
                        System.out.println(outputFilePath);
                        if (threadLocalParquetWriter.get() != null) {
                            threadLocalParquetWriter.get().close();

                        }
                        threadLocalParquetWriter.set(ExampleParquetWriter.builder(new LocalOutputFile(outputFilePath))
                                .withCompressionCodec(compressionCodecName)
                                .withDictionaryEncoding(false)
                                .withRowGroupSize(rowGroupSize)
                                .withPageSize(1024)
                                .withValidation(false)
                                .withWriterVersion(WriterVersion.PARQUET_1_0)
                                .withType(schemaMessage)
                                .withMaxPaddingSize(1024)
                                .withConf(conf)
                                .build());

//                                System.out.println("\n" + Thread.currentThread().getName() + "\n " + outputFilePath + "\n" + threadLocalGroup.get().toString());
                        threadLocalParquetWriter.get().write(threadLocalGroup.get());

                        initThreadLocalGroup(schemaMessage);
//                                writer.write(group);
//                                group = f.newGroup();


                    } else {
//                            System.out.println("\n" + Thread.currentThread().getName() + "\n " + threadLocalGroup.get().toString());
                        threadLocalParquetWriter.get().write(threadLocalGroup.get());
                        initThreadLocalGroup(schemaMessage);
//                            writer.write(group);
//                            group = f.newGroup();
                    }
                }
                threadLocalParquetWriter.get().close();
            } catch (IOException e) {
//                System.out.println(Thread.currentThread().getName());
                LOGGER.error(Thread.currentThread().getName(), e);
                LOGGER.error("Error processing file: {}", inputFilePath, e);
                throw new RuntimeException(e);
            } catch (RuntimeException e) {
//                System.out.println(Thread.currentThread().getName());
                LOGGER.error(Thread.currentThread().getName(), e);
                LOGGER.error("Error processing file: {}", inputFilePath, e);
                throw new RuntimeException(e);
            }

            System.out.println("Finish convert table " + tableName + " parse counts: " + totalCounts);
            return null;
        }

        private String getTableNameFromPath(String inputFilePath) {
            // spilt-gen files is tableName-format-xx e.g. hits-tsv-aa
            int lastIndex = inputFilePath.lastIndexOf('/');
            int lastIndexDot = inputFilePath.lastIndexOf('-');
            return inputFilePath.substring(lastIndex + 1, lastIndexDot - 4);
        }

        private void initThreadLocalGroup(MessageType schemaMessage) {
            try {
                GroupWriteSupport.setSchema(schemaMessage, conf);
                SimpleGroupFactory f = new SimpleGroupFactory(schemaMessage);
                threadLocalGroup.set(f.newGroup());
            } catch (Exception e) {
                LOGGER.error("Error initializing ThreadLocal Group", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void processFiles() {
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//        ExecutorService executorService = Executors.newFixedThreadPool(2);
        List<Future<Void>> futures = new ArrayList<>();

        for (String inputFilePath : inputFilePaths) {
            FileReadAndProcessTask task = new FileReadAndProcessTask(inputFilePath);
            futures.add(executorService.submit(task));
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Error waiting for task to complete", e);
            }
        }

        executorService.shutdown();
    }

    private static BenchmarkInfo findBenchmarkInfoByName(List<BenchmarkInfo> benchmarkInfos, String targetName) {
        for (BenchmarkInfo benchmarkInfo : benchmarkInfos) {
            if (benchmarkInfo.getBenchmarkName().equals(targetName)) {
                return benchmarkInfo;
            }
        }
        return null;
    }

    private static List<BenchmarkInfo> readBenchmarkInfosFromJson(String jsonFilePath) throws IOException {
        String jsonContent = Files.lines(Paths.get(jsonFilePath)).collect(Collectors.joining());
        JSONArray jsonArray = new JSONArray(jsonContent);
        List<BenchmarkInfo> benchmarkInfos = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String benchmarkName = jsonObject.getString("benchmarkName");
            String benchamrkSourcePaths = jsonObject.getString("benchamrkSourcePaths");
            String outputPath = jsonObject.getString("outputPath");
            String delimiter = jsonObject.getString("delimiter");

            // schema types
            JSONObject schemaJson = jsonObject.getJSONObject("schema");
            Map<String, List<String>> schema = new HashMap<>();
            for (String key : schemaJson.keySet()) {
                schema.put(key, Arrays.asList(schemaJson.getJSONArray(key).toList().toArray(new String[0])));
            }
            // columnNames
            JSONObject columnNamesJson = jsonObject.getJSONObject("columnNames");
            Map<String, List<String>> columnNames = new HashMap<>();
            for (String key : columnNamesJson.keySet()) {
                columnNames.put(key, Arrays.asList(columnNamesJson.getJSONArray(key).toList().toArray(new String[0])));
            }
            JSONObject numRowsJson = jsonObject.getJSONObject("numRows");
            Map<String, Integer> numRows = new HashMap<>();
            for (String key : numRowsJson.keySet()) {
                numRows.put(key, numRowsJson.getInt(key));
            }
            benchmarkInfos.add(new BenchmarkInfo(benchmarkName, benchamrkSourcePaths, schema, columnNames, numRows, delimiter, outputPath));
        }
        return benchmarkInfos;
    }

    private static Properties readProperties(String propertiesFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(propertiesFilePath));
        return properties;
    }

    public static void main(String[] args) throws IOException {

        ClassLoader classLoader = ParquetChunkWriter.class.getClassLoader();
        URL benchmarkResource = classLoader.getResource("benchmark.json");
        URL propertiesResource = classLoader.getResource("config.properties");

        // read benchmark.json
        List<BenchmarkInfo> benchmarkInfos = readBenchmarkInfosFromJson(benchmarkResource.getPath());
        String targetSchemaName = "clickbench";
        BenchmarkInfo foundBenchmarkInfo = findBenchmarkInfoByName(benchmarkInfos, targetSchemaName);
        // read config.properties
        Properties properties = readProperties(propertiesResource.getPath());
        String outputPath = foundBenchmarkInfo.getOutputPath();
        Map<String, List<String>> benchmarkInfoColumnNames = new HashMap<>();
        Map<String, List<String>> benchmarkInfoSchema = new HashMap<>();
        Map<String, Integer> benchmarkInfoNumRows = new HashMap<>();
        List<String> inputFilePaths = new ArrayList<>();
        assert foundBenchmarkInfo != null;
        String delimiter = foundBenchmarkInfo.getDelimiter();
        Configuration conf = new Configuration();
        TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());

        benchmarkInfoColumnNames = foundBenchmarkInfo.getColumnNames();
        benchmarkInfoSchema = foundBenchmarkInfo.getSchema();
        benchmarkInfoNumRows = foundBenchmarkInfo.getNumRows();
        String inputFilePath = foundBenchmarkInfo.getBenchamrkSourcePaths();

        for (String tableName : benchmarkInfoColumnNames.keySet()) {
            String tableDirPath = inputFilePath + "/" + tableName;
            File dir = new File(tableDirPath);
            if (dir.exists() && dir.isDirectory()) {
                File[] files = dir.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isFile()) {
                            inputFilePaths.add(file.getAbsolutePath());
                        }
                    }
                }
            }
        }
//        System.out.println(inputFilePaths);
        long startTime = System.currentTimeMillis();
        MultiThreadFileProcessing processor = new MultiThreadFileProcessing(benchmarkInfoColumnNames,
                benchmarkInfoSchema, benchmarkInfoNumRows, inputFilePaths, outputPath, delimiter, conf, properties);
        processor.processFiles();
        long endTime = System.currentTimeMillis();
        System.out.println("Execution Time: " + (endTime - startTime));
    }
}