package app.dmarts.java.diskhugger;

import com.google.gson.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.cli.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main{
    protected static boolean RANDOM_ACCESS = false;
    protected static long BUFFER_SIZE = 4096 * 2; // multiples of 4k
    protected static long FILE_SIZE = 1048576 * 5; // multiples of 1MB
    protected static List<String> LOCATION = new ArrayList<>();
    protected static int NUMBER_OF_FILES = 20;
    protected static String FILE_NAME_PREFIX = "iohogger";
    protected static ConcurrentHashMap<String, JsonElement> STATUS = new ConcurrentHashMap<>();
    protected static ConcurrentHashMap<String, JsonElement> METRIC = new ConcurrentHashMap<>();
    private static ExecutorService IOGEN_POOL;


    public static void main(String[] args) throws IOException {
        Options options = new Options();
        Main.LOCATION.add(System.getProperty("user.dir"));
        try {
            options.addOption("b", "buffer", true, "Integer. Read/write buffer size in bytes (default: " + Main.BUFFER_SIZE + ")");
            options.addOption("s", "file-size", true, "Integer. Size of each file in bytes (default: " + Main.FILE_SIZE + ". Max: " + Integer.MAX_VALUE + ")");

            OptionGroup group = new OptionGroup();
            Option seqrand = Option.builder()
                    .desc("Enable random access")
                    .longOpt("rnd")
                    .hasArg(false)
                    .build();
            group.addOption(seqrand);
            seqrand = Option.builder()
                    .desc("Enable sequential access")
                    .longOpt("seq")
                    .hasArg(false)
                    .build();
            group.addOption(seqrand);
            group.setRequired(true);
            options.addOptionGroup(group);

            options.addOption("l", "locations", true, "String. CSV of directories to read/write files (default: " + Main.LOCATION + ")");
            options.addOption("n","number-of-files",true,"Integer. Number of concurrent files to read from/write to per location (default: " + Main.NUMBER_OF_FILES + ". Max: 100)");
            options.addOption("p","prefix",true,"String. Filename prefix (default: " + Main.FILE_NAME_PREFIX + ")");
            CommandLine commandLine = new DefaultParser().parse(options, args);
            if (commandLine.hasOption("rnd"))
                Main.RANDOM_ACCESS = true;
            if (commandLine.hasOption("n")){
                Main.NUMBER_OF_FILES = Integer.parseInt(commandLine.getOptionValue("n"));
            }
            if (commandLine.hasOption("l")){
                Main.LOCATION = new ArrayList<>();
                StringTokenizer tokenizer = new StringTokenizer(commandLine.getOptionValue("l"),",");
                if(tokenizer.countTokens()<1){
                    System.err.println("Your parameter for location is wrong. It should be comma delimited");
                    System.exit(1);
                }
                while (tokenizer.hasMoreTokens()){
                    String file = tokenizer.nextToken();
                    if(!(new File(file).isDirectory())){
                        System.err.println("Path " + file + " is not a directory, and I won't create it.");
                        System.exit(1);
                    }
                    Main.LOCATION.add(file);
                }
            }
            if (commandLine.hasOption("b")){
                Main.BUFFER_SIZE = Long.parseLong(commandLine.getOptionValue("b"));
            }
            if (commandLine.hasOption("s")){
                Main.FILE_SIZE = Long.parseLong(commandLine.getOptionValue("s"));
            }
            if (commandLine.hasOption("p")){
                Main.FILE_NAME_PREFIX = commandLine.getOptionValue("p");
            }

            // populate the hashmap with configs and setup
            Main.STATUS.put("metrices",new JsonObject());
            Main.STATUS.put("main_started_when",new JsonPrimitive(System.currentTimeMillis()));
            Main.STATUS.put("total_files",new JsonPrimitive(Main.NUMBER_OF_FILES));
            Main.STATUS.put("block_size",new JsonPrimitive(Main.BUFFER_SIZE));
            Main.STATUS.put("size_per_file",new JsonPrimitive(Main.FILE_SIZE));
            Main.STATUS.put("filename_prefix",new JsonPrimitive(Main.FILE_NAME_PREFIX));
            Main.STATUS.put("random_access",new JsonPrimitive(Main.RANDOM_ACCESS));
            JsonArray target = new JsonArray();
            for(String location:Main.LOCATION) {
                target.add(location);
            }
            Main.STATUS.put("target_locations",target);


            HttpServer httpServer = HttpServer.create();
            httpServer.bind(new InetSocketAddress(8999),10);
            httpServer.createContext("/", httpExchange -> {
                if(httpExchange.getRequestMethod().equals("GET")){
                    httpExchange.getResponseHeaders().add("Content-type","application/json");
                    StringBuilder response = new StringBuilder();
                    JsonObject metrics = new JsonObject();
                    Main.STATUS.put("metrices",new Gson().toJsonTree(Main.METRIC));
                    response.append(new Gson().toJson(Main.STATUS));
                    httpExchange.sendResponseHeaders(200,response.toString().length());
                    httpExchange.getResponseBody().write(response.toString().getBytes());
                    httpExchange.close();
                }
            });
            httpServer.start();


            System.out.println("Load config summary:");
            System.out.println("Buffer size (bytes): " + Main.BUFFER_SIZE);
            System.out.println("Random access: " + Main.RANDOM_ACCESS);
            System.out.println("Number of files: " + Main.NUMBER_OF_FILES);
            System.out.println("File name prefix: " + Main.FILE_NAME_PREFIX);
            System.out.println("Size of each file (bytes): " + Main.FILE_SIZE);
            System.out.println("File location: " + Main.LOCATION);


            System.out.print("Are you sure to continue (Y/N)? ");
            if (new BufferedReader(new InputStreamReader(System.in)).readLine().toUpperCase().matches("Y")){
                IOGEN_POOL = Executors.newFixedThreadPool((Main.NUMBER_OF_FILES%50)+1);
                System.out.println("Thread pool size: " + ((Main.NUMBER_OF_FILES%50)+1));
                startProcess();
            }
            else {
                System.out.println("Exiting");
                System.exit(0);
            }
        }catch(ParseException | IOException | NumberFormatException  pex) {
            System.err.println(pex.getMessage());
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.setWidth(100);
            helpFormatter.printHelp("java -jar IOGenerator.jar [options] --rnd | --seq", options);
            System.exit(1);
        }
        System.out.print("All threads have completed writing.");

        Main.STATUS.put("metrices",new Gson().toJsonTree(Main.METRIC));
        FileChannel fileChannel = new FileOutputStream(new File(Main.FILE_NAME_PREFIX + "-loadtest-report-" + Main.STATUS.get("main_started_when") + ".json")).getChannel();
        fileChannel.write(ByteBuffer.wrap(new Gson().toJson(Main.STATUS).getBytes()));
        fileChannel.close();
    }

    private static void startProcess(){
        int EXPECTED_FILE_COUNT = 0;
        System.out.println("Please wait.");
        long start = System.currentTimeMillis();
        for(int j=0;j<Main.NUMBER_OF_FILES;j++) {
            for (int i = 0; i < Main.LOCATION.size(); i++) {
                File dir = new File(Main.LOCATION.get(i));
                if (dir.exists()) {
                    if (dir.canWrite()) {
                        //System.out.println("Writing on: " + dir.getAbsolutePath());
                        String filename = dir.getAbsolutePath() + File.separatorChar + Main.FILE_NAME_PREFIX + j;
                        EXPECTED_FILE_COUNT += 1;
                        Main.IOGEN_POOL.submit(new IOGenerator(filename));
                    } else {
                        System.out.println("Directory " + dir.getAbsolutePath() + " isn't writable.");
                        continue;
                    }
                } else {
                    System.out.println("Directory " + dir.getAbsolutePath() + " doesn't exist and I won't create it.");
                    continue;
                }
            }
        }
        //System.out.println("Metric: " + Main.METRIC.keySet().size());
        //System.out.println("Expected: " + EXPECTED_FILE_COUNT);
        while(Main.METRIC.keySet().size() != EXPECTED_FILE_COUNT){

        }
        Main.IOGEN_POOL.shutdown();
        long end = 0;
        while (!Main.IOGEN_POOL.isTerminated()) {
            end = System.currentTimeMillis();
        }
        Main.STATUS.put("threads_started_when",new JsonPrimitive(start));
        Main.STATUS.put("threads_ended_when",new JsonPrimitive(end));
        Main.STATUS.put("total_time_taken_ms",new JsonPrimitive(end-start));
        Main.STATUS.put("total_bytes_written",new JsonPrimitive(EXPECTED_FILE_COUNT * Main.FILE_SIZE));
        Main.STATUS.put("total_files_written",new JsonPrimitive(Main.METRIC.size()));
        Main.STATUS.put("avg_speed_mbps",new JsonPrimitive(((EXPECTED_FILE_COUNT * Main.FILE_SIZE)/(1048576))/((end-start)/1000.0)));
    }
}
