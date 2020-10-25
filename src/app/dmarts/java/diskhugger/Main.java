package app.dmarts.java.diskhugger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.cli.*;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main{
    protected static boolean RANDOM_ACCESS = false;
    protected static int BUFFER_SIZE = 4096 * 2; // multiples of 4k
    protected static int FILE_SIZE = 1048576 * 5; // multiples of 1MB
    protected static List<String> LOCATION = new ArrayList<String>();
    protected static int NUMBER_OF_FILES = 20;
    protected static String FILE_NAME_PREFIX = "iohogger";
    protected static ConcurrentHashMap<String, JsonElement> STATUS = new ConcurrentHashMap<>();
    private static ExecutorService IOGEN_POOL;


    public static void main(String[] args) throws IOException {
        Options options = new Options();
        Main.LOCATION.add(System.getProperty("user.dir"));
        String dt = LocalDateTime.now().toString();
        long start = 0;
        long end = 0;
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
                Main.BUFFER_SIZE = Integer.parseInt(commandLine.getOptionValue("b"));
            }
            if (commandLine.hasOption("s")){
                Main.FILE_SIZE = Integer.parseInt(commandLine.getOptionValue("s"));
            }
            if (commandLine.hasOption("p")){
                Main.FILE_NAME_PREFIX = commandLine.getOptionValue("p");
            }


            HttpServer httpServer = HttpServer.create();
            httpServer.bind(new InetSocketAddress(8999),10);
            httpServer.createContext("/", new HttpHandler() {
                @Override
                public void handle(HttpExchange httpExchange) throws IOException {
                    if(httpExchange.getRequestMethod().equals("GET")){
                        //httpExchange.getResponseBody()
                        httpExchange.getResponseHeaders().add("Content-type","application/json");
                        JsonObject response = new JsonObject();
                        response.add("datetime",new JsonPrimitive(dt));
                        response.add("total_files",new JsonPrimitive(Main.NUMBER_OF_FILES));
                        response.add("block_size",new JsonPrimitive(Main.BUFFER_SIZE));
                        response.add("size_per_file",new JsonPrimitive(Main.FILE_SIZE));
                        response.add("folders",new JsonPrimitive(Main.LOCATION.toString()));
                        JsonArray temp = new JsonArray();
                        for(String key:Main.STATUS.keySet()){
                            JsonObject object = new JsonObject();
                            object.add(key,Main.STATUS.get(key));
                            temp.add(object);
                        }
                        response.add("metrics",temp);
                        httpExchange.sendResponseHeaders(200,response.toString().length());
                        httpExchange.getResponseBody().write(response.toString().getBytes());
                        httpExchange.close();
                    }
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
                start = System.currentTimeMillis();
                IOGEN_POOL = Executors.newFixedThreadPool((Main.NUMBER_OF_FILES%50)+1);
                startProcess();
                end = System.currentTimeMillis();
            }
            else {
                System.out.println("Exiting");
                System.exit(0);
            }
        }catch(ParseException | IOException | NumberFormatException | InterruptedException pex) {
            System.err.println(pex.getMessage());
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.setWidth(100);
            helpFormatter.printHelp("java -jar IOGenerator.jar [options] --rnd | --seq", options);
            System.exit(1);
        }
        /*
        int time_taken = 0;
        double speed = 0.0;
        for(String file:Main.STATUS.keySet()){
            time_taken += (Main.STATUS.get(file).getAsJsonObject().get("time_taken_in_ms").getAsInt());
            speed += (Main.STATUS.get(file).getAsJsonObject().get("speed_in_KB_per_sec").getAsDouble());
            System.out.println("{\"" + file + "\":" + Main.STATUS.get(file) + "}");
        }
        System.out.println("Load summary:");
        System.out.println("Datetime: " + dt);
        System.out.println("Buffer size (bytes): " + Main.BUFFER_SIZE);
        System.out.println("Random access: " + Main.RANDOM_ACCESS);
        System.out.println("Number of files: " + Main.NUMBER_OF_FILES);
        System.out.println("Size of each file (bytes): " + Main.FILE_SIZE);
        System.out.println("Average speed (KB/s): " + speed/Main.STATUS.size());
        System.out.println("Average time taken (ms): " + time_taken/Main.STATUS.size());
        System.out.println("Total time taken (ms): " + (end-start));
           */

                
    }

    private static void startProcess() throws InterruptedException {
        int EXPECTED_FILE_COUNT = 0;
        System.out.println("Please wait.");
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
        while (Main.STATUS.keySet().size()!=EXPECTED_FILE_COUNT){
            System.out.println("Completed: " + (Main.STATUS.size()*100/EXPECTED_FILE_COUNT) + "%");
            Thread.sleep(500);
        }
        Main.IOGEN_POOL.shutdown();

    }



}

class IOGenerator implements Runnable{
    private File FILE;
    private ArrayList<Map<Integer, Integer>> WRITE_MAP;
    public IOGenerator(String filename){
        this.FILE = new File(filename);
        this.WRITE_MAP = getReadWriteMap(Main.BUFFER_SIZE,Main.FILE_SIZE,!Main.RANDOM_ACCESS);
    }
    @Override
    public void run() {
        JsonObject json = new JsonObject();
        JsonObject min_obj = new JsonObject();
        JsonObject max_obj = new JsonObject();
        min_obj.addProperty("ms",0);
        max_obj.addProperty("ms",0);
        json.add("min",min_obj);
        json.add("max",max_obj);
        Main.STATUS.put(this.FILE.getAbsolutePath(),json);
        double timetaken = 0;
        try {

            FileOutputStream outputStream = new FileOutputStream(this.FILE);
            FileChannel channel = outputStream.getChannel();
            String str = "huddai";
            String writeme = String.join("",Collections.nCopies((Main.BUFFER_SIZE+str.length())/str.length(),str));
            for(int index=0;index<WRITE_MAP.size();index++){
                int offset = (int)this.WRITE_MAP.get(index).keySet().toArray()[0];
                long start = System.currentTimeMillis();
                channel.position(offset);
                channel.write(ByteBuffer.wrap(writeme.getBytes()));
                long end = System.currentTimeMillis();
                timetaken += end - start;

                JsonObject temp = Main.STATUS.get(this.FILE.getAbsolutePath()).getAsJsonObject();
                System.out.println("Here");
                min_obj = temp.getAsJsonObject("min");
                max_obj = temp.getAsJsonObject("max");
                int min = min_obj.get("ms").getAsInt();
                int max = max_obj.get("ms").getAsInt();
                if((end-start) <= min){
                    min_obj.addProperty("ms",(end-start));
                    min_obj.addProperty("datetime_in_ms",start);
                }
                if((end-start) > max){
                    max_obj.addProperty("ms",(end-start));
                    max_obj.addProperty("datetime_in_ms",start);
                }
                temp.add("min",min_obj);
                temp.add("max",max_obj);
                Main.STATUS.put(this.FILE.getAbsolutePath(),temp);

            }
            outputStream.close();

        } catch (FileNotFoundException e) {
            System.err.println(e);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println(e);
            e.printStackTrace();
        }catch (Exception e){
            System.err.println(e.fillInStackTrace());
        }
        json.add("time_taken_in_ms",new JsonPrimitive(timetaken));
        //double speed = (Main.FILE_SIZE/1024.0)/(timetaken/1000.0);
        //json.add("speed_in_KB_per_sec",new JsonPrimitive(speed));
        Main.STATUS.put("\"" + this.FILE.getAbsolutePath() + "\"",json);

    }

    private ArrayList<Map<Integer, Integer>> getReadWriteMap(int BUFFER_SIZE, int TOTAL_FILE_SIZE, boolean SEQUENTIAL){
        int WRITE_START;
        WRITE_START = 0;
        ArrayList<Map<Integer, Integer>> WRITE_MAP = new ArrayList<>();
        for(int i=0;i<(TOTAL_FILE_SIZE/BUFFER_SIZE);i++){
            Map<Integer, Integer> ENTRY = new HashMap<>();
            ENTRY.put(WRITE_START,BUFFER_SIZE);
            WRITE_MAP.add(ENTRY);
            WRITE_START+=BUFFER_SIZE;
        }
        if(TOTAL_FILE_SIZE%BUFFER_SIZE!=0) {
            Map<Integer, Integer> ENTRY = new HashMap<>();
            ENTRY.put(WRITE_START, (int) TOTAL_FILE_SIZE % BUFFER_SIZE);
            WRITE_MAP.add(ENTRY);
        }
        if(!SEQUENTIAL)
            Collections.shuffle(WRITE_MAP);
        return WRITE_MAP;
    }
}
