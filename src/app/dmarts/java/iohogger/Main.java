package app.dmarts.java.iohogger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main{
    protected static boolean RANDOM_ACCESS = false;
    protected static int BUFFER_SIZE = 4096 * 2; // multiples of 4k
    protected static long FILE_SIZE = 1048576 * 5; // multiples of 1MB
    protected static List<String> LOCATION = new ArrayList<String>();
    protected static int NUMBER_OF_FILES = 20;
    protected static String FILE_NAME_PREFIX = "iohogger";
    protected static ConcurrentHashMap<String, JsonElement> STATUS = new ConcurrentHashMap<>();
    private static ExecutorService IOGEN_POOL;


    public static void main(String[] args) {
        Options options = new Options();
        Main.LOCATION.add(System.getProperty("user.dir"));
        try {
            options.addOption("b", "buffer", true, "Integer. Read/write buffer size in bytes (default: " + Main.BUFFER_SIZE + ")");
            options.addOption("s", "filesize", true, "Integer. Size of each file in bytes (default: " + Main.FILE_SIZE + ")");

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
            System.out.println("Load config summary:");
            System.out.println("Buffer size (bytes): " + Main.BUFFER_SIZE);
            System.out.println("Random access: " + Main.RANDOM_ACCESS);
            System.out.println("Number of files: " + Main.NUMBER_OF_FILES);
            System.out.println("File name prefix: " + Main.FILE_NAME_PREFIX);
            System.out.println("Size of each file (bytes): " + Main.FILE_SIZE);
            System.out.println("File location: " + Main.LOCATION);



            System.out.print("Are you sure to continue (Y/N)? ");
            if (new BufferedReader(new InputStreamReader(System.in)).readLine().toUpperCase().matches("Y")){
                System.out.println("Continuing...");
                IOGEN_POOL = Executors.newFixedThreadPool(Main.NUMBER_OF_FILES%50);
                startProcess();
            }
            else {
                System.out.println("Exiting");
                System.exit(0);
            }
        }catch(ParseException | IOException | NumberFormatException pex) {
            System.err.println(pex.getMessage());
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.setWidth(100);
            helpFormatter.printHelp("java -jar IOGenerator.jar [options] --rnd | --seq", options);
            System.exit(1);
        }
    }

    private static void startProcess() {
        int EXPECTED_FILE_COUNT = 0;
        for(int i=0;i<Main.LOCATION.size();i++){
            File dir = new File(Main.LOCATION.get(i));
            if(dir.exists()) {
                if (dir.canWrite()) {
                    System.out.println("Writing on: " + dir.getAbsolutePath());
                    for(int j=0;j<Main.NUMBER_OF_FILES;j++) {
                        String filename = dir.getAbsolutePath() + File.separatorChar + Main.FILE_NAME_PREFIX + j;
                        EXPECTED_FILE_COUNT+=1;
                        Main.IOGEN_POOL.submit(new IOGenerator(filename));
                    }
                }
                else {
                    System.out.println("Directory " + dir.getAbsolutePath() + " isn't writable.");
                    continue;
                }
            }
            else {
                System.out.println("Directory " + dir.getAbsolutePath() + " doesn't exist and I won't create it.");
                continue;
            }
        }
        while (Main.STATUS.keySet().size()!=EXPECTED_FILE_COUNT){
        }
        Main.IOGEN_POOL.shutdown();
        for(String file:Main.STATUS.keySet()){
            System.out.println(Main.STATUS.get(file));
        }

    }



}

class IOGenerator implements Runnable{
    private File FILE;
    private ArrayList<Map<Long, Integer>> WRITE_MAP;
    public IOGenerator(String filename){
        this.FILE = new File(filename);
        this.WRITE_MAP = getReadWriteMap(Main.BUFFER_SIZE,Main.FILE_SIZE,!Main.RANDOM_ACCESS);
    }
    @Override
    public void run() {
        JsonObject json = new JsonObject();
        Main.STATUS.put(this.FILE.getAbsolutePath(),json);
        System.out.println(this.WRITE_MAP);
    }

    private ArrayList<Map<Long, Integer>> getReadWriteMap(int BUFFER_SIZE, long TOTAL_FILE_SIZE, boolean SEQUENTIAL){
        long WRITE_START;
        WRITE_START = 0;
        ArrayList<Map<Long, Integer>> WRITE_MAP = new ArrayList<>();
        for(int i=0;i<(TOTAL_FILE_SIZE/BUFFER_SIZE);i++){
            Map<Long, Integer> ENTRY = new HashMap<>();
            ENTRY.put(WRITE_START,BUFFER_SIZE);
            WRITE_MAP.add(ENTRY);
            WRITE_START+=BUFFER_SIZE;
        }
        if(TOTAL_FILE_SIZE%BUFFER_SIZE!=0) {
            Map<Long, Integer> ENTRY = new HashMap<>();
            ENTRY.put(WRITE_START, (int) TOTAL_FILE_SIZE % BUFFER_SIZE);
            WRITE_MAP.add(ENTRY);
        }
        if(!SEQUENTIAL)
            Collections.shuffle(WRITE_MAP);
        return WRITE_MAP;
    }
}
