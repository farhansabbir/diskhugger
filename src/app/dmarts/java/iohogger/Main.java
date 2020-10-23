package app.dmarts.java.iohogger;

import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Main{
    protected static boolean RANDOM_ACCESS = false;
    protected static int BUFFER_SIZE = 4096 * 2; // multiples of 4k
    protected static long FILE_SIZE = 1048576 * 5; // multiples of 1MB
    protected static List<String> LOCATION = new ArrayList<String>(){

    };
    protected static int NUMBER_OF_FILES = 20;
    protected static String FILE_NAME_PREFIX = "iohogger";



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

            options.addOption("l", "locations", true, "String. Directories to read/write files (default: " + Main.LOCATION + ")");
            options.addOption("n","number-of-files",true,"Integer. Number of concurrent files to read from/write to (default: " + Main.NUMBER_OF_FILES + ". Max: 100)");
            options.addOption("p","prefix",true,"String. Filename prefix (default: " + Main.FILE_NAME_PREFIX + ")");
            options.getOption("l").setValueSeparator(':');
            CommandLine commandLine = new DefaultParser().parse(options, args);
            if (commandLine.hasOption("rnd"))
                Main.RANDOM_ACCESS = true;
            if (commandLine.hasOption("n")){
                Main.NUMBER_OF_FILES = Integer.parseInt(commandLine.getOptionValue("n"));
            }
            if (commandLine.hasOption("l")){
                Main.LOCATION = Arrays.asList(commandLine.getOptionValues("l"));
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
            System.out.println("Load summary:");
            System.out.println("Buffer size: " + Main.BUFFER_SIZE);
            System.out.println("Random access: " + Main.RANDOM_ACCESS);
            System.out.println("File location: " + Main.LOCATION);
            System.out.println("Number of files: " + Main.NUMBER_OF_FILES);
            System.out.println("Size of each file (bytes): " + Main.FILE_SIZE);


            System.out.print("Are you sure to continue (Y/N)? ");
            if (new BufferedReader(new InputStreamReader(System.in)).readLine().toUpperCase().matches("Y")){
                System.out.println("Continuing...");
                System.out.println(2%200);
            }
            else {
                System.out.println("Exiting");
                System.exit(0);
            }



        }catch(ParseException | IOException | NumberFormatException pex) {
            System.err.println(pex.getMessage());
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("java -jar IOGenerator.jar [options] --rnd | --seq", options);
            System.exit(1);
        }
    }

    private static ArrayList<Map<Long, Integer>> getReadWriteMap(int BUFFER_SIZE, long TOTAL_FILE_SIZE, boolean SEQUENTIAL){
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

