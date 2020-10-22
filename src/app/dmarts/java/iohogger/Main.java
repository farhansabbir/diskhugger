package app.dmarts.java.iohogger;

import org.apache.commons.cli.*;

import java.util.*;

public class Main{
    public static void main(String[] args) {
        Options options = new Options();
        try {
            options.addOption("b", "buffer", true, "Read/write buffer size in bytes (default is 8KB=8192)");
            options.addOption("s", "filesize", true, "Size of files in bytes (default is 1MB=1048576)");

            OptionGroup group = new OptionGroup();
            Option seqrand = Option.builder().longOpt("random").hasArg(false).build();
            group.setRequired(true);
            options.addOptionGroup(group);

            options.addRequiredOption("l", "locations", true, "Directories to read/write files");
            CommandLineParser parser = new DefaultParser();
            parser.parse(options, args);
        }catch(ParseException pex) {

            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("java -jar IOGenerator.jar [-b=#] [-s=#] <-l='/a,/b,/c'>", options);
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

