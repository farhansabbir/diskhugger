package app.dmarts.java.diskhugger;

import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public  class IOGenerator implements Runnable{

        private File FILE;
        private ArrayList<Map<Integer, Integer>> WRITE_MAP;
        private JsonObject THREAD_METRIC = new JsonObject();
        private JsonObject MIN_OBJ = new JsonObject();
        private JsonObject MAX_OBJ = new JsonObject();
        public IOGenerator(String filename){
            this.FILE = new File(filename);
            this.WRITE_MAP = getReadWriteMap(Main.BUFFER_SIZE,Main.FILE_SIZE,!Main.RANDOM_ACCESS);
            this.THREAD_METRIC.add("min",this.MIN_OBJ);
            this.THREAD_METRIC.add("max",this.MAX_OBJ);
            Main.METRIC.put(this.FILE.getAbsolutePath(), THREAD_METRIC);
        }
        @Override
        public void run() {
            double timetaken = 0;
            int max, min, cycles = 0;
            max = 0;
            min = 1;
            long maxtime, mintime;

            try {
                FileOutputStream outputStream = new FileOutputStream(this.FILE);
                FileChannel channel = outputStream.getChannel();
                String str = "maal";
                String writeme = String.join("", Collections.nCopies((Main.BUFFER_SIZE+str.length())/str.length(),str));
                //System.out.println((Main.BUFFER_SIZE+str.length())/str.length());

                for(int index=0;index<WRITE_MAP.size();index++){
                    int offset = (int)this.WRITE_MAP.get(index).keySet().toArray()[0];
                    long start = System.currentTimeMillis();
                    channel.position(offset);
                    channel.write(ByteBuffer.wrap(writeme.getBytes()));
                    long end = System.currentTimeMillis();
                    timetaken += end - start;
                    if ((int)(end-start)>max){
                        max = (int)(end-start);
                        maxtime = start;
                        this.MAX_OBJ.addProperty("ms",max);
                        this.MAX_OBJ.addProperty("when",maxtime);
                    }

                    if ((int)(end-start)<min){
                        min = (int)(end-start);
                        mintime = start;
                        this.MIN_OBJ.addProperty("ms",min);
                        this.MIN_OBJ.addProperty("when",mintime);
                    }
                    cycles+=1;
                    this.THREAD_METRIC.add("min",this.MIN_OBJ);
                    this.THREAD_METRIC.add("max",this.MAX_OBJ);
                    this.THREAD_METRIC.addProperty("cycles",cycles);
                    this.THREAD_METRIC.addProperty("time_taken",timetaken);
                    this.THREAD_METRIC.addProperty("when",end);
                    Main.METRIC.put(this.FILE.getAbsolutePath(),this.THREAD_METRIC);
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



/*



public class Main {
    private static long last_pos = 0;
    private static long max, min;
    public static void main(String[] args) throws InterruptedException, IOException {
        long filesize = 5368700L; //5368709120L
        long buffer   = 8192;
        boolean random = true;



        if(filesize<buffer){
            min = filesize;
            max = filesize;
            buffer = filesize;
        }
        else {
            min = buffer/3;
            max = buffer;
        }

        if(random) {
            // buffer size is decided here;
            if(filesize>buffer) {
                buffer = (long) (Math.random() * (max - min + 1) + min);
            }
            System.out.println("Min: " + min + ", max: " + max + ", buffer: " + buffer + ", filesize: " + filesize);
            Map<Long, Long> writemap = null;
            FileChannel channel = new FileOutputStream(new File("writerme")).getChannel();
            while (last_pos!=filesize){
                writemap = (getRandNextWrite(filesize, buffer, 10));
                for(long position:writemap.keySet()){
                    System.out.println("Position: " + position + ", buffer: " + writemap.get(position));
                    long writebuffer = writemap.get(position);
                    channel.position(position);
                    String data = LongStream.range(0,writebuffer).mapToObj(l -> "data").collect(Collectors.joining()).substring(0,(int)writebuffer);
                    channel.write(ByteBuffer.wrap(data.getBytes()));
                }
                buffer = (long) (Math.random() * (max - min + 1) + min);
            }
            channel.close();
            System.out.println("Last pos: " + last_pos + ", Remaining: " + (filesize-last_pos));
            buffer = (filesize-last_pos);
            if(buffer>0){
                System.out.println();
            }

        }
        else{
            System.out.println("Min: " + min + ", max: " + max + ", buffer: " + buffer + ", filesize: " + filesize);
            Map<Long, Long> writemap = null;
            if(filesize>=buffer) {
                for (int i = 0; i <= (filesize / buffer); i++) {
                    writemap = getSeqNextWrite(filesize, buffer);
                    System.out.printf("" + writemap);
                }
            }
            else  {
                for (int i = 0; i < (filesize / buffer); i++) {
                    writemap = (getSeqNextWrite(filesize, buffer));
                    System.out.printf("" + writemap);
                }
            }
        }
    }

    private static Map<Long, Long> getRandNextWrite(long filesize, long buffer, int writecount){
        Map<Long, Long> ret = new HashMap<>(writecount);
        long position = 0;
        while (writecount--!=0){
            position = last_pos;
            if ((buffer + position) >=filesize ){
                buffer = filesize-position;
            }
            ret.put(position,buffer);
            last_pos += buffer;
        }
        return ret;
    }

    private static Map<Long, Long> getSeqNextWrite(long filesize, long buffer){
        Map<Long, Long> ret = new HashMap<>();
        long position = last_pos;
        if (position == filesize){
            return ret;
        }
        if(position+buffer > filesize){
            buffer = filesize-position;
        }
        ret.put(position, buffer);
        last_pos = position + buffer;
        return ret;
    }


 */
