package app.dmarts.java.diskhugger;

import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public  class IOGenerator implements Runnable {

        /* JSON struct of metric update in Main
        "filename":{
                                                              "min":{
                                                                     "ms":0,
                                                                     "when":UTC,
                                                                     "buffer":8192,
                                                                     "offset":21212
                                                                     },
                                                               "max":{
                                                                     "ms":12,
                                                                     "when":UTC,
                                                                     "buffer":8192,
                                                                     "offset": 121111
                                                                     }
                                                               },
                                                               "started_when":UTC,
                                                               "ended_when":UTC
                                                 //
                                                 //
                                                 */


        private final File FILE;
        private long LAST_POSITION = 0;
        private final long MAX;
        private final long MIN;
        private long BUFFER;
        private JsonObject MIN_OBJ = new JsonObject();
        private JsonObject MAX_OBJ = new JsonObject();
        private JsonObject METRIC_OBJ = new JsonObject();

        public IOGenerator(String filename) {
                this.FILE = new File(filename);
                if (Main.FILE_SIZE < Main.BUFFER_SIZE) {
                        this.MAX = Main.FILE_SIZE;
                        this.MIN = Main.FILE_SIZE;
                        this.BUFFER = Main.FILE_SIZE;
                } else {
                        this.MIN = Main.BUFFER_SIZE / 3;
                        this.MAX = Main.BUFFER_SIZE;
                        this.BUFFER = Main.BUFFER_SIZE;
                }
                this.METRIC_OBJ.add("min",this.MIN_OBJ);
                this.METRIC_OBJ.add("max",this.MAX_OBJ);
                this.METRIC_OBJ.addProperty("started_when",System.currentTimeMillis());
                Main.METRIC.put(this.FILE.getAbsolutePath(),this.METRIC_OBJ);
        }

        @Override
        public void run() {
                long max = 0, min = 1;
                if(Main.RANDOM_ACCESS){
                        if(Main.FILE_SIZE>this.BUFFER){
                                this.BUFFER = (long)((Math.random() * (this.MAX - this.MIN + 1)) + this.MIN);
                        }
                        Map<Long, Long> writemap;
                        FileChannel fileChannel = null;
                        try {
                                 fileChannel = new FileOutputStream(this.FILE).getChannel();
                                 while (this.LAST_POSITION!=Main.FILE_SIZE){
                                         writemap = getRandNextWrite(Main.FILE_SIZE,this.BUFFER,10);
                                         for(long position:writemap.keySet()){
                                                 long writebuffer = writemap.get(position);
                                                 fileChannel.position(position);
                                                 String data = LongStream.range(0,writebuffer).mapToObj(l -> "maal").collect(Collectors.joining()).substring(0,(int)writebuffer);

                                                 long start = System.currentTimeMillis();
                                                 fileChannel.write(ByteBuffer.wrap(data.getBytes()));
                                                 long end = System.currentTimeMillis();

                                                 if ((end-start)>max && writebuffer != 0){
                                                         max = end-start;
                                                         this.MAX_OBJ.addProperty("ms",max);
                                                         this.MAX_OBJ.addProperty("when",start);
                                                         this.MAX_OBJ.addProperty("offset",position);
                                                         this.MAX_OBJ.addProperty("buffer",writebuffer);
                                                         this.METRIC_OBJ.add("max",this.MAX_OBJ);
                                                         Main.METRIC.put(this.FILE.getAbsolutePath(),this.METRIC_OBJ);
                                                 }
                                                 if ((end-start)<min && writebuffer!=0){
                                                         min = end-start;
                                                         this.MIN_OBJ.addProperty("ms",min);
                                                         this.MIN_OBJ.addProperty("when",start);
                                                         this.MIN_OBJ.addProperty("offset",position);
                                                         this.MIN_OBJ.addProperty("buffer",writebuffer);
                                                         this.METRIC_OBJ.add("max",this.MIN_OBJ);
                                                         Main.METRIC.put(this.FILE.getAbsolutePath(),this.METRIC_OBJ);
                                                 }
                                         }
                                         this.BUFFER = (long) (Math.random() * (this.MAX - this.MIN + 1) + this.MIN);
                                 }
                        } catch (IOException e) {
                                e.printStackTrace();
                        } finally {
                                if (fileChannel !=null){
                                        try {
                                                fileChannel.close();
                                        } catch (IOException e) {
                                                e.printStackTrace();
                                        }
                                }
                        }
                }
                else{
                        Map<Long, Long> writemap;
                        FileChannel fileChannel = null;
                        try {
                                fileChannel = new FileOutputStream(this.FILE).getChannel();
                                if (Main.FILE_SIZE >= this.BUFFER) {
                                        for (int i = 0; i <= (Main.FILE_SIZE / this.BUFFER); i++) {
                                                writemap = getSeqNextWrite(Main.FILE_SIZE, this.BUFFER);
                                                for(long position:writemap.keySet()){
                                                        long writebuffer = writemap.get(position);
                                                        String data = LongStream.range(0, writebuffer)
                                                                .mapToObj(l -> "maal")
                                                                .collect(Collectors.joining())
                                                                .substring(0,(int)writebuffer);


                                                        long start = System.currentTimeMillis();
                                                        fileChannel.write(ByteBuffer.wrap(data.getBytes()));
                                                        long end = System.currentTimeMillis();
                                                        if ((end-start)>max && writebuffer != 0){
                                                                max = end-start;
                                                                this.MAX_OBJ.addProperty("ms",max);
                                                                this.MAX_OBJ.addProperty("when",start);
                                                                this.MAX_OBJ.addProperty("offset",position);
                                                                this.MAX_OBJ.addProperty("buffer",writebuffer);
                                                                this.METRIC_OBJ.add("max",this.MAX_OBJ);
                                                                Main.METRIC.put(this.FILE.getAbsolutePath(),this.METRIC_OBJ);
                                                        }
                                                        if ((end-start)<min && writebuffer!=0){
                                                                min = end-start;
                                                                this.MIN_OBJ.addProperty("ms",min);
                                                                this.MIN_OBJ.addProperty("when",start);
                                                                this.MIN_OBJ.addProperty("offset",position);
                                                                this.MIN_OBJ.addProperty("buffer",writebuffer);
                                                                this.METRIC_OBJ.add("max",this.MIN_OBJ);
                                                                Main.METRIC.put(this.FILE.getAbsolutePath(),this.METRIC_OBJ);
                                                        }
                                                }
                                        }
                                } else {

                                        for (int i = 0; i < (Main.FILE_SIZE / this.BUFFER); i++) {
                                                writemap = (getSeqNextWrite(Main.FILE_SIZE, this.BUFFER));
                                                for(long position:writemap.keySet()){
                                                        long writebuffer = writemap.get(position);
                                                        String data = LongStream.range(0, writebuffer)
                                                                .mapToObj(l -> "maal")
                                                                .collect(Collectors.joining())
                                                                .substring(0, (int)writebuffer);


                                                        long start = System.currentTimeMillis();
                                                        fileChannel.write(ByteBuffer.wrap(data.getBytes()));
                                                        long end = System.currentTimeMillis();
                                                        if ((end-start)>max && writebuffer != 0){
                                                                max = end-start;
                                                                this.MAX_OBJ.addProperty("ms",max);
                                                                this.MAX_OBJ.addProperty("when",start);
                                                                this.MAX_OBJ.addProperty("offset",position);
                                                                this.MAX_OBJ.addProperty("buffer",writebuffer);
                                                                this.METRIC_OBJ.add("max",this.MAX_OBJ);
                                                                Main.METRIC.put(this.FILE.getAbsolutePath(),this.METRIC_OBJ);
                                                        }
                                                        if ((end-start)<min && writebuffer!=0){
                                                                min = end-start;
                                                                this.MIN_OBJ.addProperty("ms",min);
                                                                this.MIN_OBJ.addProperty("when",start);
                                                                this.MIN_OBJ.addProperty("offset",position);
                                                                this.MIN_OBJ.addProperty("buffer",writebuffer);
                                                                this.METRIC_OBJ.add("max",this.MIN_OBJ);
                                                                Main.METRIC.put(this.FILE.getAbsolutePath(),this.METRIC_OBJ);
                                                        }
                                                }
                                        }
                                }
                        } catch (IOException e) {
                                e.printStackTrace();
                        } finally {
                                try {
                                        if (fileChannel!=null) {
                                                fileChannel.close();
                                        }
                                } catch (IOException e) {
                                        e.printStackTrace();
                                }
                        }

                }
                //System.out.println(Thread.currentThread().getName() + ": done");
                this.METRIC_OBJ.addProperty("ended_when",System.currentTimeMillis());
                Main.METRIC.put(this.FILE.getAbsolutePath(),this.METRIC_OBJ);
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

        private Map<Long, Long> getRandNextWrite(long filesize, long buffer, int writecount) {
                Map<Long, Long> ret = new HashMap<>(writecount);
                long position;
                while (writecount-- != 0) {
                        position = this.LAST_POSITION;
                        if ((buffer + position) >= filesize) {
                                buffer = filesize - position;
                        }
                        ret.put(position, buffer);
                        this.LAST_POSITION += buffer;
                }
                return ret;
        }

        private Map<Long, Long> getSeqNextWrite(long filesize, long buffer){
                Map<Long, Long> ret = new HashMap<>();
                long position = this.LAST_POSITION;
                if (position == filesize){
                        return ret;
                }
                if(position+buffer > filesize){
                        buffer = filesize-position;
                }
                ret.put(position, buffer);
                this.LAST_POSITION = position + buffer;
                return ret;
        }
}
