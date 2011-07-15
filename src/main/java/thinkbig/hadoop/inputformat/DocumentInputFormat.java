package thinkbig.hadoop.inputformat;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.charset.Charset;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.*;
import javax.xml.stream.events.XMLEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;

import thinkbig.hadoop.inputformat.XmlInputFormat.XmlRecordReader;

public class DocumentInputFormat extends TextInputFormat {
    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) {
        try {
            return new FullDocRecordReader((FileSplit) split, job);
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /**
     * XMLRecordReader class to read through a given xml document to output xml blocks as records as specified by the start tag
     * and end tag
     * 
     */
    public static class FullDocRecordReader implements RecordReader<LongWritable, Text> {

        private final InputStream fsin;
        private final FSDataInputStream rawFsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private boolean hasRead = false;

        // with HADOOP-0412, getPos is available as a method call on the class
        private static Method getPosMethod = null;
        static {
            try {
                getPosMethod = CompressionInputStream.class.getMethod("getPos");
            } catch (NoSuchMethodException e) {
            }
        }
        
        public FullDocRecordReader(FileSplit split, Configuration conf) throws IOException {
            Path file = split.getPath();
            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
            final CompressionCodec codec = compressionCodecs.getCodec(file);
            FileSystem fs = file.getFileSystem(conf);
            rawFsin = fs.open(split.getPath());
            if (codec == null) {
                fsin = rawFsin;
            } else {
                fsin = codec.createInputStream(rawFsin);
            }
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            if (hasRead)
                return false;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] chunk=new byte[16*1024];            
            for(;;) {
                int len = fsin.read(chunk);
                if (len == -1)
                    break; //EFO
                bos.write(chunk, 0, len);
            }
            
            value.set(bos.toByteArray());
            key.set(0);
            hasRead = true;
            return true;
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        @Override
        public float getProgress() throws IOException {
            return hasRead ? 1f : 0f;
        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return rawFsin.getPos();
        }
    }

}
