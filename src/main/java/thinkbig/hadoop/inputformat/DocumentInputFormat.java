/**
 * Copyright (C) 2010-2014 Think Big Analytics, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package thinkbig.hadoop.inputformat;

import java.io.*;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;

/**
 * This input format reads an entire file (typically a text document) and emits a single record per file. Useful for processing
 * raw documents as a whole. The input key is the file path and the value is typically the contents of the document.
 * 
 * Set the parameter docinput.prepend.key to a non-empty value to have the format prepend the key followed by this value
 * before the contents of the document as the value. This is mostly useful for Hive, which oddly won't expose keys as part of the
 * data in a row.
 * 
 * @author rbodkin
 * 
 */
@SuppressWarnings("deprecation")
public class DocumentInputFormat extends FileInputFormat<Text,Text> implements JobConfigurable {
    private String prependKey = null;
    
    @Override
    public void configure(JobConf conf) {
        prependKey = conf.get("docinput.prepend.key");
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) {
        try {
            return new FullDocRecordReader((FileSplit) split, job);
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    // with HADOOP-0412, getPos is available as a method call on the class
    private static Method getPosMethod = null;
    static {
        try {
            getPosMethod = CompressionInputStream.class.getMethod("getPos");
        } catch (NoSuchMethodException e) {
        }
    }
    
    /**
     * FullDocRecordReader class to read an entire document as text.
     * 
     */
    public class FullDocRecordReader implements RecordReader<Text, Text> {

        private final InputStream fsin;
        private final FSDataInputStream rawFsin;
        private boolean hasRead = false;
        private Path file;

        public FullDocRecordReader(FileSplit split, Configuration conf) throws IOException {
            file = split.getPath();
            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
            final CompressionCodec codec = compressionCodecs.getCodec(file);
            FileSystem fs = file.getFileSystem(conf);
            rawFsin = fs.open(file);
            if (codec == null) {
                fsin = rawFsin;
            } else {
                fsin = codec.createInputStream(rawFsin);
            }
        }

        @Override
        public boolean next(Text key, Text value) throws IOException {
            if (hasRead)
                return false;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            if (prependKey!=null && !prependKey.isEmpty()) {
                bos.write(file.toString().getBytes());
                bos.write(prependKey.getBytes());
            }
            byte[] chunk=new byte[16*1024];            
            for(;;) {
                int len = fsin.read(chunk);
                if (len == -1)
                    break; //EFO
                bos.write(chunk, 0, len);
            }
            
            value.set(bos.toByteArray());
            key.set(file.toString());
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
        public Text createKey() {
            return new Text();
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
