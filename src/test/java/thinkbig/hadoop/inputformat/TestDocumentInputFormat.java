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

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

@SuppressWarnings("deprecation")
public class TestDocumentInputFormat extends Configured  implements Tool {
    
    @Override
    public int run(String[] args) throws Exception {
        JobConf job = new JobConf();
        job.setInputFormat(DocumentInputFormat.class);
        job.set("docinput.prepend.key", "TRUE");
        DocumentInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapOutputKeyClass(Text.class);
        JobClient.runJob(job);
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        new TestDocumentInputFormat().run(args);
    }

}
