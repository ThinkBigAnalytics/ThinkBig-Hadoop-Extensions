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
