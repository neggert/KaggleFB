package KaggleFB;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by nic on 12/16/13.
 */
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }
}
