import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class WordCounter {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Invalid number of arguments");
			return;
		}

		Configuration config = new Configuration();
		Path localPath = new Path(args[0]);
		Path hadoopPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(config);
		fs.copyFromLocalFile(localPath, hadoopPath);

        FSDataInputStream inputStream = null;
        try {
        	inputStream = fs.open(hadoopPath);
        	BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        	String line;
        	int counter = 0;
        	while ((line = reader.readLine()) != null) {
        		for (String word: line.split("\\s+")) {
        			if (word.length() > 0) {
        				counter += 1;
        			}
        		}
			}
        	System.out.println("Number of words in HDFS file " + args[1] + ": " + counter);
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

}
