package similarity;
import helper.ObjectToFileExporter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class Shingler {

	private static String getFirstShingle(BufferedReader reader, int k) throws IOException {
		char[] shingle = new char[k];
		for (int i = 0; i < k; ++i) {
			int actChar = reader.read();
			if (actChar == -1) {
				break;
			}
			shingle[i] = (char) actChar;
		}
		return new String(shingle);
	}

	public static void shingle(int k, String inputFileName, String outputFileName) throws IOException {
		Set<Integer> hashedShingles = new HashSet<Integer>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(inputFileName));
			String actShingle = Shingler.getFirstShingle(reader, k);
			hashedShingles.add(actShingle.hashCode());
			int actChar;
			while ((actChar = reader.read()) != -1) {
				actShingle = new StringBuilder(actShingle.substring(1)).append((char) actChar).toString();
				hashedShingles.add(actShingle.hashCode());
			}
			ObjectToFileExporter.exportObjectToFile(hashedShingles, outputFileName);
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	public static void main(String args[]) throws IOException {
		if ((args.length % 2) != 1) {
			System.err.println("Usage: java Shinger <k> <input1> <output1> <input2> <output2> ... ");
			return;
		}
		int k = Integer.parseInt(args[0]);
		for (int i = 1; i < args.length; i += 2) {
			Shingler.shingle(k, args[i], args[i + 1]);
		}
	}
}
