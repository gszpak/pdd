package similarity;
import helper.ObjectToFileExporter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;


public class Shingler {

	private static LinkedList<Character> getFirstShingle(BufferedReader reader, int k) throws IOException {
		LinkedList<Character> shingle = new LinkedList<Character>();
		for (int i = 0; i < k; ++i) {
			int actChar = reader.read();
			if (actChar == -1) {
				break;
			}
			shingle.add((char) actChar);
		}
		return shingle;
	}

	private static Set<Integer> getHashedShingles(int k, LinkedList<Character> firstShingle,
			BufferedReader reader) throws IOException {
		Set<Integer> hashedShingles = new HashSet<Integer>();
		if (firstShingle.size() < k) {
			return hashedShingles;
		}
		LinkedList<Character> actShingle = firstShingle;
		hashedShingles.add(actShingle.hashCode());
		int actChar;
		while ((actChar = reader.read()) != -1) {
			actShingle.removeFirst();
			actShingle.addLast((char) actChar);
			hashedShingles.add(actShingle.hashCode());
		}
		return hashedShingles;
	}

	public static void shingle(int k, String inputFileName, String outputFileName) throws IOException {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(inputFileName));
			LinkedList<Character> firstShingle = Shingler.getFirstShingle(reader, k);
			Set<Integer> hashedShingles = Shingler.getHashedShingles(k, firstShingle, reader);
			ObjectToFileExporter.exportObjectToFile(hashedShingles, outputFileName);
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	public static void main(String args[]) throws IOException {
		if (((args.length % 2) != 1) || (args.length < 2)) {
			throw new IllegalArgumentException("Usage: java similarity.Shinger <k> <input1> <output1> <input2> <output2> ... ");
		}
		int k = Integer.parseInt(args[0]);
		for (int i = 1; i < args.length; i += 2) {
			Shingler.shingle(k, args[i], args[i + 1]);
		}
	}
}
