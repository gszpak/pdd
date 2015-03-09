package similarity;
import helper.ObjectToFileExporter;
import helper.RandomHashFunction;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Minhasher {

	@SuppressWarnings("unchecked")
	private static Set<Integer> readColumn(String shingledDocument) throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = null;
		try {
			inputStream = new ObjectInputStream(new FileInputStream(shingledDocument));
			return (Set<Integer>) inputStream.readObject();
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	private static List<Set<Integer>> readAllColumns(String[] shingledDocuments) throws ClassNotFoundException, IOException {
		List<Set<Integer>> columns = new ArrayList<Set<Integer>>();
		for (String shingledDocument: shingledDocuments) {
			columns.add(Minhasher.readColumn(shingledDocument));
		}
		return columns;
	}

	private static int getNumOfRows(List<Set<Integer>> columns) {
		Set<Integer> rows = new HashSet<Integer>();
		for (Set<Integer> column: columns) {
			rows.addAll(column);
		}
		return rows.size();
	}

	private static int calculateMinhash(Set<Integer> column, RandomHashFunction hashFun) {
		// Number of first non - zero row, i.e. minimum from
		// hashes of numbers in column
		int minPermutedNumber = Integer.MAX_VALUE;
		for (Integer shingle: column) {
			int actHash = hashFun.getHash(shingle);
			minPermutedNumber = Math.min(minPermutedNumber, actHash);
		}
		return minPermutedNumber;
	}

	public static int[][] calculateSignatures(List<Set<Integer>> columns,
			int numOfSignatureElements, int numOfRows) {
		int numOfColumns = columns.size();
		int[][] signatures = new int[numOfSignatureElements][numOfColumns];
		for (int i = 0; i < numOfSignatureElements; ++i) {
			RandomHashFunction hashFun = new RandomHashFunction(numOfRows);
			int numOfActColumn = 0;
			for (Set<Integer> column: columns) {
				int actMinhash = Minhasher.calculateMinhash(column, hashFun);
				signatures[i][numOfActColumn] = actMinhash;
				numOfActColumn += 1;
			}
		}
		return signatures;
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		if (((args.length % 2) != 0) || (args.length < 3)) {
			throw new IllegalArgumentException("Usage: java similarity.Minhasher <num_of_signature_elems> <output> "
					+ "<shingled1> <shingled2> ...");
		}
		int numOfSignatureElems = Integer.parseInt(args[0]);
		String outputFileName = args[1];

		List<Set<Integer>> columns = Minhasher.readAllColumns(Arrays.copyOfRange(args, 2, args.length));
		int numOfRows = Minhasher.getNumOfRows(columns);
		int[][] signatures = Minhasher.calculateSignatures(columns, numOfSignatureElems, numOfRows);
		ObjectToFileExporter.exportObjectToFile(signatures, outputFileName);
	}

}
