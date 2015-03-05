package similarity;
import helper.ObjectToFileExporter;
import helper.RandomHashGenerator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


public class Minhasher {

	@SuppressWarnings("unchecked")
	private static Set<Integer> getColumn(String shingledDocument) throws IOException, ClassNotFoundException {
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

	private static List<Set<Integer>> getAllColumns(String[] shingledDocuments) throws ClassNotFoundException, IOException {
		List<Set<Integer>> columns = new ArrayList<Set<Integer>>();
		for (String shingledDocument: shingledDocuments) {
			columns.add(Minhasher.getColumn(shingledDocument));
		}
		return columns;
	}

	private static List<Integer> getAllRows(List<Set<Integer>> columns) {
		Set<Integer> rows = new TreeSet<Integer>();
		for (Set<Integer> column: columns) {
			rows.addAll(column);
		}
		return new ArrayList<Integer>(rows);
	}

	private static RandomHashGenerator[] getHashFunctions(int numOfSignatureElems, int numOfRows) {
		RandomHashGenerator[] result = new RandomHashGenerator[numOfSignatureElems];
		for (int i = 0; i < numOfSignatureElems; ++i) {
			result[i] = new RandomHashGenerator(numOfRows);
		}
		return result;
	}

	private static int[][] initSignatures(int numOfSignatureElems, int numOfColumns) {
		int[][] signatures = new int[numOfSignatureElems][numOfColumns];
		for (int i = 0; i < numOfSignatureElems; ++i) {
			for (int j = 0; j < numOfColumns; ++j) {
				signatures[i][j] = Integer.MAX_VALUE;
			}
		}
		return signatures;
	}

	private static void calculateSignatures(List<Set<Integer>> columns, List<Integer> rows,
			int[][] signatures, RandomHashGenerator[] hashFunctions) {
		int[] computedHashes = new int[hashFunctions.length];
		for (Integer row: rows) {
			for (int i = 0; i < hashFunctions.length; ++i) {
				computedHashes[i] = hashFunctions[i].getHash(row.intValue());
			}

			int numOfActColumn = 0;
			for (Set<Integer> column: columns) {
				if (column.contains(row)) {
					for (int i = 0; i < signatures.length; ++i) {
						signatures[i][numOfActColumn] = Math.min(signatures[i][numOfActColumn], computedHashes[i]);
					}
				}
				numOfActColumn += 1;
			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		if ((args.length % 2) != 0) {
			System.err.println("Usage: java Minhasher <num_of_signature_elems> <output> <shingled1> <shingled2> ...");
			return;
		}
		int numOfSignatureElems = Integer.parseInt(args[0]);
		String outputFileName = args[1];

		List<Set<Integer>> columns = Minhasher.getAllColumns(Arrays.copyOfRange(args, 2, args.length));
		List<Integer> rows = Minhasher.getAllRows(columns);
		RandomHashGenerator[] hashFunctions = Minhasher.getHashFunctions(numOfSignatureElems, rows.size());
		int[][] signatures = Minhasher.initSignatures(numOfSignatureElems, columns.size());
		Minhasher.calculateSignatures(columns, rows, signatures, hashFunctions);
		// TODO: delete
		for (int i = 0; i < signatures.length; ++i) {
			for (int j = 0; j < signatures[i].length; ++j) {
				System.out.print(signatures[i][j] + " ");
			}
			System.out.println();
		}
		ObjectToFileExporter.exportObjectToFile(signatures, outputFileName);
	}

}
