package similarity;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class LocalitySensitiveHasher {

	private static int[][] readSignatures(String signatureFileName, int numOfRows,
			int numOfColumns) throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = null;
		try {
			inputStream = new ObjectInputStream(new FileInputStream(signatureFileName));
			int[][] signatures = (int[][]) inputStream.readObject();
			assert signatures.length == numOfRows;
			for (int i = 0; i < signatures.length; ++i) {
				assert signatures[i].length == numOfColumns;
			}
			return signatures;
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	private static int getHashForVector(int[][] signatures, int rowsInBand, int actBand, int columnNum) {
		List<Integer> vector = new LinkedList<Integer>();
		for (int i = actBand; i < rowsInBand; ++i) {
			vector.add(signatures[i][columnNum]);
		}
		return vector.hashCode();
	}

	private static void printSimilarDocumentsNumbers(Map<Integer, List<Integer>> buckets) {
		for (List<Integer> bucket: buckets.values()) {
			if (bucket.size() > 1) {
				System.out.println(Arrays.toString(bucket.toArray()));
			}
		}
	}

	private static void printSimilar(int[][] signatures, int numOfBands, int numOfColumns) {
		assert signatures.length % numOfBands == 0;
		int rowsInBand = signatures.length / numOfBands;
		System.out.println("Similar documents:");
		for (int actBand = 0; actBand < signatures.length; actBand += rowsInBand) {
			Map<Integer, List<Integer>> buckets = new HashMap<Integer, List<Integer>>();
			for (int column = 0; column < numOfColumns; ++column) {
				Integer vectorHash = LocalitySensitiveHasher.getHashForVector(signatures, rowsInBand, actBand, column);
				List<Integer> bucket = buckets.get(vectorHash);
				if (bucket == null) {
					bucket = new LinkedList<Integer>();
				}
				bucket.add(column);
				buckets.put(vectorHash, bucket);
			}
			LocalitySensitiveHasher.printSimilarDocumentsNumbers(buckets);
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		if (args.length != 2) {
			System.err.println("Usage: java similarity.LocalitySensitiveHasher <b> <file_with_signatures> <num_of_rows> <num_of_columns>");
			return;
		}

		int numOfBands = Integer.parseInt(args[0]);
		String fileName = args[1];
		int numOfRows = Integer.parseInt(args[2]);
		int numOfColumns = Integer.parseInt(args[3]);
		int[][] signatures = LocalitySensitiveHasher.readSignatures(fileName, numOfRows, numOfColumns);
		assert signatures.length % numOfBands == 0;
		LocalitySensitiveHasher.printSimilar(signatures, numOfBands, numOfColumns);

	}

}
