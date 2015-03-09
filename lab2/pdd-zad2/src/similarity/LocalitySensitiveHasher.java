package similarity;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class LocalitySensitiveHasher {

	private static int[][] readSignatures(String signatureFileName,
			int numOfRows, int numOfColumns) throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = null;
		try {
			inputStream = new ObjectInputStream(new FileInputStream(signatureFileName));
			int[][] signatures = (int[][]) inputStream.readObject();
			assert signatures.length == numOfRows;
			int [][] signaturesTranspose = new int[numOfColumns][numOfRows];
			for (int i = 0; i < signatures.length; ++i) {
				for (int j = 0; j < signatures[i].length; ++j) {
					assert signatures[i].length == numOfColumns;
					signaturesTranspose[j][i] = signatures[i][j];
				}
			}
			return signaturesTranspose;
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}

	private static int getHashForVector(int[][] signaturesTranspose, int rowsInBand, int actBand, int columnNum) {
		List<Integer> vector = new LinkedList<Integer>();
		for (int i = 0; i < rowsInBand; ++i) {
			vector.add(signaturesTranspose[columnNum][actBand + i]);
		}
		return vector.hashCode();
	}

	private static void updateCandidates(Map<Integer, List<Integer>> buckets,
			Set<DocumentPair> candidates) {
		for (List<Integer> bucket: buckets.values()) {
			Integer[] bucketAsArray = (Integer[]) bucket.toArray(new Integer[bucket.size()]);
			for (int i = 0; i < bucketAsArray.length; ++i) {
				for (int j = i + 1; j < bucketAsArray.length; ++j) {
					candidates.add(new DocumentPair(bucketAsArray[i].intValue(), bucketAsArray[j].intValue()));
				}
			}
		}
	}

	private static Set<DocumentPair> getSimilarCandidates(int[][] signaturesTranspose,
			int numOfColumns, int numOfRows, int rowsInBand) {
		Set<DocumentPair> candidates = new TreeSet<DocumentPair>();
		for (int actBand = 0; actBand < numOfRows; actBand += rowsInBand) {
			Map<Integer, List<Integer>> buckets = new HashMap<Integer, List<Integer>>();
			for (int columnNum = 0; columnNum < numOfColumns; ++columnNum) {
				Integer vectorHash = getHashForVector(signaturesTranspose, rowsInBand, actBand, columnNum);
				List<Integer> bucket = buckets.get(vectorHash);
				if (bucket == null) {
					bucket = new LinkedList<Integer>();
				}
				bucket.add(columnNum);
				buckets.put(vectorHash, bucket);
			}
			updateCandidates(buckets, candidates);
		}
		return candidates;
	}

	private static void printCandidates(Set<DocumentPair> candidates) {
		System.out.println("Candidates for similar documents:");
		for (DocumentPair documentPair: candidates) {
			System.out.println(documentPair.toString());
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		if (args.length != 4) {
			throw new IllegalArgumentException("Usage: java similarity.LocalitySensitiveHasher <b> <file_with_signatures> <num_of_rows> <num_of_columns>");
		}
		int numOfBands = Integer.parseInt(args[0]);
		String fileName = args[1];
		int numOfRows = Integer.parseInt(args[2]);
		int numOfColumns = Integer.parseInt(args[3]);
		assert numOfRows % numOfBands == 0;
		int rowsInBand = numOfRows / numOfBands;
		// Signature matrix is transposed (signatures are held in rows)
		int[][] signaturesTranspose = readSignatures(fileName, numOfRows, numOfColumns);
		// TODO: delete
		for (int i = 0; i < signaturesTranspose.length; ++i) {
			for (int j = 0; j < signaturesTranspose[i].length; ++j) {
				System.out.print(signaturesTranspose[i][j]);
			}
			System.out.println();
		}
		Set<DocumentPair> similarCandidates = getSimilarCandidates(signaturesTranspose, numOfColumns, numOfRows, rowsInBand);
		printCandidates(similarCandidates);
	}

}
