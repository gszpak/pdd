package similarity;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class SimilarDocumentsFinder {

	private static String TEMP_DIR = System.getProperty("java.io.tmpdir");
	private static String MINHASHER_FILE__DIR = TEMP_DIR + File.separator + "signatures";

	private static String[] prepareFilesWithShingles(String[] filesNames) {
		String[] shingledFilesNames = Arrays.copyOf(filesNames, filesNames.length);
		for (int i = 0; i < shingledFilesNames.length; ++i) {
			File shingledFile = new File(shingledFilesNames[i]);
			String fileName = shingledFile.getName();
			shingledFilesNames[i] = TEMP_DIR + File.separator + fileName + fileName.hashCode();
		}
		return shingledFilesNames;
	}

	private static void runShingler(String k, String[] filesNames, String[] shingledFilesNames) throws IOException {
		assert filesNames.length == shingledFilesNames.length;
		int numOfShinglerArgs = filesNames.length + shingledFilesNames.length + 1;
		String[] shinglerArgs = new String[numOfShinglerArgs];
		shinglerArgs[0] = k;
		int i = 1;
		for (int j = 0; j < filesNames.length; ++j) {
			shinglerArgs[i++] = filesNames[j];
			shinglerArgs[i++] = shingledFilesNames[j];
		}
		Shingler.main(shinglerArgs);
	}

	private static void runMinhasher(String numOfSignatureElements,
			String[] shingledFilesNames) throws ClassNotFoundException, IOException {
		String[] minhasherArgs = new String[shingledFilesNames.length + 2];
		minhasherArgs[0] = numOfSignatureElements;
		minhasherArgs[1] = MINHASHER_FILE__DIR;
		for (int i = 2; i < minhasherArgs.length; ++i) {
			minhasherArgs[i] = shingledFilesNames[i - 2];
		}
		Minhasher.main(minhasherArgs);
	}

	private static void runLocalitySensitiveHasher(String numOfBands, String numOfRows,
			String numOfColumns) throws ClassNotFoundException, IOException {
		String[] localitySensitiveHasherArgs = {numOfBands, MINHASHER_FILE__DIR, numOfRows, numOfColumns};
		LocalitySensitiveHasher.main(localitySensitiveHasherArgs);
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		if (args.length < 5) {
			throw new IllegalArgumentException("Usage: java similarity.SimilarDocumentsFinder <k> "
					+ "<num_of_signature_elems> <num_of_bands> <file1> <file2> ...");
		}
		String k = args[0];
		String numOfSignatureElems = args[1];
		String numOfBands = args[2];
		String[] filesNames = Arrays.copyOfRange(args, 3, args.length);
		String[] shingledFilesNames = SimilarDocumentsFinder.prepareFilesWithShingles(filesNames);
		SimilarDocumentsFinder.runShingler(k, filesNames, shingledFilesNames);
		SimilarDocumentsFinder.runMinhasher(numOfSignatureElems, shingledFilesNames);
		// Number of rows in LSH matrix is equal to number of signature elements
		// Number of columns is equal to number of files
		SimilarDocumentsFinder.runLocalitySensitiveHasher(numOfBands, numOfSignatureElems, Integer.toString(filesNames.length));
	}

}
