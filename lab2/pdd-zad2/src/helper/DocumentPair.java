package helper;

public class DocumentPair implements Comparable<DocumentPair> {

	private int firstDocumentNumber;
	private int secondDocumentNumber;

	public DocumentPair(int doc1, int doc2) {
		this.firstDocumentNumber = Math.min(doc1, doc2);
		this.secondDocumentNumber = Math.max(doc1, doc2);
	}

	public String toString() {
		return firstDocumentNumber + ", " + secondDocumentNumber;
	}

	public int getFirstDocumentNumber() {
		return firstDocumentNumber;
	}

	public int getSecondDocumentNumber() {
		return secondDocumentNumber;
	}

	@Override
	public int compareTo(DocumentPair o) {
		if (this.firstDocumentNumber < o.firstDocumentNumber) {
			return -1;
		} else if (this.firstDocumentNumber == o.firstDocumentNumber &&
				this.secondDocumentNumber < o.secondDocumentNumber) {
			return -1;
		} else if (this.firstDocumentNumber == o.firstDocumentNumber &&
				this.secondDocumentNumber == o.secondDocumentNumber) {
			return 0;
		} else {
			return 1;
		}
	}

}
