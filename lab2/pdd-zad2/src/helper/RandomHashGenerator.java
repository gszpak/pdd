package helper;

import java.util.Random;

public class RandomHashGenerator {

	private Random generator1 = new Random();
	private Random generator2 = new Random();
	private int modVal;

	public RandomHashGenerator(int modVal) {
		super();
		this.modVal = modVal;
	}

	public int getHash(int x) {
		int hash = (generator1.nextInt() * x + generator2.nextInt()) % modVal;
		return hash >= 0 ? hash : hash + modVal;
	}
}
