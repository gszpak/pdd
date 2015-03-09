package helper;

import java.math.BigInteger;
import java.util.Random;

public class RandomHashFunction {

	private static int PRIME_BIT_LENGTH = 24;

	private int prime1;
	private int prime2;
	private int modVal;

	public RandomHashFunction(int modVal) {
		super();
		Random rand = new Random();
		this.prime1 = BigInteger.probablePrime(PRIME_BIT_LENGTH, rand).intValue();
		this.prime2 = BigInteger.probablePrime(PRIME_BIT_LENGTH, rand).intValue();
		this.modVal = modVal;
	}

	public int getHash(int x) {
		long temp = ((long) prime1) * ((long) x);
		int hash = (int) (temp % modVal);
		hash = (hash + prime2) % modVal;
		assert hash >= 0 && hash < modVal;
		return hash;
	}
}
