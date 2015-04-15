package helper;

import java.math.BigInteger;
import java.util.Random;

public class UniversalHashFunction {

	private static final int PRIME_BIT_LENGTH = 12;

	private static final Random RAND = new Random();
	private static final int PRIME1 = BigInteger.probablePrime(PRIME_BIT_LENGTH, RAND).intValue();
	private static final int PRIME2 = BigInteger.probablePrime(PRIME_BIT_LENGTH, RAND).intValue();

	public static final int P = BigInteger.probablePrime(PRIME_BIT_LENGTH, RAND).intValue();

	public static int getHash(Object o) {
		int hash = o.hashCode();
		return (PRIME1 * hash + PRIME2) % P;
	}

}
