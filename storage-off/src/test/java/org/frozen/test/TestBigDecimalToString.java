package org.frozen.test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestBigDecimalToString {
	
	public static void main(String[] args) {
		try {
			String minString = "{0013E3418219FBFEE050C80A10011452}";
			String maxString = "{FFFFC53E4354754FE040C80A10016A2E}";

//			String minString = "{0013E341-8219-FBFE-E050-C80A10011452}";
//			String maxString = "{FFFFC53E-4354-754F-E040-C80A10016A2E}";
			
			int maxPrefixLen = Math.min(minString.length(), maxString.length());
			int sharedLen;
			for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
				char c1 = minString.charAt(sharedLen);
				char c2 = maxString.charAt(sharedLen);
				if (c1 != c2) {
					break;
				}
			}

			String commonPrefix = minString.substring(0, sharedLen);
			minString = minString.substring(sharedLen);
			maxString = maxString.substring(sharedLen);

			List<String> splitStrings = split(10L, minString, maxString, commonPrefix);
			
			System.out.println(splitStrings);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	static List<String> split(Long numSplits, String minString, String maxString, String commonPrefix) throws SQLException {

		BigDecimal minVal = stringToBigDecimal(minString);
		BigDecimal maxVal = stringToBigDecimal(maxString);

		List<BigDecimal> splitPoints = split(new BigDecimal(numSplits), minVal, maxVal);
		List<String> splitStrings = new ArrayList<String>();

		for (BigDecimal bd : splitPoints) {
			splitStrings.add(commonPrefix + bigDecimalToString(bd));
		}

		if (splitStrings.size() == 0 || !splitStrings.get(0).equals(commonPrefix + minString)) {
			splitStrings.add(0, commonPrefix + minString);
		}
		if (splitStrings.size() == 1 || !splitStrings.get(splitStrings.size() - 1).equals(commonPrefix + maxString)) {
			splitStrings.add(commonPrefix + maxString);
		}

		return splitStrings;
	}
	
	private static final BigDecimal MIN_INCREMENT = new BigDecimal(10000 * Double.MIN_VALUE);
	
	static List<BigDecimal> split(BigDecimal numSplits, BigDecimal minVal, BigDecimal maxVal) throws SQLException {
		List<BigDecimal> splits = new ArrayList<BigDecimal>();

	    BigDecimal splitSize = tryDivide(maxVal.subtract(minVal), (numSplits));
	    if (splitSize.compareTo(MIN_INCREMENT) < 0) {
	      splitSize = MIN_INCREMENT;
	    }

	    BigDecimal curVal = minVal;

	    while (curVal.compareTo(maxVal) <= 0) {
	      splits.add(curVal);
	      curVal = curVal.add(splitSize);
	    }

	    if (splits.get(splits.size() - 1).compareTo(maxVal) != 0 || splits.size() == 1) {
	      splits.add(maxVal);
	    }
		
		return splits;
	}
	
	private final static BigDecimal ONE_PLACE = new BigDecimal(65536);

	private final static int MAX_CHARS = 8;

	static BigDecimal stringToBigDecimal(String str) {
		BigDecimal result = BigDecimal.ZERO;
		BigDecimal curPlace = ONE_PLACE; // start with 1/65536 to compute the
											// first digit.

		int len = Math.min(str.length(), MAX_CHARS);

		for (int i = 0; i < len; i++) {
			int codePoint = str.codePointAt(i);
			result = result.add(tryDivide(new BigDecimal(codePoint), curPlace));
			curPlace = curPlace.multiply(ONE_PLACE);
		}

		return result;
	}

	static String bigDecimalToString(BigDecimal bd) {
		BigDecimal cur = bd.stripTrailingZeros();
		StringBuilder sb = new StringBuilder();

		for (int numConverted = 0; numConverted < MAX_CHARS; numConverted++) {
			cur = cur.multiply(ONE_PLACE);
			int curCodePoint = cur.intValue();
			if (0 == curCodePoint) {
				break;
			}

			cur = cur.subtract(new BigDecimal(curCodePoint));
			sb.append(Character.toChars(curCodePoint));
		}

		return sb.toString();
	}
	
	protected static BigDecimal tryDivide(BigDecimal numerator, BigDecimal denominator) {
		try {
			return numerator.divide(denominator);
		} catch (ArithmeticException ae) {
			return numerator.divide(denominator, BigDecimal.ROUND_HALF_UP);
		}
	}
	
}
