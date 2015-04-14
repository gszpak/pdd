package helper;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;

public interface CSVLineConverterMixin {

	default String getSeparator() {
		return ",";
	}

	default List<Integer> lineToTuple(String line) {
		List<String> columnValues = Arrays.asList(line.split(getSeparator()));
		return columnValues
				.stream()
				.sequential()
				.map((s) -> Integer.valueOf(s))
				.collect(Collectors.toList());
	}

	default String tupleToLine(List<Integer> tuple) {
		Joiner joiner = Joiner.on(getSeparator());
		return joiner.join(tuple);
	}
}
