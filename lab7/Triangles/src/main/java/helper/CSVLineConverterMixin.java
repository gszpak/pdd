package helper;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;

public interface CSVLineConverterMixin {

	default String getSeparator() {
		return ",";
	}

	default List<String> lineToTuple(String line) {
		return Arrays.asList(line.split(getSeparator()));
	}

	default String tupleToLine(List<?> tuple) {
		Joiner joiner = Joiner.on(getSeparator());
		return joiner.join(tuple);
	}
}
