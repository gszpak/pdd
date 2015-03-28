package helper;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;

public interface CSVLineConverterMixin {

	default String getSeparator() {
		return ",";
	}

	default VisibleTupleWritable readCSVLineToTuple(String line) {
		List<String> columnValues = Arrays.asList(line.split(getSeparator()));
		Text[] columnValuesAsText = columnValues
				.stream()
				.sequential()
				.map((s) -> new Text(s))
				.toArray(Text[]::new);
		return new VisibleTupleWritable(columnValuesAsText);
	}

}
