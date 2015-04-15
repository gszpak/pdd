package helper;

public enum Position {
	LEFT("left"), MIDDLE("middle"), RIGHT("right");

	private String repr;

	private Position(String repr) {
		this.repr = repr;
	}

	public String toString() {
		return this.repr;
	}

	public static Position fromRepr(String repr) {
		for (Position position: Position.values()) {
			if (position.repr.equals(repr)) {
				return position;
			}
		}
		throw new IllegalArgumentException();
	}
}
