package de.julielab.neo4j.plugins.auxiliaries;

import java.util.List;

import org.apache.commons.lang.ArrayUtils;

public class JulieNeo4jUtilities {
	/**
	 * Converts <tt>elements</tt> into an array of the runtime type <tt>cls</tt>
	 * .
	 * 
	 * @param cls
	 * @param elements
	 * @return
	 */
	public static Object[] convertElementsIntoArray(Class<?> cls, Object... elements) {
		Object[] ret;
		if (cls.equals(String.class)) {
			ret = new String[elements.length];
		} else if (cls.equals(Byte.class)) {
			ret = new Byte[elements.length];
		} else if (cls.equals(Long.class)) {
			ret = new Long[elements.length];
		} else if (cls.equals(Integer.class)) {
			ret = new Integer[elements.length];
		} else if (cls.equals(Double.class)) {
			ret = new Double[elements.length];
		} else if (cls.equals(Float.class)) {
			ret = new Float[elements.length];
		} else if (cls.equals(Boolean.class)) {
			ret = new Boolean[elements.length];
		} else if (cls.equals(Character.class)) {
			ret = new Character[elements.length];
		} else if (cls.equals(Short.class)) {
			ret = new Short[elements.length];
		} else
			throw new IllegalArgumentException(String.format("%s[] is not a supported property value type",
					elements.getClass().getComponentType().getName()));
		System.arraycopy(elements, 0, ret, 0, ret.length);
		return ret;
	}

	/**
	 * Converts <tt>value</tt> to its correct runtime array type. Requires that
	 * <tt>value</tt> is an array of a Java base type (String, int, Double etc).
	 * 
	 * @param value
	 *            The array expressed as an object
	 * @return The same array but cast to the correct runtime element type.
	 */
	public static Object[] convertArray(Object value) {
		if (!value.getClass().isArray())
			throw new IllegalArgumentException("Passed value " + value + " is no array.");
		if (value instanceof String[]) {
			return (String[]) value;
		} else if (value instanceof Byte[]) {
			return (Byte[]) value;
		} else if (value instanceof byte[]) {
			byte[] primitiveArray = (byte[]) value;
			return ArrayUtils.toObject(primitiveArray);
		} else if (value instanceof Long[]) {
			return (Long[]) value;
		} else if (value instanceof long[]) {
			long[] primitiveArray = (long[]) value;
			return ArrayUtils.toObject(primitiveArray);
		} else if (value instanceof Integer[]) {
			return (Integer[]) value;
		} else if (value instanceof int[]) {
			int[] primitiveArray = (int[]) value;
			return ArrayUtils.toObject(primitiveArray);
		} else if (value instanceof Double[]) {
			return (Double[]) value;
		} else if (value instanceof double[]) {
			double[] primitiveArray = (double[]) value;
			return ArrayUtils.toObject(primitiveArray);
		} else if (value instanceof Float[]) {
			return (Float[]) value;
		} else if (value instanceof float[]) {
			float[] primitiveArray = (float[]) value;
			return ArrayUtils.toObject(primitiveArray);
		} else if (value instanceof Boolean[]) {
			return (Boolean[]) value;
		} else if (value instanceof boolean[]) {
			boolean[] primitiveArray = (boolean[]) value;
			return ArrayUtils.toObject(primitiveArray);
		} else if (value instanceof Character[]) {
			return (Character[]) value;
		} else if (value instanceof char[]) {
			char[] primitiveArray = (char[]) value;
			return ArrayUtils.toObject(primitiveArray);
		} else if (value instanceof Short[]) {
			return (Short[]) value;
		} else if (value instanceof short[]) {
			short[] primitiveArray = (short[]) value;
			return ArrayUtils.toObject(primitiveArray);
		}
		throw new IllegalArgumentException(String.format("%s[] is not a supported property value type",
				value.getClass().getComponentType().getName()));
	}

	@SuppressWarnings("unchecked")
	public static <T> T[] convertListToArray(List<T> list, Class<?> cls) {
		T[] array = (T[]) java.lang.reflect.Array.newInstance(cls, list.size());
		return list.toArray(array);
	}

	/**
	 * Expects an array that is filled with <tt>value</tt> from the right and
	 * returns the index of the first entry that equals <tt>value</tt> in the
	 * array performing a binary search. Returns -1 if the value was found.
	 * 
	 * @param array
	 * @return
	 */
	public static <T> int findFirstValueInArray(T[] array, T value) {
		if (null == array || array.length == 0)
			return -1;
		// special handling for the edge cases, empty or full array
		if (array[0].equals(value))
			return 0;
		if (!array[array.length - 1].equals(value))
			return -1;
		// neither empty nor full, do binary search
		int left = 1;
		int right = array.length - 2;
		int pos = (int) Math.ceil((right - left) / 2 + left);
		boolean posFound = false;
		while (!posFound) {
			if (!array[pos].equals(value)) {
				if (pos < array.length - 1 && array[pos + 1].equals(value)) {
					++pos;
					posFound = true;
				} else if (right == left) {
					return -1;
				} else {
					left = pos + 1;
					pos = (int) Math.ceil((right - left) / 2 + left);
				}
			} else {
				if (pos > 0 && !array[pos - 1].equals(value)) {
					posFound = true;
				} else if (right == left) {
					return -1;
				} else {
					right = pos - 1;
					pos = (int) Math.ceil((right - left) / 2 + left);
				}
			}
		}
		return pos;
	}
}
