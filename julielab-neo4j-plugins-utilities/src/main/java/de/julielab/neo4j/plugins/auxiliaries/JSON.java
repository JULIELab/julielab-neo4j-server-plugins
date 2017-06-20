package de.julielab.neo4j.plugins.auxiliaries;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

public class JSON {
	/**
	 * <p>
	 * Copies the contents of {@link JSONArray} into a newly allocated Java
	 * String array which is then returned.
	 * </p>
	 * <p>
	 * If exclusion Strings are specified, those elements will not be copied
	 * into the new Java array.
	 * </p>
	 * 
	 * @param jsonArray
	 *            A JSONArray of primitives to be converted into a Java array.
	 * @param exclusions
	 *            Elements in <tt>jsonArray</tt> to be excluded from the new
	 *            Java array.
	 * @return A Java array with the contents of <tt>jsonArray</tt>, if
	 *         <tt>jsonArray</tt> has elements, <tt>null</tt> otherwise.
	 * @throws JSONException
	 */
	@SafeVarargs
	public static <T> T[] json2JavaArray(JSONArray jsonArray, T... exclusions) throws JSONException {
		if (null == jsonArray || jsonArray.length() == 0)
			return null;
		Set<T> exclusionSet = null;
		if (exclusions.length > 0) {
			exclusionSet = new HashSet<>();
			for (T e : exclusions)
				exclusionSet.add(e);
		}

		// We have to know the class of the array elements. Just start with the
		// first element.
		Class<?> listElementClass = jsonArray.get(0).getClass();
		List<T> list = new ArrayList<>(jsonArray.length());
		for (int i = 0; i < jsonArray.length(); i++) {
			@SuppressWarnings("unchecked")
			T object = (T) jsonArray.get(i);
			if (JSONObject.NULL != object && (null == exclusionSet || !exclusionSet.contains(object))) {
				list.add(object);
			}

			// If the current element is not assignment compatible to its former
			// elements, two things can have happened:
			// 1) The current element is a super type of the former elements so
			// we just switch the class to the current
			// class. 2) The current and its former elements are of sibling
			// classes (can happen in JSONArrays). Then, we
			// have to use an Object array, thus Object is our class.
			if (!listElementClass.isAssignableFrom(object.getClass())) {
				if (!object.getClass().isAssignableFrom(listElementClass))
					listElementClass = Object.class;
				else
					listElementClass = object.getClass();
			}

		}
		return JulieNeo4jUtilities.convertListToArray(list, listElementClass);
	}

	public static String getString(JSONObject o, String key) throws JSONException {
		if (o.isNull(key))
			return null;
		return o.getString(key);
	}

	public static JSONObject getJSONObject(JSONObject o, String key) throws JSONException {
		if (o.isNull(key))
			return null;
		return o.getJSONObject(key);
	}

	public static JSONArray getJSONArray(JSONObject o, String key) throws JSONException {
		if (o.isNull(key))
			return null;
		return o.getJSONArray(key);
	}
	
	public static boolean getBoolean(JSONObject o, String key, boolean defaultValue) throws JSONException {
		if (o.isNull(key))
			return defaultValue;
		return o.getBoolean(key);
	}

	public static boolean getBoolean(JSONObject o, String key) throws JSONException {
		return getBoolean(o, key, false);
	}

	public static Integer getInt(JSONObject o, String key) throws JSONException {
		if (o.isNull(key))
			return null;
		return o.getInt(key);
	}

	@SuppressWarnings("unchecked")
	public static <T> Set<T> jsonArray2JavaSet(JSONArray jsonArray) throws JSONException {
		if (jsonArray == null)
			return null;
		Set<T> set = new HashSet<>();
		for (int i = 0; i < jsonArray.length(); ++i) {
			set.add((T) jsonArray.get(i));
		}
		return set;
	}

}
