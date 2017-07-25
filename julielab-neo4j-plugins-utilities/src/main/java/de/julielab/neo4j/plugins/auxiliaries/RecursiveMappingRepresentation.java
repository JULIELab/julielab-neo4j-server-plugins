package de.julielab.neo4j.plugins.auxiliaries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ValueRepresentation;

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class RecursiveMappingRepresentation extends MappingRepresentation {

	private Map<String, Object> map;

	public RecursiveMappingRepresentation(String type, Map<String, Object> map) {
		super(type);
		this.map = map;
	}

	protected RecursiveMappingRepresentation(String type) {
		super(type);
	}

	@Override
	protected void serialize(final MappingSerializer serializer) {
		for (Map.Entry<String, Object> pair : map.entrySet()) {
			serialize(pair.getKey(), pair.getValue(), serializer);
		}
	}

	/**
	 * Returns the underlying Java map object. This instance is always a
	 * <tt>Map&lt;String,?&gt;</tt>.
	 * 
	 * @return The underlying Java map object.
	 */
	public Map<String, Object> getUnderlyingMap() {
		return map;
	}

	protected void setUnderlyingMap(Map<String, Object> map) {
		if (null != this.map)
			throw new IllegalAccessError("The underlying map must only be set once.");
		this.map = map;
	}

	@SuppressWarnings("unchecked")
	protected void serialize(String key, Object value, MappingSerializer serializer) {
		if (key == null || key.length() == 0 || value == null)
			return;
		Class<? extends Object> valueClass = value.getClass();
		if (Map.class.isAssignableFrom(valueClass)) {
			MappingRepresentation matchRep = new RecursiveMappingRepresentation(Representation.MAP,
					(Map<String, Object>) value);
			serializer.putMapping(key, matchRep);
		} else if (valueClass.equals(String.class)) {
			serializer.putString(key, (String) value);
		} else if (valueClass.isArray()) {
			Class<?> componentType = valueClass.getComponentType();
			if (componentType.equals(int.class)) {
				int[] array = (int[]) value;
				serializer.putList(key, getIterableRepresentation(Ints.asList(array)));
			} else if (componentType.equals(long.class)) {
				long[] array = (long[]) value;
				serializer.putList(key, getIterableRepresentation(Longs.asList(array)));
			} else if (componentType.equals(double.class)) {
				double[] array = (double[]) value;
				serializer.putList(key, getIterableRepresentation(Doubles.asList(array)));
			} else if (componentType.equals(boolean.class)) {
				boolean[] array = (boolean[]) value;
				serializer.putList(key, getIterableRepresentation(Booleans.asList(array)));
			} else {
				Object[] array = (Object[]) value;
				serializer.putList(key, getIterableRepresentation(Arrays.asList(array)));
			}
		} else if (List.class.isAssignableFrom(valueClass)) {
			serializer.putList(key, getIterableRepresentation((Iterable<?>) value));
		} else if (Set.class.isAssignableFrom(valueClass)) {
			serializer.putList(key, getIterableRepresentation((Iterable<?>) value));
		} else if (valueClass.equals(Boolean.class)) {
			serializer.putBoolean(key, (Boolean) value);
		} else if (Number.class.isAssignableFrom(valueClass)) {
			serializer.putNumber(key, (Number) value);
		} else if (Node.class.isAssignableFrom(valueClass)) {
			MappingRepresentation nodeMap = new NodeRepresentation((Node) value);
			serializer.putMapping(key, nodeMap);
		} else {
			throw new IllegalArgumentException(
					"Encountered " + valueClass + ". This class is currently not supported.");
		}
	}

	@SuppressWarnings("unchecked")
	public static Representation getObjectRepresentation(Object value) {
		Representation rep = null;
		Class<?> valueClass = value.getClass();
		if (Map.class.isAssignableFrom(valueClass)) {
			rep = new RecursiveMappingRepresentation(Representation.MAP, (Map<String, Object>) value);
		} else if (valueClass.equals(String.class)) {
			rep = ValueRepresentation.string((String) value);
		} else if (valueClass.isArray()) {
			rep = getArrayRepresentation(value);
		} else if (Iterable.class.isAssignableFrom(valueClass)) {
			rep = getIterableRepresentation((Iterable<?>) value);
		} else if (valueClass.equals(Boolean.class)) {
			rep = ValueRepresentation.bool((Boolean) value);
		} else if (valueClass.equals(String.class)) {
			rep = ValueRepresentation.string((String) value);
		} else if (Number.class.isAssignableFrom(valueClass)) {
			rep = getNumberRepresentation((Number) value, valueClass);
		} else if (Node.class.isAssignableFrom(valueClass)) {
			rep = new NodeRepresentation((Node) value);
		} else {
			throw new IllegalArgumentException(
					"Encountered " + valueClass + ". This class is currently not supported.");
		}
		return rep;
	}

	private static ListRepresentation getArrayRepresentation(Object arrayObject) {
		ListRepresentation rep;
		List<Representation> repList;
		Class<?> arrayClass = arrayObject.getClass();
		if (arrayClass.equals(int[].class)) {
			int[] array = (int[]) arrayObject;
			repList = new ArrayList<Representation>();
			for (int i = 0; i < array.length; i++)
				repList.add(getObjectRepresentation(array[i]));
		} else if (arrayClass.equals(byte[].class)) {
			byte[] array = (byte[]) arrayObject;
			repList = new ArrayList<Representation>();
			for (int i = 0; i < array.length; i++)
				repList.add(getObjectRepresentation(array[i]));
		} else if (arrayClass.equals(short[].class)) {
			short[] array = (short[]) arrayObject;
			repList = new ArrayList<Representation>();
			for (int i = 0; i < array.length; i++)
				repList.add(getObjectRepresentation(array[i]));
		} else if (arrayClass.equals(double[].class)) {
			double[] array = (double[]) arrayObject;
			repList = new ArrayList<Representation>();
			for (int i = 0; i < array.length; i++)
				repList.add(getObjectRepresentation(array[i]));
		} else if (arrayClass.equals(float[].class)) {
			float[] array = (float[]) arrayObject;
			repList = new ArrayList<Representation>();
			for (int i = 0; i < array.length; i++)
				repList.add(getObjectRepresentation(array[i]));
		} else {
			Object[] array = (Object[]) arrayObject;
			repList = new ArrayList<Representation>();
			for (int i = 0; i < array.length; i++)
				repList.add(getObjectRepresentation(array[i]));
		}
		// I have absolutely no idea what this "type" should be good for.
		rep = new ListRepresentation(RepresentationType.TEMPLATE, repList);
		return rep;
	}

	public static ListRepresentation getIterableRepresentation(Iterable<?> value) {
		ListRepresentation rep;
		List<Representation> repList = new ArrayList<Representation>();
		Iterable<?> iterable = (Iterable<?>) value;
		for (Object o : iterable)
			repList.add(getObjectRepresentation(o));
		// I have absolutely no idea what this "type" should be good for.
		rep = new ListRepresentation(RepresentationType.TEMPLATE, repList);
		return rep;
	}

	public static <T extends Number> Representation getNumberRepresentation(T value, Class<?> valueClass) {
		Representation rep = null;
		if (valueClass.equals(int.class) || valueClass.equals(Integer.class))
			rep = ValueRepresentation.number((Integer) value);
		else if (valueClass.equals(double.class) || valueClass.equals(Double.class))
			rep = ValueRepresentation.number((Double) value);
		else if (valueClass.equals(long.class) || valueClass.equals(Long.class))
			rep = ValueRepresentation.number((Long) value);
		else if (valueClass.equals(float.class) || valueClass.equals(Float.class))
			rep = ValueRepresentation.number((Float) value);
		else if (valueClass.equals(short.class) || valueClass.equals(Short.class))
			rep = ValueRepresentation.number((Short) value);
		else if (valueClass.equals(byte.class) || valueClass.equals(Byte.class))
			rep = ValueRepresentation.number((Byte) value);
		else
			throw new IllegalArgumentException(
					"Encountered " + valueClass + ". This class is currently not supported.");

		return rep;
	}
}
