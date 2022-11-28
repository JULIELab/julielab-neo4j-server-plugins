package de.julielab.neo4j.plugins.auxiliaries;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Supplier;

public class PropertyUtilities {

    public static void copyObjectToEntity(Object object, Entity node) {
        copyObjectToEntity(object, node, (Set<String>) null);
    }

    public static void copyObjectToEntity(Object object, Entity node,
                                          String... exclusions) {
        Set<String> exclusionSet = new HashSet<>();
        for (String exclusion : exclusions) {
            exclusionSet.add(exclusion);
        }
        copyObjectToEntity(object, node, exclusionSet);
    }

    public static void copyObjectToEntity(Object object, Entity node,
                                          Set<String> exclusions) {

        try {
            final Field[] fields = object.getClass().getDeclaredFields();
            for (Field field : fields) {
                String key = field.getName();
                // Check if the field is annotated to give it another name
                final JsonProperty[] jsonProperties = field.getAnnotationsByType(JsonProperty.class);
                if (jsonProperties != null && jsonProperties[0].value() != null) {
                    key = jsonProperties[0].value();
                }
                if ((Modifier.PUBLIC & field.getModifiers()) != 0 && (exclusions == null || !exclusions.contains(key))) {
                    Object value = field.get(object);
                    if (value != null) {
                        if (List.class.isAssignableFrom(value.getClass())) {
                            value = ((List<?>) value).toArray();
                        }
                        node.setProperty(key, value);
                    } else {
                        node.removeProperty(key);
                    }
                }
            }
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static void mergeObjectIntoEntity(Object object, Entity node,
                                             String... exclusions) {
        Set<String> exclusionSet = new HashSet<>();
        for (String exclusion : exclusions) {
            exclusionSet.add(exclusion);
        }
        mergeObjectIntoEntity(object, node, exclusionSet);
    }

    public static void mergeObjectIntoEntity(Object object, Entity node,
                                             Set<String> exclusions) {
        try {
            final Field[] fields = object.getClass().getFields();
            for (Field field : fields) {
                String key = field.getName();
                // Check if the field is annotated to give it another name
                final JsonProperty[] jsonProperties = field.getAnnotationsByType(JsonProperty.class);
                if (jsonProperties != null && jsonProperties[0].value() != null) {
                    key = jsonProperties[0].value();
                }
                if ((Modifier.PUBLIC & field.getModifiers()) != 0 && (exclusions == null || !exclusions.contains(key))) {
                    final Object value = field.get(object);
                    if (value != null) {
                        if (value.getClass().isArray()) {
                            mergeArrayProperty(node, key, (Object[]) value, true);
                        } else if (List.class.isAssignableFrom(value.getClass())) {
                            mergeArrayProperty(node, key, ((List<?>) value).toArray(), true);
                        } else {
                            setNonNullNodeProperty(node, key, value);
                        }
                    }
                }
            }
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static void mergeEntityIntoEntity(Node from, Node to, String... exclusions) {
        Set<String> exclusionSet = new HashSet<>();
        for (String exclusion : exclusions) {
            exclusionSet.add(exclusion);
        }
        mergeJSONObjectIntoEntity(from, to, exclusionSet);
    }

    private static void mergeJSONObjectIntoEntity(Node from, Node to, Set<String> exclusions) {
        for (String key : from.getPropertyKeys()) {
            if (null != exclusions && exclusions.contains(key))
                continue;
            Object value = from.getProperty(key);
            if (value.getClass().isArray()) {
                mergeArrayProperty(to, key, (Object[]) value, true);
            } else {
                setNonNullNodeProperty(to, key, value);
            }
        }
    }

    public static <T> void mergeArrayProperty(Entity node, String key, Supplier<T[]> arraySupplier) {
        try {
            final T[] value = arraySupplier.get();
            mergeArrayProperty(node, key, value, true);
        } catch (NullPointerException e) {
            // nothing, this was expected
        }
    }

    /**
     * Gets the current property values at <tt>key</tt> for the <tt>node</tt>
     * <tt>node</tt> and adds them all into a single <tt>Set</tt> together with
     * the contents of <tt>array</tt> to avoid duplicates. Then, the
     * <tt>Set</tt> is converted back into an array which is then set as the new
     * node property. A {@link LinkedHashSet} is used to maintain the original
     * order. The original values are added first. Thus, new values are appended
     * to the end of the new array.
     *
     * @param node
     * @param key
     * @param array
     * @param setMergedValueToNode
     */
    @SuppressWarnings("unchecked")
    public static <T> void mergeArrayProperty(Entity node, String key, T[] array, boolean setMergedValueToNode) {
        try {
            if (null == array || array.length == 0)
                return;

            if (array[0] == null)
                throw new IllegalArgumentException("An array was passed whose first element is null. This is not allowed.");

            // we can't directly cast to T[] because sometimes we will get an
            // Integer[] but the node stored an int[]
            // which is not castable...
            // Use the set to avoid duplicates.
            Set<T> set = new LinkedHashSet<>();
            for (T o : array)
                set.add(o);

            if (node.hasProperty(key)) {
                Object storedArray = node.getProperty(key);
                // casting the elements one-by-one is working because int can be
                // cast to Integer and the other way round via
                // autoboxing
                for (int i = 0; i < Array.getLength(storedArray); ++i)
                    set.add((T) Array.get(storedArray, i));
            }
            T[] newArray = (T[]) Array.newInstance(array[0].getClass(), set.size());
            try {
                set.toArray(newArray);
            } catch (ArrayStoreException e) {
                T next = set.iterator().next();
                throw new IllegalArgumentException("Trying to merge array properties of different types (array class: "
                        + newArray.getClass() + " element class: " + next.getClass() + ")");
            }
            if (setMergedValueToNode)
                node.setProperty(key, newArray);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception occurred while merging property \"" + key + "\" of property container \"" + node
                            + "\" (value before merging: \"" + (node.hasProperty(key) ? node.getProperty(key) : null)
                            + ")\" with data \"" + Arrays.toString(array) + "\" (object notation: \"" + array + "\").",
                    e);
        }
    }

    /**
     * Adds <tt>value</tt> to the property with name <tt>key</tt> but does not
     * allow duplicates or null values.
     *
     * @param node
     * @param key
     * @param value
     * @return
     */
    public static <T> int addToArrayProperty(Entity node, String key, T value) {
        return addToArrayProperty(node, key, value, false, false);
    }

    public static <T> int addToArrayProperty(Entity node, String key, T value, boolean allowDuplicates) {
        return addToArrayProperty(node, key, value, allowDuplicates, false);
    }

    @SuppressWarnings("unchecked")
    public static <T> int addToArrayProperty(Entity node, String key, T value, boolean allowDuplicates,
                                             boolean allowNullValues) {
        if (!allowNullValues && null == value)
            return -1;
        T[] array = null;
        if (node.hasProperty(key)) {
            Object arrayProperty = node.getProperty(key);
            try {
                array = (T[]) arrayProperty;
            } catch (ClassCastException e) {
                // happens when the existing array on node is of a primitive array type, e.g. byte[], since then it will be tried to cast to Byte[] which does not work
                array = (T[]) JulieNeo4jUtilities.convertArray(arrayProperty);
            }
        } else {
            T[] newArray = (T[]) Array.newInstance(value.getClass(), 1);
            newArray[0] = value;
            node.setProperty(key, newArray);
            return 0;
        }

        boolean valueFound = false;
        if (!allowDuplicates) {
            for (T element : array) {
                if (element.equals(value)) {
                    valueFound = true;
                    break;
                }
            }
        }
        if (!valueFound || allowDuplicates) {
            T[] newArray = (T[]) Array.newInstance(value.getClass(), array.length + 1);
            System.arraycopy(array, 0, newArray, 0, array.length);
            newArray[newArray.length - 1] = value;
            node.setProperty(key, newArray);
            return newArray.length - 1;
        }
        return -1;
    }

    public static void setNonNullNodeProperty(Entity node, String key, Supplier<Object> value) {
        try {
            final Object nonNulValue = value.get();
            setNonNullNodeProperty(node, key, nonNulValue);
        } catch (NullPointerException e) {
            // nothing, was expected
        }
    }

    /**
     * Sets the property at property key <tt>key</tt> of <tt>node</tt> to
     * <tt>value</tt>, if value is not null and the property on the node not
     * already set.
     *
     * @param node
     * @param key
     * @param value
     */
    public static void setNonNullNodeProperty(Entity node, String key, Object value) {
        if (null == value)
            return;
        if (node.hasProperty(key))
            return;
        node.setProperty(key, value);
    }

    /**
     * Sets the property at property key <tt>key</tt> of <tt>node</tt> to
     * <tt>value</tt>, if value is not null and the property on the node not
     * already set. If <tt>value</tt> is null, the property is set to
     * <tt>defaultValue</tt>.
     *
     * @param node
     * @param key
     * @param value
     */
    public static void setNonNullNodeProperty(Entity node, String key, Object value, Object defaultValue) {
        if (node.hasProperty(key))
            return;
        if (null != value)
            node.setProperty(key, value);
        else
            node.setProperty(key, defaultValue);
    }

    /**
     * Returns the property stored at key <tt>key</tt> in the specified property
     * container or null.
     *
     * @param node
     * @param key
     * @return
     */
    public static Object getNonNullNodeProperty(Entity node, String key) {
        if (null != node && node.hasProperty(key))
            return node.getProperty(key);
        return null;
    }

    public static String getNodePropertiesAsString(Entity node) {
        List<String> values = new ArrayList<>();
        for (String pk : node.getPropertyKeys()) {
            Object p = node.getProperty(pk);
            if (p.getClass().isArray())
                values.add(pk + ": " + Arrays.toString(JulieNeo4jUtilities.convertArray(p)));
            else
                values.add(pk + ": " + p);
        }
        return StringUtils.join(values, " ; ");
    }

    public static int findFirstValueInArrayProperty(Node term, String key, String value) {
        if (!term.hasProperty(key)) {
            return -1;
        }
        Object property = term.getProperty(key);
        Object[] array = JulieNeo4jUtilities.convertArray(property);
        for (int i = 0; i < array.length; i++) {
            if (null != array[i] && array[i].equals(value))
                return i;
            else if (null == array[i] && null == value)
                return i;
        }

        return -1;

    }

    /**
     * Checks whether the property container has the exact
     * <tt>valueToCompare</tt> as value for the property identified by
     * <tt>key</tt>.
     *
     * @param term
     * @param key
     * @param valueToCompare
     * @return
     */
    public static boolean hasSamePropertyValue(Entity term, String key, Object valueToCompare) {
        if (null != valueToCompare && !term.hasProperty(key))
            return false;
        Object value = term.getProperty(key);
        if (!valueToCompare.equals(value)) {
            return false;
        }
        return true;
    }

    /**
     * Determines whether the property container has a value for the property
     * with key <tt>key</tt> and this value does not equal
     * <tt>valueToCompare</tt>. I.e. if the property value is <tt>null</tt>,
     * this is not seen as a contradiction - not even if <tt>term</tt> of
     * <tt>valueToCompare</tt> itself is null.
     *
     * @param term
     * @param key
     * @param valueToCompare
     * @return
     */
    public static boolean hasContradictingPropertyValue(Entity term, String key, Object valueToCompare) {
        if (null == term || null == valueToCompare || !term.hasProperty(key))
            return false;
        Object value = term.getProperty(key);
        if (!valueToCompare.equals(value)) {
            return true;
        }
        return false;
    }

    public static boolean mergeProperties(Entity propContainer, Object... properties) {
        boolean success = true;
        // Type and direction match.
        // Now check whether properties fit.
        if (null != properties && properties.length > 0) {
            for (int i = 0; i < properties.length; i += 2) {
                String key = (String) properties[i];
                Object value = properties[i + 1];
                boolean passedValueIsArray = value.getClass().isArray();
                if (propContainer.hasProperty(key)) {
                    Object existingValue = propContainer.getProperty(key);
                    boolean existingValueIsArray = existingValue.getClass().isArray();
                    // The property must have the correct value
                    if (!existingValueIsArray && !value.equals(existingValue)) {
                        success = false;
                    } else {
                        // If we have two array-valued values, we
                        // may merge them
                        if (existingValueIsArray != passedValueIsArray)
                            throw new IllegalArgumentException(
                                    "Trying to merge an array value with a non-array value.");
                        if (existingValueIsArray) {
                            PropertyUtilities.mergeArrayProperty(propContainer, key, (Object[]) value, true);
                        }
                    }
                } else {
                    propContainer.setProperty(key, value);
                }
            }
        }
        return success;
    }
}
