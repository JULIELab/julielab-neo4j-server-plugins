package de.julielab.neo4j.plugins.auxiliaries;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
public class PropertyUtilitiesTest {
    @Test
    public void mergeArrayValue() {
        int[] intArray = new int[]{1, 2};
        Integer[] integerArray = new Integer[]{Integer.valueOf(2), Integer.valueOf(3)};
        final Integer[] integers = PropertyUtilities.mergeArrayValue(intArray, integerArray);
        assertThat(integers).containsExactly(1, 2, 3);
    }

    @Test
    public void mergeArrayValueWithNullArray() {
        int[] intArray = new int[]{1, 2};
        assertThatIllegalArgumentException().isThrownBy(() -> PropertyUtilities.mergeArrayValue(intArray, null));
    }

    @Test
    public void mergeIntegerArrayValueWithNullArray() {
        Integer[] integerArray = new Integer[]{Integer.valueOf(2), Integer.valueOf(3)};
        final Integer[] integers = PropertyUtilities.mergeArrayValue(integerArray, null);
        assertThat(integers).containsExactly(2, 3);
    }

    @Test
    public void mergeStringArrayValueWithNullArray() {
        String[] strArray = new String[]{"kaese", "rolltreppe"};
        final String[] strings = PropertyUtilities.mergeArrayValue(strArray, null);
        assertThat(strings).containsExactly("kaese", "rolltreppe");
    }
}
