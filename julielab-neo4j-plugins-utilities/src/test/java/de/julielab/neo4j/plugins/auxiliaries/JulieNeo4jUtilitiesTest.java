package de.julielab.neo4j.plugins.auxiliaries;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JulieNeo4jUtilitiesTest {
	
	@Test
	public void testFindFirstNullInArray1() {
		String[] s = {"<null>" };
		assertEquals(0, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}

	@Test
	public void testFindFirstNullInArray2() {
		String[] s = { "" };
		assertEquals(-1, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}

	@Test
	public void testFindFirstNullInArray3() {
		String[] s = { "","<null>","<null>" };
		assertEquals(1, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}

	@Test
	public void testFindFirstNullInArray4() {
		String[] s = { "", "","<null>","<null>" };
		assertEquals(2, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}

	@Test
	public void testFindFirstNullInArray5() {
		String[] s = { "", "", "","<null>","<null>" };
		assertEquals(3, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}

	@Test
	public void testFindFirstNullInArray6() {
		String[] s = { "", "", "", "","<null>","<null>" };
		assertEquals(4, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}

	@Test
	public void testFindFirstNullInArray7() {
		String[] s = { "", "", "", "", "", "", "", "","<null>","<null>" };
		assertEquals(8, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}

	@Test
	public void testFindFirstNullInArray8() {
		String[] s = { "", "", "", "", "", "", "", "","<null>","<null>","<null>" };
		assertEquals(8, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}
	
	@Test
	public void testFindFirstNullInArray9() {
		String[] s = { "", "", "", "", "", "", "", "", "", "","<null>" };
		assertEquals(10, JulieNeo4jUtilities.findFirstValueInArray(s,"<null>"));
	}
}
