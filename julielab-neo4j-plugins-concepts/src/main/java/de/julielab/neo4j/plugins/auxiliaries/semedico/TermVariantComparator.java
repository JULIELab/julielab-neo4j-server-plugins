package de.julielab.neo4j.plugins.auxiliaries.semedico;

import java.util.Comparator;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;

/**
 * This comparator aims to judge whether two term writing variants are the same
 * or not. For this purpose, some minor normalizations are applied to the
 * compared terms such as lowercasing, space normalization and possibly more.
 * However, not too much normalization should be done since this is meant for
 * the term property 'writing variants' that is mainly used for the query term
 * dictionary. Thus, all different variants that the user could input and the
 * gazetteer cannot correctly recognize on its own should appear.
 * 
 * @author faessler
 * 
 */
public class TermVariantComparator implements Comparator<String> {

	@Override
	public int compare(String o1, String o2) {
		String o1Norm = normalizeVariant(o1);
		String o2Norm = normalizeVariant(o2);
		return o1Norm.compareTo(o2Norm);
	}

	public static String normalizeVariant(String name) {
		return StringUtils.normalizeSpace(StringUtils.lowerCase(name, Locale.ENGLISH));
	}

}
