package de.julielab.neo4j.plugins.auxiliaries.semedico;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This comparator can be used to sort nodes according to their similarity in preferred name and synonyms. The
 * comparator uses a {@link RuleBasedCollator} to remove differences due to case or accents.
 * 
 * @author faessler
 * @deprecated Not used anymore. Was used to build same-name aggregates in de.julielab.neo4j.plugins.concepts.ConceptAggregateManager#buildAggregatesForEqualNames(org.neo4j.graphdb.Transaction, java.lang.String, java.lang.String[])
 */
@Deprecated
public class TermNameAndSynonymComparator implements Comparator<Node> {
	private final RuleBasedCollator collator = createComparisonCollator();

	@Override
	public int compare(Node o1, Node o2) {
		Object o1Name = o1.getProperty(ConceptConstants.PROP_PREF_NAME);
		Object o2Name = o2.getProperty(ConceptConstants.PROP_PREF_NAME);
		int prefLabelComp = collator.compare(o1Name, o2Name);
		if (prefLabelComp != 0)
			return prefLabelComp;
		String o1Syns = getSortedAndNormalizedSynonyms(o1);
		String o2Syns = getSortedAndNormalizedSynonyms(o2);

		return o1Syns != null && o2Syns != null ? o1Syns.compareTo(o2Syns) : prefLabelComp;
	}

	private String getSortedAndNormalizedSynonyms(Node n) {
		if (n.hasProperty(ConceptConstants.PROP_SYNONYMS)) {
			String[] synonyms = (String[]) n.getProperty(ConceptConstants.PROP_SYNONYMS);
			List<String> synonymsList = Arrays.asList(synonyms);
			Collections.sort(synonymsList);
			return StringUtils.join(synonymsList, "");
		}
		return null;
	}

	private static RuleBasedCollator createComparisonCollator() {
		try {
			// default rules
			RuleBasedCollator collator = new RuleBasedCollator("");
			// only primary differences matter, i.e. we don't care for differences in case or accents.
			collator.setStrength(Collator.PRIMARY);
			collator.freeze();
			return collator;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
