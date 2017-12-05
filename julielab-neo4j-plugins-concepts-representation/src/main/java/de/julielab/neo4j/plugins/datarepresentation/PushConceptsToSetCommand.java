package de.julielab.neo4j.plugins.datarepresentation;


/**
 * A convenience class for the usage of the "push all concepts to set" endpoint of the <code>FacetConceptManager</code>. The
 * fields of this class are modeled after the required parameters for the endpoint. Thus, this class may be used to
 * declare the desired command and then be serialized as-is for the correct output format, e.g. by using
 * <code>Gson</code>.
 * 
 * @author faessler
 * 
 */
public class PushConceptsToSetCommand {
	/**
	 * The concept label defining the set to push concepts in. Must be defined as a node label in this server plugin.
	 */
	public String setName;

	public ConceptSelectionDefinition eligibleConceptDefinition;

	public ConceptSelectionDefinition excludeConceptDefinition;

	public PushConceptsToSetCommand(String setName) {
		this.setName = setName;
	}
	
	public PushConceptsToSetCommand() {
		
	}


	public class ConceptSelectionDefinition {
		public String facetPropertyKey;
		public String facetPropertyValue;
		public String facetLabel;
		public String conceptPropertyKey;
		public String conceptPropertyValue;
		public String conceptLabel;

		public ConceptSelectionDefinition(String facetPropertyKey, String facetPropertyValue) {
			this.facetPropertyKey = facetPropertyKey;
			this.facetPropertyValue = facetPropertyValue;
		}

		public ConceptSelectionDefinition(String conceptLabel, String conceptPropertyKey, String conceptPropertyValue) {
			this.conceptLabel = conceptLabel;
			this.conceptPropertyKey = conceptPropertyKey;
			this.conceptPropertyValue = conceptPropertyValue;
		}
		public ConceptSelectionDefinition(String facetPropertyKey, String facetPropertyValue, String conceptPropertyKey,
				String conceptPropertyValue) {
					this.facetPropertyKey = facetPropertyKey;
					this.facetPropertyValue = facetPropertyValue;
					this.conceptPropertyKey = conceptPropertyKey;
					this.conceptPropertyValue = conceptPropertyValue;
		}
		
		public ConceptSelectionDefinition() {}


	}

}
