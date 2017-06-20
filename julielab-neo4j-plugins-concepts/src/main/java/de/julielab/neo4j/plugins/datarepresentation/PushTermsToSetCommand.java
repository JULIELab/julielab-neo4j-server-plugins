package de.julielab.neo4j.plugins.datarepresentation;


/**
 * A convenience class for the usage of the "push all terms to set" endpoint of the <code>FacetTermManager</code>. The
 * fields of this class are modeled after the required parameters for the endpoint. Thus, this class may be used to
 * declare the desired command and then be serialized as-is for the correct output format, e.g. by using
 * <code>Gson</code>.
 * 
 * @author faessler
 * 
 */
public class PushTermsToSetCommand {
	/**
	 * The term label defining the set to push terms in. Must be defined as a node label in this server plugin.
	 */
	public String setName;

	public TermSelectionDefinition eligibleTermDefinition;

	public TermSelectionDefinition excludeTermDefinition;

//	transient Gson gson = new Gson();
//
//	public String toJson() {
//		return gson.toJson(this);
//	}

	public PushTermsToSetCommand(String setName) {
		this.setName = setName;
	}
	
	public PushTermsToSetCommand() {
		
	}


	public class TermSelectionDefinition {
		public String facetPropertyKey;
		public String facetPropertyValue;
		public String facetLabel;
		public String termPropertyKey;
		public String termPropertyValue;
		public String termLabel;

		public TermSelectionDefinition(String facetPropertyKey, String facetPropertyValue) {
			this.facetPropertyKey = facetPropertyKey;
			this.facetPropertyValue = facetPropertyValue;
		}

		public TermSelectionDefinition(String termLabel, String termPropertyKey, String termPropertyValue) {
			this.termLabel = termLabel;
			this.termPropertyKey = termPropertyKey;
			this.termPropertyValue = termPropertyValue;
		}
		public TermSelectionDefinition(String facetPropertyKey, String facetPropertyValue, String termPropertyKey,
				String termPropertyValue) {
					this.facetPropertyKey = facetPropertyKey;
					this.facetPropertyValue = facetPropertyValue;
					this.termPropertyKey = termPropertyKey;
					this.termPropertyValue = termPropertyValue;
		}
		
		public TermSelectionDefinition() {}


	}

}
