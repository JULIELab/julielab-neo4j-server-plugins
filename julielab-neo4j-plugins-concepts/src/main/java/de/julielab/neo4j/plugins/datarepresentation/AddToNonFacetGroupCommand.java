package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AddToNonFacetGroupCommand {
	public enum ParentCriterium {
		/**
		 * If a concept has no parent at all, move it to the no-facet category.
		 */
		NO_PARENT
	}
	
//	public class FacetPropertyValueCriterium {
//		public FacetPropertyValueCriterium(String propertyKey, String propertyValue) {
//			this.propertyKey = propertyKey;
//			this.propertyValue = propertyValue;
//		}
//		public String propertyKey;
//		public String propertyValue;
//	}

	private List<ParentCriterium> parentCriteria;
//	private List<FacetPropertyValueCriterium> facetPropertyValueCriteria;

	public List<ParentCriterium> getParentCriteria() {
		return parentCriteria;
	}

	public void addParentCriterium(ParentCriterium criterium) {
		if (null == parentCriteria || parentCriteria.size() == 0)
			parentCriteria = new ArrayList<>();
		parentCriteria.add(criterium);
	}
	
//	public List<FacetPropertyValueCriterium>  getGeneralFacetPropertiesCriteria() {
//		return facetPropertyValueCriteria;
//	}
//
//	public void addGeneralFacetPropertiesCriterium(FacetPropertyValueCriterium criterium) {
//		if (null == facetPropertyValueCriteria || facetPropertyValueCriteria.size() == 0)
//			facetPropertyValueCriteria = new ArrayList<>();
//		facetPropertyValueCriteria.add(criterium);
//	}
	
	public AddToNonFacetGroupCommand() {
		parentCriteria = Collections.emptyList();
//		facetPropertyValueCriteria = Collections.emptyList();
	}
}
