package de.julielab.neo4j.plugins.datarepresentation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.julielab.neo4j.plugins.ConceptManager;

public class ImportConceptAndFacet {
	public ImportConceptAndFacet(List<ImportConcept> termList, ImportFacet facet) {
		this.terms = termList;
		this.facet = facet;
	}

	public ImportConceptAndFacet(ImportFacet facet) {
		this.facet = facet;
	}

	public ImportConceptAndFacet(List<ImportConcept> termList, ImportFacet importFacet, ImportOptions importOptions) {
		this(termList, importFacet);
		this.importOptions = importOptions;
	}

	public ImportFacet facet;
	public List<ImportConcept> terms;
	public ImportOptions importOptions;

	public String toNeo4jRestRequest() {
		Map<String, String> requestMap = new HashMap<>();
		if (null != terms)
			requestMap.put(ConceptManager.KEY_TERMS, JsonSerializer.toJson(terms));
		if (null != facet)
			requestMap.put(ConceptManager.KEY_FACET, JsonSerializer.toJson(facet));
		if (null != importOptions)
			requestMap.put(ConceptManager.KEY_IMPORT_OPTIONS, JsonSerializer.toJson(importOptions));
		return JsonSerializer.toJson(requestMap);
	}
}
