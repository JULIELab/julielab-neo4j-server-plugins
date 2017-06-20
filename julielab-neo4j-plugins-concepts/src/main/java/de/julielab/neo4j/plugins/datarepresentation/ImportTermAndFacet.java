package de.julielab.neo4j.plugins.datarepresentation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.julielab.neo4j.plugins.TermManager;

public class ImportTermAndFacet {
	public ImportTermAndFacet(List<ImportTerm> termList, ImportFacet facet) {
		this.terms = termList;
		this.facet = facet;
	}

	public ImportTermAndFacet(ImportFacet facet) {
		this.facet = facet;
	}

	public ImportTermAndFacet(List<ImportTerm> termList, ImportFacet importFacet, ImportOptions importOptions) {
		this(termList, importFacet);
		this.importOptions = importOptions;
	}

	public ImportFacet facet;
	public List<ImportTerm> terms;
	public ImportOptions importOptions;

	public String toNeo4jRestRequest() {
		Map<String, String> requestMap = new HashMap<>();
		if (null != terms)
			requestMap.put(TermManager.KEY_TERMS, JsonSerializer.toJson(terms));
		if (null != facet)
			requestMap.put(TermManager.KEY_FACET, JsonSerializer.toJson(facet));
		if (null != importOptions)
			requestMap.put(TermManager.KEY_IMPORT_OPTIONS, JsonSerializer.toJson(importOptions));
		return JsonSerializer.toJson(requestMap);
	}
}
