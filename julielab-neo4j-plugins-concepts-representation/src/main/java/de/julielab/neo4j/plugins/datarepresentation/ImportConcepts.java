package de.julielab.neo4j.plugins.datarepresentation;

import java.util.List;

public class ImportConcepts {
	public ImportConcepts(List<ImportConcept> termList, ImportFacet facet) {
		this.concepts = termList;
		this.facet = facet;
	}

	public ImportConcepts(ImportFacet facet) {
		this.facet = facet;
	}

	public ImportConcepts(List<ImportConcept> termList, ImportFacet importFacet, ImportOptions importOptions) {
		this(termList, importFacet);
		this.importOptions = importOptions;
	}

	public ImportFacet facet;
	public List<ImportConcept> concepts;
	public ImportOptions importOptions;
}
