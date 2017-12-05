package de.julielab.neo4j.plugins.datarepresentation;

import java.util.List;

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
}
