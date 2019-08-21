package de.julielab.neo4j.plugins.datarepresentation;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ImportConcepts {
	private List<ImportConcept> concepts;

	private ImportFacet facet;

	private ImportOptions importOptions;

	public ImportConcepts(){}

	public ImportConcepts(ImportFacet facet) {
		this.facet = facet;
	}

	public ImportConcepts(Stream<ImportConcept> termList, ImportFacet facet) {
		this(termList != null ? termList.collect(Collectors.toList()) : Collections.emptyList(),  facet);
	}

	public ImportConcepts(Stream<ImportConcept> termList, ImportFacet importFacet, ImportOptions importOptions) {
		this(termList, importFacet);
		this.importOptions = importOptions;
	}


	/**
	 * Constructor for an already existing list of concepts. The list will directly
	 * be used for {@link #getConcepts()} and {@link #getConcepts()};
	 * 
	 * @param concepts
	 *            The list of concepts.
	 * @param importFacet
	 *            The facet to add the concepts to.
	 */
	public ImportConcepts(List<ImportConcept> concepts, ImportFacet importFacet) {
		this.concepts = concepts;
        this.facet = importFacet;
	}

	/**
	 * Constructor for an already existing list of concepts. The list will directly
	 * be used for {@link #getConcepts()} and {@link #getConcepts()};
	 * 
	 * @param concepts
	 *            The list of concepts.
	 * @param importFacet
	 *            The facet to add the concepts to.
	 * @param importOptions
	 *            Options regarding the database import of the given concepts.
	 */
	public ImportConcepts(List<ImportConcept> concepts, ImportFacet importFacet, ImportOptions importOptions) {
		this((Stream<ImportConcept>) null, importFacet, importOptions);
	}

	/**
	 * Returns the concepts as a stream. This may be the single, original stream
	 * given to the constructor, or a stream derived from a underlying list in case
	 * a list of concepts was passed to a constructor or
	 * {@link #getConcepts()} was called at least once.
	 * 
	 * @return The concepts as a stream.
	 */
	public List<ImportConcept> getConcepts() {
		return concepts;
	}



	public ImportFacet getFacet() {
		return facet;
	}

	public ImportOptions getImportOptions() {
		return importOptions;
	}

	public void setConcepts(List<ImportConcept> concepts) {
		this.concepts = concepts;
	}

	public void setFacet(ImportFacet facet) {
		this.facet = facet;
	}

	public void setImportOptions(ImportOptions importOptions) {
		this.importOptions = importOptions;
	}
}
