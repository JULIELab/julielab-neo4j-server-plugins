package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ImportConcepts {
	private Stream<ImportConcept> concepts;
	private List<ImportConcept> conceptsAsList;

	private ImportFacet facet;

	private ImportOptions importOptions;

	public ImportConcepts(ImportFacet facet) {
		this.facet = facet;
	}

	public ImportConcepts(Stream<ImportConcept> termList, ImportFacet facet) {
		this.concepts = termList;
		this.facet = facet;
	}

	public ImportConcepts(Stream<ImportConcept> termList, ImportFacet importFacet, ImportOptions importOptions) {
		this(termList, importFacet);
		this.importOptions = importOptions;
	}

	/**
	 * Constructor for an already existing list of concepts. The list will directly
	 * be used for {@link #getConcepts()} and {@link #getConceptsAsList()};
	 * 
	 * @param concepts
	 *            The list of concepts.
	 * @param importFacet
	 *            The facet to add the concepts to.
	 */
	public ImportConcepts(List<ImportConcept> concepts, ImportFacet importFacet) {
		this((Stream<ImportConcept>) null, importFacet);
		this.conceptsAsList = concepts;
	}

	/**
	 * Constructor for an already existing list of concepts. The list will directly
	 * be used for {@link #getConcepts()} and {@link #getConceptsAsList()};
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
		this.conceptsAsList = concepts;
	}

	/**
	 * Returns the concepts as a stream. This may be the single, original stream
	 * given to the constructor, or a stream derived from a underlying list in case
	 * a list of concepts was passed to a constructor or
	 * {@link #getConceptsAsList()} was called at least once.
	 * 
	 * @return The concepts as a stream.
	 */
	public Stream<ImportConcept> getConcepts() {
		if (conceptsAsList != null)
			return conceptsAsList.stream();
		return concepts;
	}

	/**
	 * <p>
	 * If index-based access to the concepts is required, this method can be called.
	 * If one of the constructors was called taking a stream of ImportConcepts, this
	 * causes the concept stream to be collected into a list. Starting from the
	 * first call of this method, only the list representation will then be used.
	 * Even {@link #getConcepts()} will return a new stream created from the list.
	 * Thus, this method comes with a - possibly minor - performance penalty.
	 * </p>
	 * <p>
	 * In case that one of the constructors taking a list of concepts was used, this
	 * is the method to go.
	 * </p>
	 * 
	 * @return The concepts as list.
	 */
	@JsonIgnore
	public List<ImportConcept> getConceptsAsList() {
		if (conceptsAsList == null) {
			conceptsAsList = concepts.collect(Collectors.toCollection(ArrayList::new));
		}
		return conceptsAsList;
	}

	public ImportFacet getFacet() {
		return facet;
	}

	public ImportOptions getImportOptions() {
		return importOptions;
	}

	public void setConcepts(Stream<ImportConcept> concepts) {
		this.concepts = concepts;
	}

	public void setFacet(ImportFacet facet) {
		this.facet = facet;
	}

	public void setImportOptions(ImportOptions importOptions) {
		this.importOptions = importOptions;
	}
}
