package de.julielab.neo4j.plugins.datarepresentation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// The order is important for streaming. The list of concepts should come at the end so we don't need to have the
// complete data available to begin importing them
@JsonPropertyOrder({ImportConcepts.NAME_FACET, ImportConcepts.NAME_IMPORT_OPTIONS, ImportConcepts.NAME_NUM_CONCEPTS, ImportConcepts.NAME_CONCEPTS})
public class ImportConcepts {
    public static final String NAME_FACET = "facet";
    public static final String NAME_IMPORT_OPTIONS = "importOptions";
    public static final String NAME_CONCEPTS = "concepts";
    public static final String NAME_NUM_CONCEPTS = "name_num_concepts";

    @JsonProperty(NAME_CONCEPTS)
    private Stream<ImportConcept> concepts;
    @JsonProperty(NAME_FACET)
    private ImportFacet facet;
    @JsonProperty(NAME_IMPORT_OPTIONS)
    private ImportOptions importOptions;
    @JsonProperty(NAME_NUM_CONCEPTS)
    private long numConcepts = -1;
    @JsonIgnore
    private List<ImportConcept> conceptsList;

    public ImportConcepts() {
    }

    public ImportConcepts(ImportFacet facet) {
        this.facet = facet;
    }

    public ImportConcepts(Stream<ImportConcept> conceptStream, ImportFacet facet) {
        this.concepts = conceptStream;
        this.facet = facet;
    }

    public ImportConcepts(Stream<ImportConcept> termList, ImportFacet importFacet, ImportOptions importOptions) {
        this(termList, importFacet);
        this.importOptions = importOptions;
    }

    /**
     * Constructor for an already existing list of concepts. The list will directly
     * be used for {@link #getConceptsAsList()} and {@link #getConceptsAsList()};
     *
     * @param concepts    The list of concepts.
     * @param importFacet The facet to add the concepts to.
     */
    public ImportConcepts(List<ImportConcept> concepts, ImportFacet importFacet) {
        this(concepts.stream(), importFacet);
        this.conceptsList = concepts;
    }

    /**
     * Constructor for an already existing list of concepts. The list will directly
     * be used for {@link #getConceptsAsList()} and {@link #getConceptsAsList()};
     *
     * @param concepts      The list of concepts.
     * @param importFacet   The facet to add the concepts to.
     * @param importOptions Options regarding the database import of the given concepts.
     */
    public ImportConcepts(List<ImportConcept> concepts, ImportFacet importFacet, ImportOptions importOptions) {
        this(concepts.stream(), importFacet, importOptions);
        this.conceptsList = concepts;
    }

    public Stream<ImportConcept> getConcepts() {
        return concepts;
    }

    public void setConcepts(List<ImportConcept> concepts) {
        this.concepts = concepts.stream();
    }

    public void setConcepts(Stream<ImportConcept> concepts) {
        this.concepts = concepts;
    }

    /**
     * Optional specification of the number of elements returned by the concept stream. If this returns -1, the total
     * number of concepts is unknown.
     *
     * @return The total number of concepts.
     */
    public long getNumConcepts() {
        return numConcepts;
    }

    public void setNumConcepts(long numConcepts) {
        this.numConcepts = numConcepts;
    }

    /**
     * Returns the concepts as a stream. This may be the single, original stream
     * given to the constructor, or a stream derived from a underlying list in case
     * a list of concepts was passed to a constructor or
     * {@link #getConceptsAsList()} was called at least once.
     *
     * @return The concepts as a stream.
     */
    @JsonIgnore
    public List<ImportConcept> getConceptsAsList() {
        if (conceptsList == null) {
            conceptsList = concepts.collect(Collectors.toList());
            concepts = conceptsList.stream();
        }
        return conceptsList;
    }

    public ImportFacet getFacet() {
        return facet;
    }

    public void setFacet(ImportFacet facet) {
        this.facet = facet;
    }

    public ImportOptions getImportOptions() {
        return importOptions;
    }

    public void setImportOptions(ImportOptions importOptions) {
        this.importOptions = importOptions;
    }
}
