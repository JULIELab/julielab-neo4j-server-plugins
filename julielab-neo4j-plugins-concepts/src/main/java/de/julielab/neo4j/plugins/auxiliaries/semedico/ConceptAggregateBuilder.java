package de.julielab.neo4j.plugins.auxiliaries.semedico;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.ConceptManager.ConceptLabel;
import de.julielab.neo4j.plugins.ConceptManager.EdgeTypes;
import de.julielab.neo4j.plugins.auxiliaries.JulieNeo4jUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.AggregateConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptRelationConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.NodeIDPrefixConstants;
import org.neo4j.graphdb.*;

import java.util.*;
import java.util.logging.Logger;

import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.mergeArrayProperty;
import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.setNonNullNodeProperty;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;

public class ConceptAggregateBuilder {

    private static final Logger log = Logger.getLogger(ConceptAggregateBuilder.class.getName());

    /**
     * Aggregates terms that have equal preferred name and synonyms, after some
     * minor normalization.
     *
     * @param tx              The graph database to work on.
     * @param termPropertyKey actually not clear right now ;-)
     * @param propertyValues  The properties to merge for aggregated nodes.
     */
    public static void buildAggregatesForEqualNames(Transaction tx, String termPropertyKey,
                                                    String[] propertyValues) {
        TermNameAndSynonymComparator nameAndSynonymComparator = new TermNameAndSynonymComparator();
        // At first, delete all equal-name aggregates since they will be
        // built again afterwards.
        ResourceIterable<Node> aggregates = () -> tx.findNodes(ConceptManager.ConceptLabel.AGGREGATE_EQUAL_NAMES);
        for (Node aggregate : aggregates) {
            for (Relationship rel : aggregate.getRelationships())
                rel.delete();
            aggregate.delete();
        }

        // Get all terms and sort them by name and synonyms
        ResourceIterable<Node> termIterable = () -> tx.findNodes(ConceptManager.ConceptLabel.CONCEPT);
        List<Node> terms = new ArrayList<>();
        for (Node term : termIterable) {
            terms.add(term);
        }
        terms.sort(nameAndSynonymComparator);

        String[] copyProperties = new String[]{ConceptConstants.PROP_PREF_NAME, ConceptConstants.PROP_SYNONYMS,
                ConceptConstants.PROP_DESCRIPTIONS};
        List<Node> equalNameTerms = new ArrayList<>();
        for (Node term : terms) {
            boolean equalTerm = 0 == equalNameTerms.size()
                    || 0 == nameAndSynonymComparator.compare(equalNameTerms.get(equalNameTerms.size() - 1), term);
            if (equalTerm) {
                equalNameTerms.add(term);
            } else if (equalNameTerms.size() > 1) {
                createAggregate(tx, copyProperties, new HashSet<>(equalNameTerms),
                        new String[]{ConceptManager.ConceptLabel.AGGREGATE_EQUAL_NAMES.toString()},
                        ConceptManager.ConceptLabel.AGGREGATE_EQUAL_NAMES);
                for (Node equalNameTerm : equalNameTerms)
                    NodeUtilities.mergeArrayProperty(equalNameTerm, termPropertyKey, propertyValues);
                equalNameTerms.clear();
                equalNameTerms.add(term);
            } else {
                equalNameTerms.clear();
                equalNameTerms.add(term);
            }
        }
        if (equalNameTerms.size() > 1)
            createAggregate(tx, copyProperties, new HashSet<>(equalNameTerms),
                    new String[]{ConceptManager.ConceptLabel.AGGREGATE_EQUAL_NAMES.toString()},
                    ConceptManager.ConceptLabel.AGGREGATE_EQUAL_NAMES);
        for (Node term : equalNameTerms)
            NodeUtilities.mergeArrayProperty(term, termPropertyKey, propertyValues);

    }

    public static void deleteAggregates(Transaction tx, Label aggregateLabel) {
        ResourceIterable<Node> aggregates = () -> tx.findNodes(aggregateLabel);
        for (Node aggregate : aggregates) {
            if (!aggregate.hasLabel(ConceptLabel.AGGREGATE)) {
                // For terms that are not really aggregates, we just remove
                // the label - we want to keep the term
                // itself.
                aggregate.removeLabel(aggregateLabel);
                continue;
            }
            // Delete the aggregate.
            for (Relationship rel : aggregate.getRelationships()) {
                rel.delete();
            }
            aggregate.delete();
        }
    }

    /**
     * @param allowedMappingTypes  The mapping types that will be used to build aggregates. This
     *                             relates to the property of mapping relationships that exposes
     *                             the type of mapping the relationships is part of. All edges
     *                             that are of at least one mapping type delivered by this
     *                             parameter will be traversed to build an aggregate. That means,
     *                             the more mapping types are allowed, the bigger the aggregates
     *                             become. Also, each distinguished set of allowed mapping types
     *                             defines a particular set of aggregations.
     * @param allowedTermLabel     Label to restrict the terms for which aggregates are built.
     * @param aggregatedTermsLabel Label for terms that have been processed by the aggregation
     *                             algorithm. Such terms can be aggregate terms (with the label
     *                             {@link ConceptLabel#AGGREGATE}) or just plain terms (with the label
     *                             {@link ConceptLabel#CONCEPT}) that are not an element of an aggregate.
     */
    public static void buildAggregatesForMappings(Transaction tx, Set<String> allowedMappingTypes,
                                                  Label allowedTermLabel, Label aggregatedTermsLabel) {
        log.info("Building aggregates for mappings " + allowedMappingTypes + " and terms with label "
                + allowedTermLabel);
        String[] copyProperties = new String[]{ConceptConstants.PROP_PREF_NAME, ConceptConstants.PROP_SYNONYMS,
                ConceptConstants.PROP_WRITING_VARIANTS, ConceptConstants.PROP_DESCRIPTIONS, ConceptConstants.PROP_FACETS};
        // At first, delete all mapping aggregates since they will be built
        // again afterwards.
        deleteAggregates(tx, aggregatedTermsLabel);

        // Iterate through terms, look for mappings and generate mapping
        // aggregates
        Label label = null == allowedTermLabel ? ConceptManager.ConceptLabel.CONCEPT : allowedTermLabel;
        ResourceIterable<Node> termIterable = () -> tx.findNodes(label);
        for (Node term : termIterable) {
            // Determine recursively other nodes with which a new aggregate
            // should be created.

            // First collect all mapping aggregates with the correct mapping
            // types this term already is element of.
            // We use this for duplicate avoidance.
            Set<Node> aggregateNodes = getMatchingAggregates(term, allowedMappingTypes, aggregatedTermsLabel);
            if (aggregateNodes.size() > 1)
                throw new IllegalStateException("Term with ID " + term.getProperty(ConceptConstants.PROP_ID)
                        + " is part of multiple aggregates of the same type, thus duplicates. The aggregate nodes are: "
                        + aggregateNodes);
            if (aggregateNodes.size() == 1)
                // This term already is part of an aggregate of correct
                // type, continue to the next term
                continue;

            // This set will be used to recursively collect all terms that
            // are mapped directory or indirectly to the
            // current term, thus traversing the whole "mapped-to subgraph"
            // of this term. I.e. the "mapped-to"
            // relation is an equivalence relation to us.
            Set<Node> elements = new HashSet<>();
            Set<Node> visited = new HashSet<>();
            determineMappedSubgraph(allowedMappingTypes, allowedTermLabel, term, elements, visited);

            if (elements.size() > 1) {
                createAggregate(tx, copyProperties, elements,
                        allowedMappingTypes.toArray(new String[allowedMappingTypes.size()]),
                        // TermLabel.AGGREGATE_MAPPING);
                        aggregatedTermsLabel);

                // aggregate could be null if we have a term that just has
                // no mappings
                // aggregate.addLabel(TermLabel.AGGREGATE_MAPPING);
                // aggregate.addLabel(aggregatedTermsLabel);
            } else {
                // The current is not mapped to other terms, at least not
                // with one of the allowed mapping types. So
                // it is "its own" aggregate.
                term.addLabel(aggregatedTermsLabel);
            }

        }
    }

    protected static void determineMappedSubgraph(Set<String> allowedMappingTypes, Label allowedTermLabel, Node term,
                                                  Set<Node> elements, Set<Node> visited) {
        if (visited.contains(term))
            return;
        visited.add(term);
        Iterable<Relationship> mappings = term.getRelationships(ConceptManager.EdgeTypes.IS_MAPPED_TO);
        // Set<String> aggregateMappingTypes = new HashSet<>();
        for (Relationship mapping : mappings) {
            if (!mapping.hasProperty(ConceptRelationConstants.PROP_MAPPING_TYPE))
                throw new IllegalStateException("The mapping relationship " + mapping + " does not specify its type.");
            // We must check whether the found mapping is of a type that should
            // be aggregated in this call.
            String[] mappingTypes = (String[]) mapping.getProperty(ConceptRelationConstants.PROP_MAPPING_TYPE);
            for (String mappingType : mappingTypes) {
                if (allowedMappingTypes.contains(mappingType)) {
                    // There is at least one allowed mapping and thus an
                    // aggregate has to be created. Of course, the
                    // current term itself will also be an element of the
                    // aggregate.
                    if (null == allowedTermLabel || term.hasLabel(allowedTermLabel))
                        elements.add(term);
                    // This is an allowed mapping of the current term. Thus, all
                    // directly mapped terms to this
                    // term should be elements of the resulting aggregate.
                    Node otherTerm = mapping.getOtherNode(term);
                    if (!elements.contains(otherTerm)) {
                        if (null == allowedTermLabel || otherTerm.hasLabel(allowedTermLabel))
                            elements.add(otherTerm);
                        determineMappedSubgraph(allowedMappingTypes, allowedTermLabel, otherTerm, elements, visited);
                    }
                }
            }
        }
    }

    /**
     * Returns all the aggregate nodes where <tt>conceptNode</tt> is an element of and
     * where the aggregate is exactly of the mapping types specified in
     * <tt>allowedMappingTypes</tt>.
     *
     * @param conceptNode         The concept node that is an element of the sought aggregates.
     * @param allowedMappingTypes The mapping type for which aggregates are requested.
     * @param aggregateLabel      The aggregate label for requested aggregate nodes.
     * @return Matching aggregates for which <tt>conceptNode</tt> is an element of.
     */
    protected static Set<Node> getMatchingAggregates(Node conceptNode, Set<String> allowedMappingTypes, Label aggregateLabel) {
        Set<Node> aggregateNodes;
        aggregateNodes = new HashSet<>();
        Iterable<Relationship> elementRelationships = conceptNode.getRelationships(ConceptManager.EdgeTypes.HAS_ELEMENT);
        for (Relationship elementRelationship : elementRelationships) {
            Node aggregate = elementRelationship.getOtherNode(conceptNode);
            if (aggregate.hasLabel(aggregateLabel) && aggregate.hasLabel(ConceptLabel.AGGREGATE) && aggregate.hasProperty(ConceptConstants.PROP_MAPPING_TYPE)) {
                String[] mappingTypes = (String[]) aggregate.getProperty(ConceptConstants.PROP_MAPPING_TYPE);
                List<String> mappingTypesList = Arrays.asList(mappingTypes);
                boolean correctMappingTypes = true;
                for (String mappingType : mappingTypesList) {
                    if (!allowedMappingTypes.contains(mappingType)) {
                        correctMappingTypes = false;
                        break;
                    }
                }
                for (String mappingType : allowedMappingTypes) {
                    if (!mappingTypesList.contains(mappingType)) {
                        correctMappingTypes = false;
                        break;
                    }
                }
                if (correctMappingTypes)
                    aggregateNodes.add(aggregate);
            }
        }
        return aggregateNodes;
    }

    private static Node createAggregate(Transaction tx, String[] copyProperties, Set<Node> elementTerms,
                                        String[] mappingTypes, Label... labels) {
        if (elementTerms.isEmpty())
            return null;
        Node aggregate = tx.createNode(labels);
        aggregate.addLabel(ConceptManager.ConceptLabel.AGGREGATE);
        aggregate.setProperty(ConceptConstants.PROP_COPY_PROPERTIES, copyProperties);
        aggregate.setProperty(ConceptConstants.PROP_MAPPING_TYPE, mappingTypes);
        for (Label termLabel : labels) {
            aggregate.addLabel(termLabel);
        }
        for (Node elementTerm : elementTerms) {
            aggregate.createRelationshipTo(elementTerm, ConceptManager.EdgeTypes.HAS_ELEMENT);
        }
        String aggregateId = NodeIDPrefixConstants.AGGREGATE_TERM
                + SequenceManager.getNextSequenceValue(tx, SequenceConstants.SEQ_AGGREGATE_TERM);
        aggregate.setProperty(PROP_ID, aggregateId);
        return aggregate;
    }

    /**
     * Fills <tt>aggregate</tt> with property values from its elements.
     * Currently only called separately after all aggregates have been
     * determined to save performance. During the process of aggregate creation,
     * sometimes aggregations are merged. We don't copy all properties again but
     * only merge the elements and compute the property values from the final
     * elements after the aggregation creation process has finished. This has to
     * be done explicitly and is not done automatically.
     *
     * @param aggregate      The aggregate node to assembly element properties to.
     * @param copyProperties The properties that should be copied into the aggregate.
     * @param copyStats      An object to collect statistics over the copy process.
     */
    public static void copyAggregateProperties(Node aggregate, String[] copyProperties,
                                               CopyAggregatePropertiesStatistics copyStats) {
        // first, clear the properties be copied in case we make a refresh
        for (String copyProperty : copyProperties) {
            aggregate.removeProperty(copyProperty);
        }
        Iterable<Relationship> elementRels = aggregate.getRelationships(EdgeTypes.HAS_ELEMENT);
        // List of properties to copy that are no array properties - and thus
        // cannot be merged - but have different
        // values.
        Set<String> divergentProperties = new HashSet<>();
        // For each element...
        for (Relationship elementRel : elementRels) {
            Node term = elementRel.getEndNode();
            if (null != copyStats)
                copyStats.numElements++;
            // Copy each specified property.
            // Array properties are merged.
            // Non-array properties are set, if non existent. If a non-array
            // property has multiple, different values
            // among the elements, this property is subject to the majority vote
            // after this loop.
            for (String copyProperty : copyProperties) {
                if (term.hasProperty(copyProperty)) {
                    if (null != copyStats)
                        copyStats.numProperties++;
                    Object property = term.getProperty(copyProperty);
                    if (property.getClass().isArray()) {
                        mergeArrayProperty(aggregate, copyProperty, JulieNeo4jUtilities.convertArray(property));
                    } else {
                        setNonNullNodeProperty(aggregate, copyProperty, property);
                        Object aggregateProperty = PropertyUtilities.getNonNullNodeProperty(aggregate, copyProperty);
                        if (!aggregateProperty.equals(property)) {
                            divergentProperties.add(copyProperty);
                        }
                    }
                }
            }
        }
        // We now make majority votes on the values of divergent properties. The
        // minority property values are then
        // stored in a special property with the suffix
        // AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY

        for (String divergentProperty : divergentProperties) {
            Multiset<Object> propertyValues = HashMultiset.create();
            elementRels = aggregate.getRelationships(EdgeTypes.HAS_ELEMENT);
            for (Relationship elementRel : elementRels) {
                Node term = elementRel.getEndNode();
                Object propertyValue = PropertyUtilities.getNonNullNodeProperty(term, divergentProperty);
                if (null != propertyValue)
                    propertyValues.add(propertyValue);
            }
            // Determine the majority value.
            Object majorityValue = null;
            int maxCount = 0;
            for (Entry<Object> entry : propertyValues.entrySet()) {
                if (entry.getCount() > maxCount) {
                    majorityValue = entry.getElement();
                    maxCount = entry.getCount();
                }
            }

            // Set the majority value to the aggregate.
            aggregate.setProperty(divergentProperty, majorityValue);
            // Set the minority values to the aggregate as a special property.
            for (Object propertyValue : propertyValues.elementSet()) {
                if (!propertyValue.equals(majorityValue)) {
                    Object[] convert = JulieNeo4jUtilities.convertElementsIntoArray(propertyValue.getClass(),
                            propertyValue);
                    PropertyUtilities.mergeArrayProperty(aggregate,
                            divergentProperty + AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY, convert);
                }
            }
        }

        // The aggregate could have a conflict on the preferred name. This is
        // already resolved by a majority
        // vote above. We now additionally merge the minority names to the
        // synonyms.
        PropertyUtilities.mergeArrayProperty(aggregate, ConceptConstants.PROP_SYNONYMS,
                (Object[]) PropertyUtilities.getNonNullNodeProperty(aggregate,
                        ConceptConstants.PROP_PREF_NAME + AggregateConstants.SUFFIX_DIVERGENT_ELEMENT_ROPERTY));

        // As a last step, remove duplicate synonyms, case ignored
        if (aggregate.hasProperty(ConceptConstants.PROP_SYNONYMS)) {
            String[] synonyms = (String[]) aggregate.getProperty(ConceptConstants.PROP_SYNONYMS);
            Set<String> lowerCaseSynonyms = new HashSet<>();
            List<String> acceptedSynonyms = new ArrayList<>();
            for (String synonym : synonyms) {
                String lowerCaseSynonym = synonym.toLowerCase();
                if (!lowerCaseSynonyms.contains(lowerCaseSynonym)) {
                    lowerCaseSynonyms.add(lowerCaseSynonym);
                    acceptedSynonyms.add(synonym);
                }
            }
            Collections.sort(acceptedSynonyms);
            aggregate.setProperty(ConceptConstants.PROP_SYNONYMS,
                    acceptedSynonyms.toArray(new String[0]));
        }
    }

    public static class CopyAggregatePropertiesStatistics {
        public int numProperties = 0;
        public int numElements = 0;

        @Override
        public String toString() {
            return "CopyAggregatePropertiesStatistics [numProperties=" + numProperties + ", numElements=" + numElements
                    + "]";
        }

    }
}
