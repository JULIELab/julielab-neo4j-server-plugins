package de.julielab.neo4j.plugins;

import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.addToArrayProperty;
import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.findFirstValueInArrayProperty;
import static de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities.mergeArrayProperty;
import static de.julielab.neo4j.plugins.constants.semedico.NodeConstants.PROP_ID;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.AGGREGATE_INCLUDE_IN_HIERARCHY;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.INDEX_NAME;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PARENT_COORDINATES;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PARENT_SOURCES;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PARENT_SRC_IDS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_CHILDREN_IN_FACETS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_COORDINATES;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_COPY_PROPERTIES;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_FACETS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_ORG_ID;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_ORG_SRC;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_PREF_NAME;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_SOURCES;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_SRC_IDS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_SYNONYMS;
import static de.julielab.neo4j.plugins.constants.semedico.TermConstants.PROP_UNIQUE_SRC_ID;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.queryParser.QueryParser;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import de.julielab.neo4j.plugins.FacetManager.FacetLabel;
import de.julielab.neo4j.plugins.auxiliaries.JSON;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.RecursiveMappingRepresentation;
import de.julielab.neo4j.plugins.auxiliaries.semedico.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.auxiliaries.semedico.SequenceManager;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermAggregateBuilder;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermAggregateBuilder.CopyAggregatePropertiesStatistics;
import de.julielab.neo4j.plugins.auxiliaries.semedico.TermVariantComparator;
import de.julielab.neo4j.plugins.constants.semedico.CoordinateConstants;
import de.julielab.neo4j.plugins.constants.semedico.FacetConstants;
import de.julielab.neo4j.plugins.constants.semedico.MorphoConstants;
import de.julielab.neo4j.plugins.constants.semedico.MorphoRelationConstants;
import de.julielab.neo4j.plugins.constants.semedico.NodeConstants;
import de.julielab.neo4j.plugins.constants.semedico.NodeIDPrefixConstants;
import de.julielab.neo4j.plugins.constants.semedico.SequenceConstants;
import de.julielab.neo4j.plugins.constants.semedico.TermConstants;
import de.julielab.neo4j.plugins.constants.semedico.TermRelationConstants;
import de.julielab.neo4j.plugins.datarepresentation.AddToNonFacetGroupCommand;
import de.julielab.neo4j.plugins.datarepresentation.ConceptCoordinates;
import de.julielab.neo4j.plugins.datarepresentation.ImportOptions;
import de.julielab.neo4j.plugins.datarepresentation.PushTermsToSetCommand;
import de.julielab.neo4j.plugins.datarepresentation.PushTermsToSetCommand.TermSelectionDefinition;

@Description("This plugin discloses special operation for efficient access to the FacetTerms for Semedico.")
public class TermManager extends ServerPlugin {

	private static final String UNKNOWN_TERM_SOURCE = "<unknown>";

	public static enum EdgeTypes implements RelationshipType {
		/**
		 * Relationship type for connecting aggregate classes with their element
		 * terms.
		 */
		HAS_ELEMENT, HAS_ROOT_TERM,
		/**
		 * Relationship type to express that two terms seem to be identical
		 * regarding preferred label and synonyms.
		 */
		HAS_SAME_NAMES, IS_BROADER_THAN,
		/**
		 * A term mapping that expresses some similarity between to terms, e.g.
		 * 'equal' or 'related'. The actual type of relatedness should be added
		 * as a property to the relationship.
		 */
		IS_MAPPED_TO,
		/**
		 * Term writing variants and their frequencies are stored in a special
		 * kind of node, connected to the respective term with this relationship
		 * type.
		 */
		HAS_VARIANTS, HAS_ACRONYMS
	}

	private class InsertionReport {
		/**
		 * A temporary storage to keep track over relationships created during a
		 * single term insertion batch. It is used for determination whether
		 * existing relationships between two terms must be checked or not. This
		 * is required for the case that a term is inserted multiple times in a
		 * single insertion batch. Then, the "term already existing" method does
		 * not work anymore.
		 */
		public Set<String> createdRelationshipsCache = new HashSet<>();
		/**
		 * The source IDs of terms that already existed before they should have
		 * been inserted again (duplicate detection). This is used to determine
		 * whether a check about already existing relationships between two
		 * nodes is necessary. If at least one of two terms between which a
		 * relationships should be created did not exist before, no check is
		 * necessary: A term that did not exist could not have had any
		 * relationships.
		 */
		public Set<Node> existingTerms = new HashSet<>();
		/**
		 * The source IDs of terms that have been omitted from the data for -
		 * hopefully - good reasons. The first (and perhaps only) use case were
		 * aggregates which had a single elements and were thus omitted but
		 * should also be included into the term hierarchy and have other terms
		 * referring to them as a parent. This set serves as a lookup in this
		 * case so we know there is not an error.
		 */
		public Set<String> omittedTerms = new HashSet<>();
		public int numRelationships = 0;
		public int numTerms = 0;

		public void addCreatedRelationship(Node source, Node target, RelationshipType type) {
			createdRelationshipsCache.add(getRelationshipIdentifier(source, target, type));
		}

		public void addExistingTerm(Node term) {
			existingTerms.add(term);
		}

		private String getRelationshipIdentifier(Node source, Node target, RelationshipType type) {
			return source.getId() + type.name() + target.getId();
		}

		public boolean relationshipAlreadyWasCreated(Node source, Node target, RelationshipType type) {
			return createdRelationshipsCache.contains(getRelationshipIdentifier(source, target, type));
		}

	}

	public static enum TermLabel implements Label {
		/**
		 * Label to indicate a node is not an actual term but an aggregate term.
		 * Such terms have {@link EdgeTypes#HAS_ELEMENT} relationships to terms,
		 * determining the set of terms the aggregate represents.
		 */
		AGGREGATE,
		/** A particular type of {@link #AGGREGATE} node. */
		AGGREGATE_EQUAL_NAMES,
		/**
		 * Label for nodes that are referenced by at least one other term in
		 * imported data, but are not included in the imported data themselves.
		 * Such terms know their source ID (given by the reference of another
		 * term) and will be made un-HOLLOW as soon at a term with this source
		 * ID occurs in imported data.
		 */
		HOLLOW, TERM, EVENT_TERM,
		/**
		 * @deprecated It doesn't seem this label is required or used anywhere
		 */
		@Deprecated
		AGGREGATE_ELEMENT
	}

	/**
	 * Labels for nodes representing lexico-morphological variations of terms.
	 *
	 */
	public static enum MorphoLabel implements Label {
		WRITING_VARIANTS, ACRONYMS, WRITING_VARIANT, ACRONYM
	}

	public static final String INSERT_MAPPINGS = "insert_mappings";
	public static final String BUILD_AGGREGATES_BY_NAME_AND_SYNONYMS = "build_aggregates_by_name_and_synonyms";
	public static final String BUILD_AGGREGATES_BY_MAPPINGS = "build_aggregates_by_mappings";
	public static final String DELETE_AGGREGATES = "delete_aggregates";
	public static final String COPY_AGGREGATE_PROPERTIES = "copy_aggregate_properties";
	public static final String CREATE_SCHEMA_INDEXES = "create_schema_indexes";
	public static final String GET_CHILDREN_OF_TERMS = "get_children_of_terms";
	public static final String GET_NUM_TERMS = "get_num_terms";
	public static final String GET_PATHS_FROM_FACETROOTS = "get_paths_to_facetroots";
	public static final String INSERT_TERMS = "insert_terms";
	public static final String GET_FACET_ROOTS = "get_facet_roots";
	public static final String ADD_TERM_VARIANTS = "add_term_variants";
	public static final String KEY_AMOUNT = "amount";
	public static final String KEY_CREATE_HOLLOW_PARENTS = "createHollowParents";
	public static final String KEY_FACET = "facet";
	public static final String KEY_FACET_ID = "facetId";
	public static final String KEY_FACET_IDS = "facetIds";
	public static final String KEY_FACET_PROP_KEY = "propertyKey";
	public static final String KEY_FACET_PROP_VALUE = "propertyValue";
	public static final String KEY_ID_TYPE = "idType";
	public static final String KEY_IMPORT_OPTIONS = "importOptions";
	public static final String KEY_LABEL = "label";
	public static final String KEY_SORT_RESULT = "sortResult";
	public static final String KEY_TERM_IDS = "termIds";
	public static final String KEY_MAX_ROOTS = "maxRoots";
	public static final String KEY_TERM_PROP_KEY = "termPropertyKey";
	public static final String KEY_TERM_PROP_VALUE = "termPropertyValue";
	public static final String KEY_TERM_PROP_VALUES = "termPropertyValues";
	public static final String KEY_TERM_PUSH_CMD = "termPushCommand";
	public static final String KEY_AGGREGATED_LABEL = "aggregatedLabel";
	public static final String KEY_ALLOWED_MAPPING_TYPES = "allowedMappingTypes";
	public static final String KEY_TERM_VARIANTS = "termVariants";
	public static final String KEY_TERM_ACRONYMS = "termAcronyms";

	/**
	 * The key of the map to send to the {@link #INSERT_TERMS} endpoint.
	 */
	public static final String KEY_TERMS = "terms";
	public static final String KEY_TIME = "time";
	public static final String KEY_MAPPINGS = "mappings";
	private static final Logger log = LoggerFactory.getLogger(TermManager.class);
	public static final String POP_TERMS_FROM_SET = "pop_terms_from_set";
	public static final String PUSH_TERMS_TO_SET = "push_terms_to_set";
	public static final String RET_KEY_CHILDREN = "children";
	public static final String RET_KEY_NUM_AGGREGATES = "numAggregates";
	public static final String RET_KEY_NUM_CREATED_RELS = "numCreatedRelationships";
	public static final String RET_KEY_NUM_CREATED_TERMS = "numCreatedTerms";
	public static final String RET_KEY_NUM_ELEMENTS = "numElements";
	public static final String RET_KEY_NUM_PROPERTIES = "numProperties";
	public static final String RET_KEY_PATHS = "paths";
	public static final String RET_KEY_RELTYPES = "reltypes";

	public static final String RET_KEY_TERMS = "terms";

	/**
	 * The REST context path to this plugin. This is for convenience for usage
	 * from external programs that make use of the plugin.
	 */
	public static final String TERM_MANAGER_ENDPOINT = "db/data/ext/" + TermManager.class.getSimpleName() + "/graphdb/";

	private static final int TERM_INSERT_BATCH_SIZE = 10000;

	public static final String UPDATE_CHILDREN_INFORMATION = "update_children_information";

	@Name(BUILD_AGGREGATES_BY_MAPPINGS)
	@Description("Creates term aggregates with respect to 'IS_MAPPED_TO' relationships.")
	@PluginTarget(GraphDatabaseService.class)
	public void buildAggregatesByMappigs(@Source GraphDatabaseService graphDb,
			@Description("The allowed types for IS_MAPPED_TO relationships to be included in aggregation building.") @Parameter(name = KEY_ALLOWED_MAPPING_TYPES) String allowedMappingTypesArray,
			@Description("Label for terms that have been processed by the aggregation algorithm. Such terms"
					+ " can be aggregate terms (with the label AGGREGATE) or just plain terms"
					+ " (with the label TERM) that are not an element of an aggregate.") @Parameter(name = KEY_AGGREGATED_LABEL) String aggregatedTermsLabelString,
			@Description("Label to restrict the terms to that are considered for aggregation creation.") @Parameter(name = KEY_LABEL, optional = true) String allowedTermLabelString)
			throws JSONException {
		JSONArray allowedMappingTypesJson = new JSONArray(allowedMappingTypesArray);
		Set<String> allowedMappingTypes = new HashSet<>();
		for (int i = 0; i < allowedMappingTypesJson.length(); i++) {
			allowedMappingTypes.add(allowedMappingTypesJson.getString(i));
		}
		Label aggregatedTermsLabel = DynamicLabel.label(aggregatedTermsLabelString);
		Label allowedTermLabel = StringUtils.isBlank(allowedTermLabelString) ? null
				: DynamicLabel.label(allowedTermLabelString);
		log.info("Creating mapping aggregates for terms with label " + allowedTermLabel + " and mapping types "
				+ allowedMappingTypesJson);
		TermAggregateBuilder.buildAggregatesForMappings(graphDb, allowedMappingTypes, allowedTermLabel,
				aggregatedTermsLabel);
	}

	@Name(DELETE_AGGREGATES)
	@Description("Deletes aggregates with respect to a specific aggregate label. Only real aggregates are actually deleted, plain terms that are 'their own' aggregates just lose the label.")
	@PluginTarget(GraphDatabaseService.class)
	public void deleteAggregatesByMappigs(@Source GraphDatabaseService graphDb,
			@Description("Label for terms that have been processed by the aggregation algorithm. Such terms"
					+ " can be aggregate terms (with the label AGGREGATE) or just plain terms"
					+ " (with the label TERM) that are not an element of an aggregate.") @Parameter(name = KEY_AGGREGATED_LABEL) String aggregatedTermsLabelString)
			throws JSONException {
		Label aggregatedTermsLabel = DynamicLabel.label(aggregatedTermsLabelString);
		TermAggregateBuilder.deleteAggregates(graphDb, aggregatedTermsLabel);
	}

	@Name(BUILD_AGGREGATES_BY_NAME_AND_SYNONYMS)
	// TODO
	@Description("TODO")
	@PluginTarget(GraphDatabaseService.class)
	public void buildAggregatesByNameAndSynonyms(@Source GraphDatabaseService graphDb,
			@Description("TODO") @Parameter(name = KEY_TERM_PROP_KEY) String termPropertyKey,
			@Description("TODO") @Parameter(name = KEY_TERM_PROP_VALUES) String propertyValues) throws JSONException {
		JSONArray jsonPropertyValues = new JSONArray(propertyValues);
		TermAggregateBuilder.buildAggregatesForEqualNames(graphDb, termPropertyKey, jsonPropertyValues);
	}

	@Name(COPY_AGGREGATE_PROPERTIES)
	// TODO
	@Description("TODO")
	@PluginTarget(GraphDatabaseService.class)
	public Representation copyAggregateProperties(@Source GraphDatabaseService graphDb) {
		int numAggregates = 0;
		CopyAggregatePropertiesStatistics copyStats = new CopyAggregatePropertiesStatistics();
		try (Transaction tx = graphDb.beginTx()) {
			try (ResourceIterator<Node> aggregateIt = graphDb.findNodes(TermLabel.AGGREGATE)) {
				while (aggregateIt.hasNext()) {
					Node aggregate = aggregateIt.next();
					numAggregates += copyAggregatePropertiesRecursively(aggregate, copyStats, new HashSet<Node>());
				}
			}
			tx.success();
		}
		Map<String, Object> reportMap = new HashMap<>();
		reportMap.put(RET_KEY_NUM_AGGREGATES, numAggregates);
		reportMap.put(RET_KEY_NUM_ELEMENTS, copyStats.numElements);
		reportMap.put(RET_KEY_NUM_PROPERTIES, copyStats.numProperties);
		return new RecursiveMappingRepresentation(Representation.MAP, reportMap);
	}

	private int copyAggregatePropertiesRecursively(Node aggregate, CopyAggregatePropertiesStatistics copyStats,
			Set<Node> alreadySeen) {
		if (alreadySeen.contains(aggregate))
			return 0;
		List<Node> elementAggregates = new ArrayList<>();
		Iterable<Relationship> elementRels = aggregate.getRelationships(Direction.OUTGOING, EdgeTypes.HAS_ELEMENT);
		for (Relationship elementRel : elementRels) {
			Node endNode = elementRel.getEndNode();
			if (endNode.hasLabel(TermLabel.AGGREGATE) && !alreadySeen.contains(endNode))
				elementAggregates.add(endNode);
		}
		for (Node elementAggregate : elementAggregates) {
			copyAggregatePropertiesRecursively(elementAggregate, copyStats, alreadySeen);
		}
		if (aggregate.hasProperty(PROP_COPY_PROPERTIES)) {
			String[] copyProperties = (String[]) aggregate.getProperty(PROP_COPY_PROPERTIES);
			TermAggregateBuilder.copyAggregateProperties(aggregate, copyProperties, copyStats);
		}
		alreadySeen.add(aggregate);
		return alreadySeen.size();
	}

	private void createRelationships(GraphDatabaseService graphDb, JSONArray jsonTerms, Node facet,
			Map<String, Node> nodesBySrcId, ImportOptions importOptions, InsertionReport insertionReport)
			throws JSONException {
		log.info("Creating relationship between inserted terms.");
		Index<Node> idIndex = graphDb.index().forNodes(TermConstants.INDEX_NAME);
		String facetId = null;
		if (null != facet)
			facetId = (String) facet.getProperty(FacetConstants.PROP_ID);
		DynamicRelationshipType relBroaderThanInFacet = null;
		if (null != facet)
			relBroaderThanInFacet = DynamicRelationshipType
					.withName(EdgeTypes.IS_BROADER_THAN.toString() + "_" + facetId);
		AddToNonFacetGroupCommand noFacetCmd = importOptions.noFacetCmd;
		Node noFacet = null;
		int quarter = jsonTerms.length() / 4;
		int numQuarter = 1;
		long totalTime = 0;
		long relCreationTime = 0;
		for (int i = 0; i < jsonTerms.length(); i++) {
			long time = System.currentTimeMillis();
			JSONObject jsonTerm = jsonTerms.getJSONObject(i);
			if (JSON.getBoolean(jsonTerm, TermConstants.AGGREGATE)
					&& !JSON.getBoolean(jsonTerm, TermConstants.AGGREGATE_INCLUDE_IN_HIERARCHY))
				continue;
			JSONObject coordinates = jsonTerm.getJSONObject(PROP_COORDINATES);
			// Every term must have a source ID...
			String srcId = coordinates.getString(CoordinateConstants.SOURCE_ID);
			// ...but it is not required to have a parent in its source.
			// Then, it's a facet root.
			Node term = nodesBySrcId.get(srcId);
			// Perhaps the term was omitted on purpose?
			if (null == term && insertionReport.omittedTerms.contains(srcId))
				continue;
			if (null == term) {
				throw new IllegalStateException("No node for source ID " + srcId
						+ " was created but the respective concept is included into the data for import and it is unknown why no node instance was created.");
			}
			// Default-relationships (taxonomical).
			{
				if (jsonTerm.has(PARENT_COORDINATES)) {
					JSONArray parentCoordinates = jsonTerm.getJSONArray(PARENT_COORDINATES);
					for (int j = 0; j < parentCoordinates.length(); j++) {
						JSONObject parentCoordinate = parentCoordinates.getJSONObject(j);
						String parentSrcId = parentCoordinate.getString(CoordinateConstants.SOURCE_ID);
						String parentSource = parentCoordinate.getString(CoordinateConstants.SOURCE);
						if (importOptions.cutParents.contains(parentSrcId)) {
							createRelationShipIfNotExists(facet, term, EdgeTypes.HAS_ROOT_TERM, insertionReport);
							continue;
						}

						// The term has another term as parent. Connect
						// them.
						Node parent = nodesBySrcId.get(parentSrcId);

						if (null != parent) {
							long creationTime = System.currentTimeMillis();
							createRelationShipIfNotExists(parent, term, EdgeTypes.IS_BROADER_THAN, insertionReport);
							// Since a term may appear in multiple facets, we
							// connect terms with a general taxonomic
							// relation as well as a special relation only
							// relevant to the
							// particular structure‚ of the current facet.
							createRelationShipIfNotExists(parent, term, relBroaderThanInFacet, insertionReport);
							relCreationTime += System.currentTimeMillis() - creationTime;
						} else {
							// If the parent is not found in nodesBySrcId it
							// does not exist in the currently imported data nor
							// in the database. If it would have existed in the
							// database, we would have added it to the map in
							// insertFacetTerm().
							// TODO this approach fails completely with ontology
							// imports: Imported classes are defined within the
							// ontology neither is it clear, what the defining
							// ontology will have as an ID (BioPortal). However,
							// class IRIs have to be unique anyway. We need a
							// mechanism where sources may be ignored. Think
							// this
							// through: When do we really know the source(s)?
							// Are there cases where sourceIds are unique and
							// other cases where they aren't? Then perhaps we
							// need an option to allow "source-less" lookup
							// explicitly.
							log.info("Term with source ID \"" + srcId + "\" referenced the term with source ID \""
									+ parentSrcId + "\" as its parent. However, that parent node does not exist.");

							if (importOptions.createHollowParents) {
								log.info(
										"Creating hollow parents is switched on. The parent will be created with the label \""
												+ TermLabel.HOLLOW + "\" and be connected to the facet root.");
								// We create the parent as a "hollow" term and
								// connect it to the facet root. The latter
								// is the only thing we can do because we can't
								// get to know the
								// parent's parent since it is not included in
								// the data.
								Node hollowParent = graphDb.createNode(TermLabel.TERM, TermLabel.HOLLOW);
								addToArrayProperty(hollowParent, PROP_SRC_IDS, parentSrcId);
								// Analogous to the parent handling in
								// insertFacetTerms, we fall back to the terms
								// source if no parent source is given for
								// legacy reasons.
								String source = parentSource != null ? parentSource
										: JSON.getString(jsonTerm, PROP_SOURCES);
								if (source == null)
									source = UNKNOWN_TERM_SOURCE;
								addToArrayProperty(hollowParent, PROP_SOURCES, source);
								idIndex.add(hollowParent, PROP_SRC_IDS, parentSrcId);
								nodesBySrcId.put(parentSrcId, hollowParent);
								insertionReport.numTerms++;
								createRelationShipIfNotExists(hollowParent, term, EdgeTypes.IS_BROADER_THAN,
										insertionReport);
								createRelationShipIfNotExists(hollowParent, term, relBroaderThanInFacet,
										insertionReport);
								createRelationShipIfNotExists(facet, hollowParent, EdgeTypes.HAS_ROOT_TERM,
										insertionReport);
							} else {
								log.info(
										"Creating hollow parents is switched off. Hence the term will be added as root term for its facet (\""
												+ facet.getProperty(FacetConstants.PROP_NAME) + "\").");
								// Connect the term as a root, it's the best we
								// can
								// do.
								createRelationShipIfNotExists(facet, term, EdgeTypes.HAS_ROOT_TERM, insertionReport);
							}
						}
						if (null != parent && parent.hasLabel(TermLabel.AGGREGATE) && !parent.hasLabel(TermLabel.TERM))
							throw new IllegalArgumentException("Concept with source ID " + srcId
									+ " specifies source ID " + parentSrcId
									+ " as parent. This node is an aggregate but not a TERM itself and thus is not included in the hierarchy and cannot be the conceptual parent of other concepts. To achieve this, import the aggregate with the property "
									+ TermConstants.AGGREGATE_INCLUDE_IN_HIERARCHY
									+ " set to true or build the aggregates in a way that assignes the TERM label to them.");
					}

				} else {
					if (noFacetCmd != null && noFacetCmd.getParentCriteria()
							.contains(AddToNonFacetGroupCommand.ParentCriterium.NO_PARENT)) {
						if (null != noFacetCmd && null == noFacet) {
							noFacet = FacetManager.getNoFacet(graphDb, (String) facet.getProperty(PROP_ID));
						}

						createRelationShipIfNotExists(noFacet, term, EdgeTypes.HAS_ROOT_TERM, insertionReport);
					} else if (null != facet) {
						// This term does not have a term parent. It is a facet
						// root,
						// thus connect it to the facet node.
						log.debug("Installing term with source ID " + srcId + " (ID: " + term.getProperty(PROP_ID)
								+ ") as root for facet " + facet.getProperty(NodeConstants.PROP_NAME) + "(ID: "
								+ facet.getProperty(PROP_ID) + ")");
						createRelationShipIfNotExists(facet, term, EdgeTypes.HAS_ROOT_TERM, insertionReport);
					}
					// else: nothing, because the term already existed, we are
					// merely merging here.
				}
			}
			// Explicitly specified relationships (has-same-name-as,
			// is-mapped-to,
			// whatever...)
			{
				if (jsonTerm.has(TermConstants.RELATIONSHIPS)) {
					log.info("Adding explicitly specified relationships");
					JSONArray jsonRelationships = jsonTerm.getJSONArray(TermConstants.RELATIONSHIPS);
					for (int j = 0; j < jsonRelationships.length(); j++) {
						JSONObject jsonRelationship = jsonRelationships.getJSONObject(j);
						String rsTypeStr = jsonRelationship.getString(TermConstants.RS_TYPE);
						String targetOrgId = JSON.getString(jsonRelationship, TermConstants.RS_TARGET_ORG_ID);
						String targetOrgSource = JSON.getString(jsonRelationship, TermConstants.RS_TARGET_ORG_SRC);
						String targetSrcId = JSON.getString(jsonRelationship, TermConstants.RS_TARGET_SRC_ID);
						String targetSource = JSON.getString(jsonRelationship, TermConstants.RS_TARGET_SRC);
						Node target = lookupTerm(new ConceptCoordinates(targetSrcId, targetSource, targetOrgId, targetOrgSource,
								JSON.getBoolean(jsonTerm, PROP_UNIQUE_SRC_ID, false)), idIndex);
						if (null == target) {
							log.debug("Creating hollow relationship target with orig Id/orig source (" + targetOrgId
									+ "," + targetOrgSource + ") and source Id/source : (" + targetSrcId + ", "
									+ targetSource + ")");
							target = graphDb.createNode(TermLabel.TERM, TermLabel.HOLLOW);
							addToArrayProperty(target, PROP_SRC_IDS, targetSrcId);
							addToArrayProperty(target, PROP_SOURCES, targetSource);
							if (null != targetOrgId)
								target.setProperty(PROP_ORG_ID, targetOrgId);
							if (null != targetOrgSource)
								target.setProperty(PROP_ORG_SRC, targetOrgSource);
							if (null != targetOrgId)
								idIndex.add(target, PROP_ORG_ID, targetOrgId);
							if (null != targetSrcId)
								idIndex.add(target, PROP_SRC_IDS, targetSrcId);
						}
						EdgeTypes type = EdgeTypes.valueOf(rsTypeStr);
						Object[] properties = null;
						if (jsonRelationship.has(TermConstants.RS_PROPS)) {
							JSONObject relProps = jsonRelationship.getJSONObject(TermConstants.RS_PROPS);
							JSONArray propNames = relProps.names();
							properties = new Object[propNames.length() * 2];
							for (int k = 0; k < propNames.length(); ++k) {
								String propName = propNames.getString(k);
								Object propValue = relProps.get(propName);
								properties[2 * k] = propName;
								properties[2 * k + 1] = propValue;
							}
						}
						createRelationShipIfNotExists(term, target, type, insertionReport, Direction.OUTGOING,
								properties);
						// term.createRelationshipTo(target, type);
						insertionReport.numRelationships++;
					}
				}
			}
			totalTime += System.currentTimeMillis() - time;
			if (i >= numQuarter * quarter) {
				log.info("Finished " + (25 * numQuarter) + "% of terms for relationship creation.");
				log.info("Relationship creation took " + relCreationTime + "ms.");
				log.info("Total time consumption for creation of " + insertionReport.numRelationships
						+ " relationships until now: " + totalTime + "ms.");
				numQuarter++;
			}
		}
		log.info("Finished 100% of terms for relationship creation.");
	}

	/**
	 * Checks whether an automatic index for the <tt>label</tt> exists on the
	 * {@link TermConstants#PROP_ID} property and creates it, if not.
	 * 
	 * @param graphDb
	 * @param label
	 */
	private void createIndexIfAbsent(GraphDatabaseService graphDb, Label label, String key, boolean unique) {
		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			boolean indexExists = false;
			for (IndexDefinition id : schema.getIndexes(label)) {
				for (String propertyKey : id.getPropertyKeys()) {
					if (propertyKey.equals(key))
						indexExists = true;
				}
			}
			if (!indexExists) {
				log.info("Creating index for label " + label + " on property " + key + " (unique: " + unique + ").");
				// IndexDefinition indexDefinition;
				// indexDefinition = schema.indexFor(label).on(key).create();
				schema.constraintFor(label).assertPropertyIsUnique(key).create();
				// schema.awaitIndexOnline(indexDefinition, 15,
				// TimeUnit.MINUTES);
				tx.success();
			}
		}
	}

	/**
	 * Creates a relationship of type <tt>type</tt> from <tt>source</tt> to
	 * <tt>target</tt>, if this relationship does not already exist.
	 * 
	 * @param source
	 * @param target
	 * @param type
	 * @param insertionReport
	 * @return
	 */
	private Relationship createRelationShipIfNotExists(Node source, Node target, RelationshipType type,
			InsertionReport insertionReport) {
		return createRelationShipIfNotExists(source, target, type, insertionReport, Direction.OUTGOING);
	}

	/**
	 * Creates a relationship of type <tt>type</tt> from <tt>source</tt> to
	 * <tt>target</tt>, if this relationship does not already exist.
	 * <p>
	 * The parameter <tt>direction</tt> may be used to determine for which
	 * direction there should be checked for an existing relationship, outgoing,
	 * incoming or both. Note that the new relationship will <em>always</em> be
	 * created from <tt>source</tt> to <tt>target</tt>, no matter for which
	 * direction existing relationships should be checked.
	 * </p>
	 * <p>
	 * If a relationship of type <tt>type</tt> already exists but has different
	 * properties than specified by <tt>properties</tt>, it will be tried to
	 * merge the properties instead of creating a new relationship.
	 * </p>
	 * 
	 * @param source
	 * @param target
	 * @param type
	 * @param insertionReport
	 * @param direction
	 * @param properties
	 *            A sequence of property key and property values. These
	 *            properties will be used to determine whether a relationship -
	 *            with those properties - already exists.
	 * @return
	 */
	private Relationship createRelationShipIfNotExists(Node source, Node target, RelationshipType type,
			InsertionReport insertionReport, Direction direction, Object... properties) {
		if (null != properties && properties.length % 2 != 0)
			throw new IllegalArgumentException("Property list must contain of key/value pairs but its length was odd.");

		boolean relationShipExists = false;
		Relationship createdRelationship = null;
		if (insertionReport.relationshipAlreadyWasCreated(source, target, type)) {
			relationShipExists = true;
		} else if (insertionReport.existingTerms.contains(source) && insertionReport.existingTerms.contains(target)) {
			// Both terms existing before the current processing call to
			// insert_terms. Thus, we have to check whether
			// the relation already exists and cannot just use
			// insertionReport.relationshipAlreadyWasCreated()
			Iterable<Relationship> relationships = source.getRelationships(direction, type);
			for (Relationship relationship : relationships) {
				if (relationship.getEndNode().equals(target)) {
					relationShipExists = true;
					if (!PropertyUtilities.mergeProperties(relationship, properties))
						relationShipExists = false;
				}
			}
		}
		if (!relationShipExists) {
			// The relationship does not exist. Create it.
			createdRelationship = source.createRelationshipTo(target, type);
			// Add the properties.
			for (int i = 0; null != properties && i < properties.length; i += 2) {
				String key = (String) properties[i];
				Object value = properties[i + 1];
				createdRelationship.setProperty(key, value);
			}
			insertionReport.addCreatedRelationship(source, target, type);
			insertionReport.numRelationships++;
		}
		return createdRelationship;
	}

	@Name(CREATE_SCHEMA_INDEXES)
	@Description("Creates uniqueness constraints (and thus, indexes), on the following label / property combinations: TERM / "
			+ TermConstants.PROP_ID + "; TERM / " + TermConstants.PROP_ORG_ID + "; FACET / " + FacetConstants.PROP_ID
			+ "; NO_FACET / " + FacetConstants.PROP_ID + "; ROOT / " + NodeConstants.PROP_NAME
			+ ". This should be done after the main initial import because node insertion with uniqueness switched on costs significant insertion performance.")
	@PluginTarget(GraphDatabaseService.class)
	public void createSchemaIndexes(@Source GraphDatabaseService graphDb) {
		createIndexIfAbsent(graphDb, TermLabel.TERM, TermConstants.PROP_ID, true);
		createIndexIfAbsent(graphDb, TermLabel.TERM, TermConstants.PROP_ORG_ID, true);
		createIndexIfAbsent(graphDb, FacetLabel.FACET, FacetConstants.PROP_ID, true);
		createIndexIfAbsent(graphDb, FacetLabel.NO_FACET, FacetConstants.PROP_ID, true);
		createIndexIfAbsent(graphDb, NodeConstants.Labels.ROOT, NodeConstants.PROP_NAME, true);
	}

	@Name(GET_CHILDREN_OF_TERMS)
	@Description("Returns all non-hollow children of terms identified via the " + KEY_TERM_IDS
			+ " parameter. The return format is a map from the children's id"
			+ " to respective child term. This endpoint has been created due"
			+ " to performance reasons. All tried Cypher queries to achieve"
			+ " the same behaviour were less performant (tested for version 2.0.0 M3).")
	@PluginTarget(GraphDatabaseService.class)
	public MappingRepresentation getChildrenOfTerms(@Source GraphDatabaseService graphDb,
			@Description("JSON array of term IDs for which to return their children.") @Parameter(name = KEY_TERM_IDS) String termIdArray,
			@Description("The label agsinst which the given term IDs are resolved. Defaults to 'TERM'.") @Parameter(name = KEY_LABEL, optional = true) String labelString)
			throws JSONException {
		Label label = TermLabel.TERM;
		if (!StringUtils.isBlank(labelString))
			label = DynamicLabel.label(labelString);
		JSONArray termIds = new JSONArray(termIdArray);
		try (Transaction tx = graphDb.beginTx()) {
			Map<String, Object> childrenByTermId = new HashMap<>();
			for (int i = 0; i < termIds.length(); i++) {
				Map<String, List<String>> reltypesByNodeId = new HashMap<>();
				Set<Node> childList = new HashSet<>();
				String termId = termIds.getString(i);
				Node term = NodeUtilities.findSingleNodeByLabelAndProperty(graphDb, label, PROP_ID, termId);
				if (null != term) {
					Iterator<Relationship> rels = term.getRelationships(Direction.OUTGOING).iterator();
					while (rels.hasNext()) {
						Relationship rel = rels.next();
						String reltype = rel.getType().name();
						Node child = rel.getEndNode();
						boolean isHollow = false;
						for (Label l : child.getLabels())
							if (l.equals(TermLabel.HOLLOW))
								isHollow = true;
						if (isHollow)
							continue;
						String childId = (String) child.getProperty(PROP_ID);
						List<String> reltypeList = reltypesByNodeId.get(childId);
						if (null == reltypeList) {
							reltypeList = new ArrayList<>();
							reltypesByNodeId.put(childId, reltypeList);
						}
						reltypeList.add(reltype);
						childList.add(child);
					}
					Map<String, Object> childrenAndReltypes = new HashMap<>();
					childrenAndReltypes.put(RET_KEY_CHILDREN, childList);
					childrenAndReltypes.put(RET_KEY_RELTYPES, reltypesByNodeId);
					childrenByTermId.put(termId, childrenAndReltypes);
				}
			}
			return new RecursiveMappingRepresentation(Representation.MAP, childrenByTermId);
		}
	}

	@Name(GET_NUM_TERMS)
	@Description("Returns the number of terms in the database, i.e. the number of nodes with the \"TERM\" label.")
	@PluginTarget(GraphDatabaseService.class)
	public long getNumTerms(@Source GraphDatabaseService graphDb) {
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> terms = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(TermLabel.TERM);
			long count = 0;
			for (@SuppressWarnings("unused")
			Node term : terms) {
				count++;
			}
			return count;
		}
	}

	@Name(GET_PATHS_FROM_FACETROOTS)
	@Description("TODO")
	@PluginTarget(GraphDatabaseService.class)
	public Representation getPathsFromFacetroots(@Source GraphDatabaseService graphDb,
			@Description("TODO") @Parameter(name = KEY_TERM_IDS) String termIdsJsonString,
			@Description("TODO") @Parameter(name = KEY_ID_TYPE) String idType,
			@Description("TODO") @Parameter(name = KEY_SORT_RESULT) boolean sort,
			@Description("TODO") @Parameter(name = KEY_FACET_ID) final String facetId) throws JSONException {
		JSONArray termIds = new JSONArray(termIdsJsonString);

		Evaluator rootTermEvaluator = new Evaluator() {

			@Override
			public Evaluation evaluate(Path path) {
				Node endNode = path.endNode();

				Iterator<Relationship> iterator = endNode.getRelationships(EdgeTypes.HAS_ROOT_TERM).iterator();
				if (iterator.hasNext()) {
					if (StringUtils.isBlank(facetId)) {
						return Evaluation.INCLUDE_AND_CONTINUE;
					} else {
						String[] facetIds = (String[]) endNode.getProperty(PROP_FACETS);
						for (String facetIdOfRootNode : facetIds) {
							if (facetIdOfRootNode.equals(facetId))
								return Evaluation.INCLUDE_AND_CONTINUE;
						}
					}
				}
				return Evaluation.EXCLUDE_AND_CONTINUE;
			}

		};
		RelationshipType relType = StringUtils.isBlank(facetId) ? TermManager.EdgeTypes.IS_BROADER_THAN
				: DynamicRelationshipType.withName(TermManager.EdgeTypes.IS_BROADER_THAN.name() + "_" + facetId);
		TraversalDescription td = graphDb.traversalDescription().uniqueness(Uniqueness.NODE_PATH).depthFirst()
				.relationships(relType, Direction.INCOMING).evaluator(rootTermEvaluator);

		try (Transaction tx = graphDb.beginTx()) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < termIds.length(); i++) {
				String termId = termIds.getString(i);
				// escape the termId for the Lucene lookup
				termId = QueryParser.escape(termId);
				sb.append(idType).append(":").append(termId);
				if (i < termIds.length() - 1)
					sb.append(" ");
			}
			String termQuery = sb.toString();

			Index<Node> termIndex = graphDb.index().forNodes(INDEX_NAME);
			IndexHits<Node> indexHits = termIndex.query(termQuery);
			ResourceIterator<Node> startNodeIterator = indexHits.iterator();
			Node[] startNodes = new Node[indexHits.size()];
			for (int i = 0; i < indexHits.size(); i++) {
				if (startNodeIterator.hasNext()) {
					startNodes[i] = startNodeIterator.next();
				} else
					throw new IllegalStateException(indexHits.size()
							+ " index hits for start nodes expected but iterator expired unexpectedly.");
			}

			Traverser traverse = td.traverse(startNodes);
			List<String[]> pathsTermIds = new ArrayList<>();
			int c = 0;
			for (Path p : traverse) {
				log.info("Path nr. " + c++ + ":" + p.toString());
				// The length of paths is measured in the number of edges, not
				// nodes, in Neo4j.
				String[] pathTermIds = new String[p.length() + 1];
				Iterator<Node> nodesIt = p.nodes().iterator();
				boolean error = false;
				for (int i = p.length(); i >= 0; i--) {
					Node n;
					if (nodesIt.hasNext())
						n = nodesIt.next();
					else
						throw new IllegalStateException("Length of path wrong, more nodes expected.");
					if (!n.hasProperty(PROP_ID)) {
						log.warn("Came across the term " + n + " (" + NodeUtilities.getNodePropertiesAsString(n)
								+ ") when computing root paths. But this term does not have an ID.");
						error = true;
						break;
					}
					pathTermIds[i] = (String) n.getProperty(PROP_ID);
				}
				if (!error)
					pathsTermIds.add(pathTermIds);
			}
			if (sort)
				Collections.sort(pathsTermIds, new Comparator<String[]>() {
					@Override
					public int compare(String[] o1, String[] o2) {
						return o1.length - o2.length;
					}
				});
			Map<String, Object> pathsWrappedInMap = new HashMap<>();
			pathsWrappedInMap.put(RET_KEY_PATHS, pathsTermIds);
			return new RecursiveMappingRepresentation(Representation.MAP, pathsWrappedInMap);
		}
	}

	/**
	 * Adds an aggregate term. An aggregate term is a term of the following
	 * input form:<br/>
	 * 
	 * <pre>
	 * {
	 *     'aggregate':true,
	 *     'elementSrcIds':['id4','id29','id41']
	 *     'sources':['NCBI Gene'],
	 *     'copyProperties':['prefName','synonyms']
	 * }
	 * </pre>
	 * 
	 * I.e. a representative term that has no distinct properties on its own. It
	 * will get links to the term source IDs given in <code>elementSrcIds</code>
	 * with respect to <code>source</code>. The <code>copyProperties</code>
	 * property contains the properties of element terms that should be copied
	 * into the aggregate and does not have to be present in which case nothing
	 * will be copied. The copy process will NOT be done in this method call but
	 * must be triggered manually via
	 * {@link #copyAggregateProperties(GraphDatabaseService)}.
	 * 
	 * @param graphDb
	 * @param termIndex
	 * @param jsonTerm
	 * @param nodesBySrcId
	 * @param insertionReport
	 * @param importOptions
	 * @return
	 * @throws JSONException
	 */
	private void insertAggregateTerm(GraphDatabaseService graphDb, Index<Node> termIndex, JSONObject jsonTerm,
			Map<String, Node> nodesBySrcId, InsertionReport insertionReport, ImportOptions importOptions)
			throws JSONException {
		JSONObject aggCoordinates = jsonTerm.has(PROP_COORDINATES) ? jsonTerm.getJSONObject(PROP_COORDINATES) : new JSONObject();
		String aggOrgId = JSON.getString(aggCoordinates, CoordinateConstants.ORIGINAL_ID);
		String aggOrgSource = JSON.getString(aggCoordinates, CoordinateConstants.ORIGINAL_SOURCE);
		String aggSrcId = JSON.getString(aggCoordinates, CoordinateConstants.SOURCE_ID);
		String aggSource = JSON.getString(aggCoordinates, CoordinateConstants.SOURCE);
		if (null == aggSource)
			aggSource = UNKNOWN_TERM_SOURCE;
		log.debug("Looking up aggregate (" + aggOrgId + "," + aggOrgSource + ") / (" + aggSrcId + "," + aggSource
				+ "), original/source coordinates.");
		Node aggregate = lookupTerm(new ConceptCoordinates(aggCoordinates, false), termIndex);
		if (null != aggregate) {
			String isHollowMessage = "";
			if (aggregate.hasLabel(TermLabel.HOLLOW))
				isHollowMessage = ", however it is hollow and its properties will be set now.";
			log.debug("    aggregate does already exist" + isHollowMessage);
			if (!aggregate.hasLabel(TermLabel.HOLLOW))
				return;
			// remove the HOLLOW label, we have to aggregate information now and
			// will set it to the node in the following
			aggregate.removeLabel(TermLabel.HOLLOW);
			aggregate.addLabel(TermLabel.AGGREGATE);
		}
		if (aggregate == null) {
			log.debug("    aggregate is being created");
			aggregate = graphDb.createNode(TermLabel.AGGREGATE);
		}
		boolean includeAggreationInHierarchy = jsonTerm.has(AGGREGATE_INCLUDE_IN_HIERARCHY)
				&& jsonTerm.getBoolean(AGGREGATE_INCLUDE_IN_HIERARCHY);
		// If the aggregate is to be included into the hierarchy, it also should
		// be a TERM for path creation
		if (includeAggreationInHierarchy)
			aggregate.addLabel(TermLabel.TERM);
		JSONArray elementCoords = jsonTerm.getJSONArray(TermConstants.ELEMENT_COORDINATES);
		int numElementsFound = 0;
		log.debug("    looking up aggregate elements");
		for (int i = 0; i < elementCoords.length(); i++) {
			JSONObject elementCoord = elementCoords.getJSONObject(i);
			String elementSrcId = elementCoord.getString(TermConstants.COORD_ID);
			String elementSource = elementCoord.getString(TermConstants.COORD_SOURCE);
			if (null == elementSource)
				elementSource = UNKNOWN_TERM_SOURCE;
			Node element = nodesBySrcId.get(elementSrcId);
			if (null != element) {
				String[] srcIds = (String[]) element.getProperty(PROP_SRC_IDS);
				String[] sources = element.hasProperty(PROP_SOURCES) ? (String[]) element.getProperty(PROP_SOURCES)
						: new String[0];
				for (int j = 0; j < srcIds.length; j++) {
					String srcId = srcIds[j];
					String source = sources.length > j ? sources[j] : null;
					// If the source ID matches but not the sources then this is
					// the wrong node.
					if (srcId.equals(elementSrcId)
							&& !((elementSource == null && source == null) || (elementSource.equals(source))))
						element = null;
					else
						break;
				}
				if (null != element)
					log.debug("\tFound element with source ID and source (" + elementSrcId + ", " + elementSource
							+ ") in in-memory map.");
			}
			if (null == element)
				element = lookupTermBySourceId(elementSrcId, elementSource, false, termIndex);
			// this is just a filter; if no sources to filter have been
			// specified, all terms are eligible
			// if (null != sources) {
			// String[] conceptSources = (String[])
			// element.getProperty(PROP_SOURCES);
			// // if sources are given but the concept does not define any,
			// // we exclude it
			// if (null == conceptSources)
			// continue;
			// // we look for a single source match between the concept
			// // sources and the given aggregate sources
			// boolean sourcesMatch = false;
			// for (int j = 0; j < conceptSources.length; j++) {
			// String conceptSource = conceptSources[j];
			// if (sources.contains(conceptSource))
			// sourcesMatch = true;
			// }
			// if (!sourcesMatch)
			// continue;
			// }
			if (null == element && importOptions.createHollowAggregateElements) {
				element = graphDb.createNode(TermLabel.TERM, TermLabel.HOLLOW);
				log.debug("    Creating HOLLOW element with source coordinates (" + elementSrcId + "," + elementSource
						+ ")");
				addToArrayProperty(element, PROP_SRC_IDS, elementSrcId);
				addToArrayProperty(element, PROP_SOURCES, elementSource);
				termIndex.add(element, PROP_SRC_IDS, elementSrcId);
			}
			if (element != null) {
				aggregate.createRelationshipTo(element, EdgeTypes.HAS_ELEMENT);
				++numElementsFound;
			}
		}
		// don't create aggregates with less than two elements, they would be
		// obsolete
		// if (numElementsFound < 2) {
		// log.debug(
		// "The aggregate with source ID {} and elements {} has only a single
		// element in the current database. The aggregate will not be created
		// because it is redundant.",
		// aggSrcId, elementSrcIds);
		// for (Relationship rel : aggregate.getRelationships()) {
		// rel.delete();
		// }
		// aggregate.delete();
		// insertionReport.omittedTerms.add(aggSrcId);
		// } else {

		// now that we know we will keep the aggregate, set its properties
		if (null != aggSrcId) {
			int idIndex = findFirstValueInArrayProperty(aggregate, PROP_SRC_IDS, aggSrcId);
			int sourceIndex = findFirstValueInArrayProperty(aggregate, PROP_SOURCES, aggSource);
			if (!StringUtils.isBlank(aggSrcId)
					&& ((idIndex == -1 && sourceIndex == -1) || (idIndex != sourceIndex))) {
				addToArrayProperty(aggregate, PROP_SRC_IDS, aggSrcId, true);
				addToArrayProperty(aggregate, PROP_SOURCES, aggSource, true);
			}
			// if the aggregate has a source ID, add it to the respective
			// map for later access during the relationship insertion phase
			nodesBySrcId.put(aggSrcId, aggregate);
			termIndex.add(aggregate, PROP_SRC_IDS, aggSrcId);
		}
		if (null != aggOrgId)
			aggregate.setProperty(PROP_ORG_ID, aggOrgId);
		if (null != aggOrgSource)
			aggregate.setProperty(PROP_ORG_SRC, aggOrgSource);
		JSONArray copyProperties = JSON.getJSONArray(jsonTerm, TermConstants.PROP_COPY_PROPERTIES);
		if (null != copyProperties && copyProperties.length() > 0)
			aggregate.setProperty(TermConstants.PROP_COPY_PROPERTIES, JSON.json2JavaArray(copyProperties));

		JSONArray generalLabels = JSON.getJSONArray(jsonTerm, TermConstants.PROP_GENERAL_LABELS);
		for (int i = 0; null != generalLabels && i < generalLabels.length(); i++) {
			aggregate.addLabel(DynamicLabel.label(generalLabels.getString(i)));
		}

		String aggregateId = NodeIDPrefixConstants.AGGREGATE_TERM
				+ SequenceManager.getNextSequenceValue(graphDb, SequenceConstants.SEQ_AGGREGATE_TERM);
		aggregate.setProperty(PROP_ID, aggregateId);
		termIndex.add(aggregate, PROP_ID, aggregateId);

		insertionReport.numTerms++;
		// }
	}

	private void insertFacetTerm(GraphDatabaseService graphDb, String facetId, Index<Node> termIndex,
			JSONObject jsonTerm, Map<String, Node> nodesBySrcId, InsertionReport insertionReport,
			ImportOptions importOptions) throws JSONException {
		// Name is mandatory, thus we don't use the
		// null-convenience method here.
		String prefName = JSON.getString(jsonTerm, PROP_PREF_NAME);
		JSONArray synonyms = JSON.getJSONArray(jsonTerm, PROP_SYNONYMS);
		JSONArray generalLabels = JSON.getJSONArray(jsonTerm, TermConstants.PROP_GENERAL_LABELS);

		JSONObject coordinates = jsonTerm.getJSONObject(PROP_COORDINATES);

		if (!jsonTerm.has(PROP_COORDINATES) || coordinates.length() == 0)
			throw new IllegalArgumentException(
					"The concept " + jsonTerm.toString(2) + " does not specify coordinates.");

		// Source ID is mandatory if we have a real term import and not just a
		// merging operation.
		String srcId = importOptions.merge ? JSON.getString(coordinates, CoordinateConstants.SOURCE_ID)
				: coordinates.getString(CoordinateConstants.SOURCE_ID);
		// The other properties may have values or not, make it
		// null-proof.
		String orgId = JSON.getString(coordinates, CoordinateConstants.ORIGINAL_ID);
		String source = JSON.getString(coordinates, CoordinateConstants.SOURCE);
		String orgSource = JSON.getString(coordinates, CoordinateConstants.ORIGINAL_SOURCE);
		boolean uniqueSourceId = JSON.getBoolean(coordinates, CoordinateConstants.UNIQUE_SOURCE_ID, false);

		boolean hasBeenNewlyCreated = false;
		boolean srcIduniqueMarkerChanged = false;

		if (StringUtils.isBlank(srcId) && !StringUtils.isBlank(orgId)
				&& ((StringUtils.isBlank(source) && !StringUtils.isBlank(orgSource)) || source.equals(orgSource))) {
			srcId = orgId;
			source = orgSource;
		}

		if (StringUtils.isBlank(source))
			source = UNKNOWN_TERM_SOURCE;

		if (StringUtils.isBlank(orgId) ^ StringUtils.isBlank(orgSource))
			throw new IllegalArgumentException(
					"Term to be inserted defines only its original ID or its original source but not both. This is not allowed. The term data was: "
							+ jsonTerm);
		if (importOptions.merge && jsonTerm.has(PARENT_SRC_IDS))
			// The problem is that we use the nodeBySrcId map to check whether
			// relationships have to be created or not.
			// Thus, for relationships we need source IDs. Could be adapted in
			// the future to switch to original IDs if
			// terms do not come with a source ID.
			throw new IllegalArgumentException("Term " + jsonTerm
					+ " is supposed to be merged with an existing database term but defines parents. This is currently not supported in merging mode.");

		// Perhaps we already know this term? This can happen, when
		// multiple
		// sources contain terms from a common origin. E.g.
		// BioPortal
		// ontologies contain terms from MeSH and UniProt.
		// Or some terms are organized to be in multiple facets.
		// How do we identify terms? The tid is given automatically.

		Node term = null;
		term = lookupTerm(new ConceptCoordinates(coordinates), termIndex);
		if (null == term) {
			// If we still haven't found an existing term, we have to create a
			// new one. First check whether we are
			// perhaps in merge-only mode.
			if (importOptions.merge)
				return;
		}
		String termId = null;
		if (null != term && !term.hasLabel(TermLabel.HOLLOW))
			// If we found a term by now, get its ID.
			termId = (String) term.getProperty(TermConstants.PROP_ID);
		if (null == term || term.hasLabel(TermLabel.HOLLOW)) {
			// The term could already exist as a hollow node, e.g. because it
			// was the
			// target of a relationship of another
			// already inserted term. Please not that is currently not
			// possible for the default taxonomic relationship because in the
			// original Semedico term data there are
			// terms specifying parents that don't exist within the data at
			// all.
			if (null == term) {
				// New term, create it.
				term = graphDb.createNode(TermLabel.TERM);
				log.debug("Created new TERM node for original / source coordinates (" + orgId + "," + orgSource
						+ ") / (" + srcId + "," + source + ")");
				hasBeenNewlyCreated = true;
				insertionReport.numTerms++;
			} else {
				// The term already existed, so it was hollow. But now it won't
				// be no more.
				term.removeLabel(TermLabel.HOLLOW);
				// The hollow term had its source ID set by the parent source Id
				// of its children. We remove it so the following algorithms can
				// add it again without handling of already existing properties.
				term.removeProperty(PROP_SRC_IDS);
				term.removeProperty(PROP_SOURCES);
				term.removeProperty(PROP_ORG_ID);
				term.removeProperty(PROP_ORG_SRC);
				insertionReport.addExistingTerm(term);
				// The hollow term possibly was connected as a facet root due to
				// the lack of a better alternative. Those
				// relationships are now removed and later replaced by the
				// actual relationships defined in the new data
				// that is currently processed.
				Iterable<Relationship> relationships = term.getRelationships(EdgeTypes.HAS_ROOT_TERM);
				for (Relationship rel : relationships) {
					Node startNode = rel.getStartNode();
					if (startNode.hasLabel(FacetManager.FacetLabel.FACET))
						rel.delete();
				}
			}
			termId = NodeIDPrefixConstants.TERM
					+ SequenceManager.getNextSequenceValue(graphDb, SequenceConstants.SEQ_TERM);
			term.setProperty(PROP_ID, termId);
			termIndex.putIfAbsent(term, PROP_ID, termId);
		} else if (null != term) {
			// The term existed before we inserted the current data.
			insertionReport.addExistingTerm(term);
		}

		// Merge the new or an already existing term with what we
		// already
		// have, perhaps the
		// stored information
		// and the new information is complementary to each other
		// (if
		// there
		// is any information already stored, the term could be
		// fresh
		// and
		// empty).
		// Currently, just do the following: For non-array property
		// values, set those properties which are currently non
		// existent. For array, merge the arrays.
		PropertyUtilities.mergeJSONObjectIntoPropertyContainer(jsonTerm, term, TermConstants.PROP_GENERAL_LABELS,
				PROP_SRC_IDS, PROP_SOURCES, PROP_UNIQUE_SRC_ID, PROP_SYNONYMS, PARENT_SRC_IDS, PROP_COORDINATES,
				PARENT_COORDINATES, TermConstants.RELATIONSHIPS);
		// set the original ID and source
		if (orgId != null) {
			term.setProperty(PROP_ORG_ID, orgId);
			term.setProperty(PROP_ORG_SRC, orgSource);
		}
		// There could be multiple sources containing a term. For
		// now, we just note that facet (if these sources give the same original
		// ID, otherwise we won't notice) but don't do anything about
		// it. In the future, it could be interesting to link back to the
		// different sources, but this requires quite some more modeling. At
		// least parallel arrays of source IDs and addresses of sources
		// themselves (in a property
		// of their own). Or the sources will be nodes and have
		// relationships to the terms they contain.
		// Check, if the parallel pair of source ID and source already exists.
		// If not, insert it. Unless a source ID
		// wasn't specified.
		int idIndex = findFirstValueInArrayProperty(term, PROP_SRC_IDS, srcId);
		int sourceIndex = findFirstValueInArrayProperty(term, PROP_SOURCES, source);
		// (sourceID, source) coordinate has not been found, create it
		if (!StringUtils.isBlank(srcId) && ((idIndex == -1 && sourceIndex == -1) || (idIndex != sourceIndex))) {
			// on first creation, no concept node has a source ID at this point
			if (term.hasProperty(PROP_SRC_IDS))
				srcIduniqueMarkerChanged = checkUniqueIdMarkerClash(term, srcId, uniqueSourceId);
			addToArrayProperty(term, PROP_SRC_IDS, srcId, true);
			addToArrayProperty(term, PROP_SOURCES, source, true);
			addToArrayProperty(term, PROP_UNIQUE_SRC_ID, uniqueSourceId, true);
		}
		mergeArrayProperty(term, TermConstants.PROP_SYNONYMS, JSON.json2JavaArray(synonyms, prefName));
		addToArrayProperty(term, PROP_FACETS, facetId);

		for (int i = 0; null != generalLabels && i < generalLabels.length(); i++) {
			term.addLabel(DynamicLabel.label(generalLabels.getString(i)));
		}

		// If we have a merging operation, we don't have to care about indexes
		if (!importOptions.merge) {
			// it is actually possible that multiple terms share the same source
			// ID because multiple sources - i.e. databases - might use the same
			// ID scheme (e.g. NCBI Gene and NCBI Taxonomy).
			// Please note that this change - made 10th Feb 2016 - breaks the
			// inner logic in this method and in other methods that assume that
			// source IDs are unique (all calls use getSingle() which will throw
			// an exception on duplicates). So it might be that further fixes -
			// i.e.
			// taking into account the (original) source will be necessary
			if (hasBeenNewlyCreated)
				termIndex.add(term, PROP_SRC_IDS, srcId);
			if (null != orgId)
				termIndex.putIfAbsent(term, PROP_ORG_ID, orgId);
			termIndex.putIfAbsent(term, TermConstants.PROP_ID, termId);
			nodesBySrcId.put(srcId, term);
		}

		if (srcIduniqueMarkerChanged) {
			log.warn("Merging concept nodes with unique source ID " + srcId
					+ " because on term with this source ID and source " + source
					+ " the ID was declared non-unique in the past but unique now. Properties from all nodes are merged together and relationships are moved from obsolete nodes to the single remaining node. This is experimental and might lead to errors.");
			List<Node> obsoleteNodes = new ArrayList<>();
			Node mergedNode = NodeUtilities.mergeConceptNodesWithUniqueSourceId(srcId, termIndex, obsoleteNodes);
			// now move the relationships of all nodes to be removed to the
			// merged node
			for (Node obsoleteNode : obsoleteNodes) {
				Iterable<Relationship> relationships = obsoleteNode.getRelationships();
				for (Relationship rel : relationships) {
					Node startNode = rel.getStartNode();
					Node endNode = rel.getEndNode();
					// replace the obsolete node by the merged node for the new
					// relationships
					if (startNode.getId() == obsoleteNode.getId())
						startNode = mergedNode;
					if (endNode.getId() == obsoleteNode.getId())
						endNode = mergedNode;
					// create the new relationship between the merged node and
					// the other nodes the obsolete node is connected with
					createRelationShipIfNotExists(startNode, endNode, rel.getType(), insertionReport,
							Direction.OUTGOING, rel.getAllProperties());
					// delete the original relationship
					rel.delete();
					// and finally, delete the obsolete node
					obsoleteNode.delete();
				}
			}
		}

		// Check whether the parents of the current term already existed before
		// we inserted the current data. This information is required for
		// relation insertion which is done in a separate step.
		// If the parents exist, add them to the nodesBySrcId map to allow quick
		// lookup for relationship creation.
		// NOTE that this currently can only connect to parents having the same
		// source as the currently inserted concept.
		JSONArray parentCoordinateArray = JSON.getJSONArray(jsonTerm, PARENT_COORDINATES);
		for (int i = 0; null != parentCoordinateArray && i < parentCoordinateArray.length(); i++) {
			ConceptCoordinates parentCoordinates = new ConceptCoordinates(parentCoordinateArray.getJSONObject(i));
			
			Node parent = nodesBySrcId.get(parentCoordinates.sourceId);
			if (null == parent) {
				try {
					parent = lookupTerm(parentCoordinates, termIndex);
				} catch (NoSuchElementException e) {
					throw new IllegalStateException("Error while looking up the parents of term source ID \"" + srcId
							+ "\".\n Parent source ID is: " + parentCoordinates.sourceId, e);
				}
				if (null != parent) {
					nodesBySrcId.put(parentCoordinates.sourceId, parent);
					insertionReport.addExistingTerm(parent);
				}
			}
		}

		if (StringUtils.isBlank(prefName) && !insertionReport.existingTerms.contains(term))
			throw new IllegalArgumentException("Term has no property \"" + PROP_PREF_NAME + "\": " + jsonTerm);

	}

	private boolean checkUniqueIdMarkerClash(Node conceptNode, String srcId, boolean uniqueSourceId) {
		boolean uniqueOnConcept = NodeUtilities.isSourceUnique(conceptNode, srcId);
		// case: the source ID was already set on this concept node and
		// uniqueSourceId was
		// false; then, other concepts might have been inserted with
		// the same source ID marked as unique, but would not have been merged
		// since this concept marks its source ID as not unique (the rule says
		// that then the concept differ). But now the
		// same source ID will be marked as unique which would cause an
		// inconsistent database state because then, the formerly imported
		// concepts with the same unique source ID should have been merged
		if (!uniqueOnConcept && uniqueOnConcept != uniqueSourceId)
			return true;
		return false;
	}

	private InsertionReport insertFacetTerms(GraphDatabaseService graphDb, JSONArray jsonTerms, String facetId,
			Map<String, Node> nodesBySrcId, ImportOptions importOptions) throws JSONException {
		long time = System.currentTimeMillis();
		InsertionReport insertionReport = new InsertionReport();
		// Idea: First create all nodes and just store which Node has which
		// parent. Then, after all nodes have been created, do the actual
		// connection.
		Index<Node> termIndex = null;
		IndexManager indexManager = graphDb.index();
		termIndex = indexManager.forNodes(INDEX_NAME);
		log.info("Starting to insert " + jsonTerms.length() + " terms.");
		for (int i = 0; i < jsonTerms.length(); i++) {
			JSONObject jsonTerm = jsonTerms.getJSONObject(i);
			boolean isAggregate = JSON.getBoolean(jsonTerm, TermConstants.AGGREGATE);
			if (isAggregate) {
				insertAggregateTerm(graphDb, termIndex, jsonTerm, nodesBySrcId, insertionReport, importOptions);
			} else {
				insertFacetTerm(graphDb, facetId, termIndex, jsonTerm, nodesBySrcId, insertionReport, importOptions);
			}
		}
		log.debug(jsonTerms.length() + " terms inserted.");
		time = System.currentTimeMillis() - time;
		log.info(insertionReport.numTerms + " new terms - but not yet relationships - have been inserted. This took "
				+ time + " ms (" + (time / 1000) + " s)");
		return insertionReport;
	}

	public Representation insertFacetTerms(GraphDatabaseService graphDb, String termsAndFacetJson)
			throws JSONException {
		JSONObject input = new JSONObject(termsAndFacetJson);
		JSONObject jsonFacet = JSON.getJSONObject(input, KEY_FACET);
		JSONArray jsonTerms = input.getJSONArray(KEY_TERMS);
		JSONObject importOptionsJson = JSON.getJSONObject(input, KEY_IMPORT_OPTIONS);
		return insertFacetTerms(graphDb, jsonFacet != null ? jsonFacet.toString() : null, jsonTerms.toString(),
				importOptionsJson != null ? importOptionsJson.toString() : null);
	}

	@Name(INSERT_TERMS)
	// TODO
	@Description("TODO")
	@PluginTarget(GraphDatabaseService.class)
	public Representation insertFacetTerms(@Source GraphDatabaseService graphDb,
			@Description("TODO") @Parameter(name = KEY_FACET, optional = true) String facetJson,
			@Description("TODO") @Parameter(name = KEY_TERMS, optional = true) String termsJson,
			@Description("TODO") @Parameter(name = KEY_IMPORT_OPTIONS, optional = true) String importOptionsJsonString)
			throws JSONException {
		log.info(INSERT_TERMS + " was called");
		long time = System.currentTimeMillis();
		// Data incoming, create indexes if not present.
		// No, don't. The unique constraint slows things painfully down.
		// createSchemaIndexes(graphDb);

		Gson gson = new Gson();
		log.debug("Parsing input.");
		JSONObject jsonFacet = null;
		JSONArray jsonTerms = null;
		JSONObject importOptionsJson = null;
		jsonFacet = !StringUtils.isEmpty(facetJson) ? new JSONObject(facetJson) : null;
		if (null != termsJson)
			jsonTerms = new JSONArray(termsJson);
		ImportOptions importOptions;
		if (null != importOptionsJsonString) {
			importOptionsJson = new JSONObject(importOptionsJsonString);
			importOptions = gson.fromJson(importOptionsJson.toString(), ImportOptions.class);
		} else {
			importOptions = new ImportOptions();
		}

		Map<String, Object> report = new HashMap<>();
		InsertionReport insertionReport = new InsertionReport();
		log.debug("Beginning processing of term insertion.");
		try (Transaction tx = graphDb.beginTx()) {
			Node facet = null;
			String facetId = null;
			// The facet Id will be added to the facets-property of the term
			// nodes.
			log.debug("Handling import of facet.");
			if (null != jsonFacet && jsonFacet.has(PROP_ID)) {
				facetId = jsonFacet.getString(PROP_ID);
				log.info("Facet ID " + facetId + " has been given to add the terms to.");
				boolean isNoFacet = JSON.getBoolean(jsonFacet, FacetConstants.NO_FACET);
				if (isNoFacet)
					facet = FacetManager.getNoFacet(graphDb, facetId);
				else
					facet = FacetManager.getFacetNode(graphDb, facetId);
				if (null == facet)
					throw new IllegalArgumentException("The facet with ID \"" + facetId
							+ "\" was not found. You must pass the ID of an existing facet or deliver all information required to create the facet from scratch. Then, the facetId must not be included in the request, it will be created dynamically.");
			} else if (null != jsonFacet && jsonFacet.has(FacetConstants.PROP_NAME)) {
				ResourceIterator<Node> facetIterator = graphDb.findNodes(FacetLabel.FACET);
				while (facetIterator.hasNext()) {
					facet = (Node) facetIterator.next();
					if (facet.getProperty(FacetConstants.PROP_NAME)
							.equals(jsonFacet.getString(FacetConstants.PROP_NAME)))
						break;
					facet = null;
				}

			}
			if (null != jsonFacet && null == facet) {
				// No existing ID is given, create a new facet.
				facet = FacetManager.createFacet(graphDb, jsonFacet);
			}
			if (null != facet)
				facetId = (String) facet.getProperty(PROP_ID);
			log.debug("Facet " + facetId + " was successfully created or determined by ID.");

			if (null != jsonTerms) {
				log.debug("Beginning to create term nodes and relationships.");
				Map<String, Node> nodesBySrcId = new HashMap<>(jsonTerms.length());
				insertionReport = insertFacetTerms(graphDb, jsonTerms, facetId, nodesBySrcId, importOptions);
				// If the nodesBySrcId map is empty we either have no terms or
				// at least no terms with a source ID. Then,
				// relationship creation is currently not supported.
				if (!nodesBySrcId.isEmpty())
					createRelationships(graphDb, jsonTerms, facet, nodesBySrcId, importOptions, insertionReport);
				time = System.currentTimeMillis() - time;
				report.put(RET_KEY_NUM_CREATED_TERMS, insertionReport.numTerms);
				report.put(RET_KEY_NUM_CREATED_RELS, insertionReport.numRelationships);
				report.put(KEY_FACET_ID, facetId);
				report.put(KEY_TIME, time);
				log.debug("Done creating terms and relationships.");
			} else {
				log.info("No terms were included in the request.");
			}

			tx.success();
		}
		log.info("Term insertion complete.");
		log.info(INSERT_TERMS + " is finished processing after " + time + " ms. " + insertionReport.numTerms
				+ " terms and " + insertionReport.numRelationships + " relationships have been created.");
		return new RecursiveMappingRepresentation(Representation.MAP, report);
	}

	/**
	 * RULE: Two terms are equal, iff they have the same original source ID
	 * assigned from the same original source or both have no contradicting
	 * original ID and original source but the same source ID and source.
	 * Contradicting means two non-null values that are not equal.
	 * 
	 * @param orgId
	 * @param orgSource
	 * @param srcId
	 * @param source
	 * @param termIndex
	 * @return
	 */
	private Node lookupTerm(ConceptCoordinates coordinates,
			Index<Node> termIndex) {
		String orgId = coordinates.originalId;
		String orgSource = coordinates.originalSource;
		String srcId = coordinates.sourceId;
		String source = coordinates.source;
		boolean uniqueSourceId = coordinates.uniqueSourceId;
		log.debug("Looking up term via original ID and source ({}, {}) and source ID and source ({}, {}).",
				new Object[] { orgId, orgSource, srcId, source });
		if ((null == orgId || null == orgSource) && (null == srcId || null == source)) {
			// no source information is complete, per definition we cannot find
			// an equal term
			log.debug("Neither original ID and original source nor source ID and source were given, returning null.");
			return null;
		}
		Node term;
		// Do we know the original ID?
		term = null != orgId ? termIndex.get(PROP_ORG_ID, orgId).getSingle() : null;
		if (term != null)
			log.debug("Found term by original ID " + orgId);
		// 1. Check if there is a term with the given original ID and a matching
		// original source.
		if (null != term) {
			if (!PropertyUtilities.hasSamePropertyValue(term, PROP_ORG_SRC, orgSource)) {
				log.debug("Original source doesn't match; requested: " + orgSource + ", found term has: "
						+ NodeUtilities.getString(term, PROP_ORG_SRC));
				term = null;
			} else {
				log.debug("Found existing term for original ID " + orgId + " and original source " + orgSource);
			}
		}
		// 2. If there was no original ID, check for a term with the same source
		// ID and source and a non-contradicting original ID.
		if (null == term && null != srcId) {
			term = lookupTermBySourceId(srcId, source, uniqueSourceId, termIndex);
			if (null != term) {
				// check for an original ID contradiction
				Object existingOrgId = NodeUtilities.getNonNullNodeProperty(term, PROP_ORG_ID);
				Object existingOrgSrc = NodeUtilities.getNonNullNodeProperty(term, PROP_ORG_SRC);
				if (null != existingOrgId && null != existingOrgSrc && null != orgId && null != orgSource) {
					if (!existingOrgId.equals(orgId) || !existingOrgSrc.equals(orgSource)) {
						throw new IllegalStateException(String.format(
								"Inconsistent data: A newly imported term has original ID, original source (%s, %s) "
										+ "and source ID, source (%s, %s); the latter matches the found term with ID %s "
										+ "but a this term has an original ID and source (%s, %s)",
								orgId, orgSource, srcId, source, NodeUtilities.getNonNullNodeProperty(term, PROP_ID),
								existingOrgId, existingOrgSrc));
					}
				}
			}
		}
		if (null == term)
			log.debug(
					"    Did not find an existing term with original ID and source ({}, {}) or source ID and source ({}, {}).",
					new Object[] { orgId, orgSource, srcId, source });
		return term;
	}

	/**
	 * Returns the concept node with source ID <tt>srcId</tt> given from source
	 * <tt>source</tt> or <tt>null</tt> if no such node exists.
	 * 
	 * @param srcId
	 *            The source ID of the requested concept node.
	 * @param source
	 *            The source in which the concept node should be given
	 *            <tt>srcId</tt> as a source ID.
	 * @param uniqueSourceId
	 *            Whether the ID should be unique, independently from the
	 *            source. This holds, for example, for ontology class IRIs.
	 * @param termIndex
	 *            The term index.
	 * @return The requested concept node or <tt>null</tt> if no such node is
	 *         found.
	 */
	private Node lookupTermBySourceId(String srcId, String source, boolean uniqueSourceId, Index<Node> termIndex) {
		log.debug("Trying to look up existing term by source ID and source ({}, {})", srcId, source);
		IndexHits<Node> indexHits = termIndex.get(PROP_SRC_IDS, srcId);
		if (!indexHits.hasNext())
			log.debug("    Did not find any term with source ID " + srcId);

		Node soughtConcept = null;
		boolean uniqueSourceIdNodeFound = false;

		while (indexHits.hasNext()) {
			Node conceptNode = (Node) indexHits.next();
			if (null != conceptNode) {
				// The rule goes as follows: Two concepts that share a source ID
				// which is marked as being unique on both terms are equal. If
				// on at least one concept the source ID is not marked as
				// unique, the concepts are different.
				if (uniqueSourceId) {
					boolean uniqueOnConceptNode = NodeUtilities.isSourceUnique(conceptNode, srcId);
					if (uniqueOnConceptNode) {
						if (soughtConcept == null)
							soughtConcept = conceptNode;
						else if (uniqueSourceIdNodeFound == true)
							throw new IllegalStateException("There are multiple concept nodes with unique source ID "
									+ srcId
									+ ". This means that some sources define the ID as unique and others not. This can lead to an inconsistent database as happened in this case.");
						log.debug("    Found existing term with unique source ID " + srcId
								+ " which matches given unique source ID");
						uniqueSourceIdNodeFound = true;
					}
				}

				Set<String> sources = NodeUtilities.getSourcesForSourceId(conceptNode, srcId);
				if (!sources.contains(source)) {
					log.debug("    Did not find a match for source ID " + srcId + " and source " + source);
					conceptNode = null;
				} else {
					log.debug("    Found existing term for source ID " + srcId + " and source " + source);
				}
				if (soughtConcept == null)
					soughtConcept = conceptNode;
				// if soughtConcept is not null, we already found a matching
				// concept in the last iteration
				else if (!uniqueSourceIdNodeFound)
					throw new IllegalStateException(
							"There are multiple concept nodes with source ID " + srcId + " and source " + source);
			}
		}
		return soughtConcept;
	}

	@Name(POP_TERMS_FROM_SET)
	// TODO
	@Description("TODO")
	@PluginTarget(GraphDatabaseService.class)
	public Representation popTermsFromSet(@Source GraphDatabaseService graphDb,
			@Description("TODO") @Parameter(name = KEY_LABEL) String labelString,
			@Description("TODO") @Parameter(name = KEY_AMOUNT) int amount) {
		Label label = DynamicLabel.label(labelString);
		List<Node> poppedTerms = new ArrayList<>(amount);

		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> nodesWithLabel = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(label);
			{
				Node term = null;
				int popCount = 0;
				for (Iterator<Node> it = nodesWithLabel.iterator(); it.hasNext() && popCount < amount; popCount++) {
					term = it.next();
					poppedTerms.add(term);
				}
			}
			tx.success();
		}
		try (Transaction tx = graphDb.beginTx()) {
			// Remove the retrieved terms from the set.
			for (Node term : poppedTerms) {
				term.removeLabel(label);
			}
			tx.success();
		}
		Map<String, Object> retMap = new HashMap<>();
		retMap.put(RET_KEY_TERMS, poppedTerms);
		return new RecursiveMappingRepresentation(Representation.MAP, retMap);
	}

	@Name(PUSH_TERMS_TO_SET)
	@Description("TODO")
	@PluginTarget(GraphDatabaseService.class)
	public long pushTermsToSet(@Source GraphDatabaseService graphDb,
			@Description("TODO") @Parameter(name = KEY_TERM_PUSH_CMD) String termPushCommandString,
			@Description("The amount of terms to push into the set. If equal or less than zero or omitted, all terms will be pushed.") @Parameter(name = KEY_AMOUNT, optional = true) Integer amount) {
		Gson gson = new Gson();
		PushTermsToSetCommand cmd = gson.fromJson(termPushCommandString, PushTermsToSetCommand.class);
		Label setLabel = DynamicLabel.label(cmd.setName);
		Set<String> facetsWithSpecifiedGeneralLabel = new HashSet<>();
		TermSelectionDefinition eligibleTermDefinition = cmd.eligibleTermDefinition;
		TermSelectionDefinition excludeDefinition = cmd.excludeTermDefinition;
		Label facetLabel = null;
		if (null != eligibleTermDefinition && null != eligibleTermDefinition.facetLabel)
			facetLabel = DynamicLabel.label(eligibleTermDefinition.facetLabel);
		String facetPropertyKey = null != eligibleTermDefinition ? eligibleTermDefinition.facetPropertyKey : "*";
		String facetPropertyValue = null != eligibleTermDefinition ? eligibleTermDefinition.facetPropertyValue : "*";

		// Get the facets for which we want to push terms into the set.
		try (Transaction tx = graphDb.beginTx()) {
			Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			TraversalDescription facetTraversal = PredefinedTraversals.getFacetTraversal(graphDb, facetPropertyKey,
					facetPropertyValue);
			Traverser traverse = facetTraversal.traverse(facetGroupsNode);
			for (Path path : traverse) {
				Node facetNode = path.endNode();
				// Filter for allowed facet labels.
				if (null != facetLabel && !facetNode.hasLabel(facetLabel))
					continue;
				facetsWithSpecifiedGeneralLabel.add((String) facetNode.getProperty(FacetConstants.PROP_ID));
			}
			tx.success();
		}

		log.info("Determined " + facetsWithSpecifiedGeneralLabel.size() + " facets with given restrictions.");

		long numberOfTermsAdded = 0;
		long numberOfTermsToAdd = null != amount && amount > 0 ? amount : Long.MAX_VALUE;
		try (Transaction tx = graphDb.beginTx()) {
			Label eligibleTermLabel = TermLabel.TERM;
			if (null != eligibleTermDefinition && !StringUtils.isBlank(eligibleTermDefinition.termLabel))
				eligibleTermLabel = DynamicLabel.label(eligibleTermDefinition.termLabel);
			ResourceIterable<Node> allTerms = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(eligibleTermLabel);
			ResourceIterator<Node> termIt = allTerms.iterator();
			while (termIt.hasNext() && numberOfTermsAdded < numberOfTermsToAdd) {

				// Since Neo4j 2.0.0M6 it is not really possible to do this in a
				// batch manner because the term iterator
				// access must happen inside a transaction.
				// try (Transaction tx = graphDb.beginTx()) {
				for (int i = 0; termIt.hasNext() && i < TERM_INSERT_BATCH_SIZE
						&& numberOfTermsAdded < numberOfTermsToAdd; i++) {
					Node term = termIt.next();

					if (null != excludeDefinition) {
						boolean exclude = false;
						String termPropertyKey = excludeDefinition.termPropertyKey;
						String termPropertyValue = excludeDefinition.termPropertyValue;
						Label termLabel = excludeDefinition.termLabel == null ? null
								: DynamicLabel.label(excludeDefinition.termLabel);
						if (term.hasProperty(termPropertyKey)) {
							Object property = term.getProperty(termPropertyKey);
							if (property.getClass().isArray()) {
								Object[] propertyArray = (Object[]) property;
								for (Object o : propertyArray) {
									if (o.equals(termPropertyValue)) {
										exclude = true;
										break;
									}
								}
							} else {
								if (property.equals(termPropertyValue))
									exclude = true;
							}
						}
						if (term.hasLabel(termLabel))
							exclude = true;
						if (exclude)
							continue;
					}

					boolean hasFacetWithCorrectGeneralLabel = false;
					if (null != eligibleTermDefinition) {
						if (term.hasLabel(TermLabel.HOLLOW))
							continue;
						if (!term.hasProperty(PROP_FACETS)) {
							log.warn("Term with internal ID " + term.getId() + " has no facets property.");
							continue;
						}
						String[] facetIds = (String[]) term.getProperty(PROP_FACETS);
						for (String facetId : facetIds) {
							if (facetsWithSpecifiedGeneralLabel.contains(facetId)) {
								hasFacetWithCorrectGeneralLabel = true;
								break;
							}
						}
					}
					if (hasFacetWithCorrectGeneralLabel || null == eligibleTermDefinition) {
						term.addLabel(setLabel);
						numberOfTermsAdded++;
					}
				}
			}
			tx.success();
		}
		log.info("Finished pushing " + numberOfTermsAdded + " terms to set \"" + cmd.setName + "\".");
		return numberOfTermsAdded;
	}

	@Name(UPDATE_CHILDREN_INFORMATION)
	@Description("Updates - or creates - the information which term has children in which facets."
			+ " This information is used in Semedico to either render an 'opening' arrow next to"
			+ " a term to display its children, or no 'drill-down' option depending on whether"
			+ " the term in question has children in the facet it is shown in or not.")
	@PluginTarget(GraphDatabaseService.class)
	public String updateChildrenInformation(@Source GraphDatabaseService graphDb) {
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterator<Node> termIt = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(TermLabel.TERM)
					.iterator();
			while (termIt.hasNext()) {
				Node term = termIt.next();
				Iterator<Relationship> relIt = term.getRelationships(Direction.OUTGOING).iterator();
				Set<String> facetsContainingChildren = new HashSet<>();
				while (relIt.hasNext()) {
					Relationship rel = relIt.next();
					String type = rel.getType().name();
					if (type.startsWith(EdgeTypes.IS_BROADER_THAN.toString())) {
						String[] typeNameParts = type.split("_");
						String lastPart = typeNameParts[typeNameParts.length - 1];
						if (lastPart.startsWith(NodeIDPrefixConstants.FACET)) {
							facetsContainingChildren.add(lastPart);
						}
					}
				}
				if (facetsContainingChildren.size() == 0 && term.hasProperty(PROP_CHILDREN_IN_FACETS))
					term.removeProperty(PROP_CHILDREN_IN_FACETS);
				else if (facetsContainingChildren.size() > 0)
					term.setProperty(PROP_CHILDREN_IN_FACETS,
							facetsContainingChildren.toArray(new String[facetsContainingChildren.size()]));
			}
			tx.success();
			return "success";
		}
	}

	@Name("include_terms")
	@Description("This is only a remedy for a problem we shouldnt have, delete in the future.")
	@PluginTarget(GraphDatabaseService.class)
	public void includeTerms(@Source GraphDatabaseService graphDb) throws JSONException {
		Label includeLabel = DynamicLabel.label("INCLUDE");
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> terms = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(includeLabel);
			for (Node term : terms)
				term.removeLabel(includeLabel);
			tx.success();
		}

		Set<String> facetIds = new HashSet<>();
		try (Transaction tx = graphDb.beginTx()) {
			Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			TraversalDescription facetTraversal = PredefinedTraversals.getFacetTraversal(graphDb, null, null);
			Traverser traverse = facetTraversal.traverse(facetGroupsNode);
			for (Path path : traverse) {
				Node facetNode = path.endNode();
				facetIds.add((String) facetNode.getProperty(FacetConstants.PROP_ID));
			}
			log.info("Including terms from facets " + facetIds);
			tx.success();
		}
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> terms = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(TermLabel.TERM);
			for (Node term : terms) {
				if (!term.hasProperty(PROP_FACETS)) {
					log.info("Doesnt have facets: " + PropertyUtilities.getNodePropertiesAsString(term));
					continue;
				}
				String[] facets = (String[]) term.getProperty(PROP_FACETS);
				for (int i = 0; i < facets.length; i++) {
					String string = facets[i];
					if (facetIds.contains(string)) {
						term.addLabel(includeLabel);
						break;
					}
				}
			}
			tx.success();
		}
	}

	@Name("exclude_terms")
	@Description("This is only a remedy for a problem we shouldnt have, delete in the future.")
	@PluginTarget(GraphDatabaseService.class)
	public void excludeTerms(@Source GraphDatabaseService graphDb) throws JSONException {
		Label includeLabel = DynamicLabel.label("INCLUDE");
		Label excludeLabel = DynamicLabel.label("EXCLUDE");
		Label mappingAggregateLabel = DynamicLabel.label("MAPPING_AGGREGATE");
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> terms = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(TermLabel.TERM);
			for (Node term : terms) {
				if (!term.hasLabel(includeLabel)) {
					term.addLabel(excludeLabel);
					term.removeLabel(mappingAggregateLabel);
					term.removeLabel(TermLabel.AGGREGATE);
					term.removeLabel(TermLabel.TERM);
				} else {
					term.addLabel(mappingAggregateLabel);
				}
			}
			tx.success();
		}
		try (Transaction tx = graphDb.beginTx()) {
			ResourceIterable<Node> terms = GlobalGraphOperations.at(graphDb).getAllNodesWithLabel(TermLabel.AGGREGATE);
			for (Node a : terms) {
				a.removeLabel(TermLabel.AGGREGATE);
				a.removeLabel(mappingAggregateLabel);
			}
			tx.success();
		}

	}

	@Name(INSERT_MAPPINGS)
	@Description("Adds a set of term mappings to the database. Here, a 'mapping'"
			+ " between two terms means that those terms are 'similar' to one"
			+ " another. The actual similarity - e.g. 'equal' or 'related' - is"
			+ " defined by the type of the mapping. Here, all mappings are interpreted"
			+ " as being symmetric. That does not mean that two relationships are created"
			+ " but that reading commands don't care about the relationship direction.")
	@PluginTarget(GraphDatabaseService.class)
	public int insertMappings(@Source GraphDatabaseService graphDb,
			@Description("An array of mappings in JSON format. Each mapping is an object with the keys for \"id1\", \"id2\" and \"mappingType\", respectively.") @Parameter(name = KEY_MAPPINGS) String mappingsJson)
			throws JSONException {
		JSONArray mappings = new JSONArray(mappingsJson);
		log.info("Starting to insert " + mappings.length() + " mappings.");
		try (Transaction tx = graphDb.beginTx()) {
			Index<Node> termIndex = graphDb.index().forNodes(TermConstants.INDEX_NAME);
			Map<String, Node> nodesBySrcId = new HashMap<>(mappings.length());
			InsertionReport insertionReport = new InsertionReport();

			for (int i = 0; i < mappings.length(); i++) {
				JSONObject mapping = mappings.getJSONObject(i);
				String id1 = mapping.getString("id1");
				String id2 = mapping.getString("id2");
				String mappingType = mapping.getString("mappingType");

				log.debug("Inserting mapping " + id1 + " -" + mappingType + "- " + id2);

				if (StringUtils.isBlank(id1))
					throw new IllegalArgumentException("id1 in mapping \"" + mapping + "\" is missing.");
				if (StringUtils.isBlank(id2))
					throw new IllegalArgumentException("id2 in mapping \"" + mapping + "\" is missing.");
				if (StringUtils.isBlank(mappingType))
					throw new IllegalArgumentException("mappingType in mapping \"" + mapping + "\" is missing.");

				Node n1 = nodesBySrcId.get(id1);
				if (null == n1) {
					IndexHits<Node> indexHits = termIndex.get(PROP_SRC_IDS, id1);
					if (indexHits.size() > 1) {
						log.error("More than one node for source ID {}", id1);
						for (Node n : indexHits)
							log.error(NodeUtilities.getNodePropertiesAsString(n));
						throw new IllegalStateException("More than one node for source ID " + id1);
					}
					n1 = indexHits.getSingle();
					if (null == n1) {
						log.warn("There is no term with source ID \"" + id1 + "\" as required by the mapping \""
								+ mapping + "\" Mapping is skipped.");
						continue;
					}
					nodesBySrcId.put(id1, n1);
				}
				Node n2 = nodesBySrcId.get(id2);
				if (null == n2) {
					n2 = termIndex.get(PROP_SRC_IDS, id2).getSingle();
					if (null == n2) {
						log.warn("There is no term with source ID \"" + id2 + "\" as required by the mapping \""
								+ mapping + "\" Mapping is skipped.");
						continue;
					}
					nodesBySrcId.put(id2, n2);
				}
				if (mappingType.equalsIgnoreCase("LOOM")) {
					// Exclude mappings that map classes within the same
					// ontology. LOOM as delivered from BioPortal does this but
					// all I saw were errors.
					String[] n1Facets = (String[]) n1.getProperty(PROP_FACETS);
					String[] n2Facets = (String[]) n2.getProperty(PROP_FACETS);
					Set<String> n1FacetSet = new HashSet<>();
					Set<String> n2FacetSet = new HashSet<>();
					for (int j = 0; j < n1Facets.length; j++) {
						String facet = n1Facets[j];
						n1FacetSet.add(facet);
					}
					for (int j = 0; j < n2Facets.length; j++) {
						String facet = n2Facets[j];
						n2FacetSet.add(facet);
					}
					if (!Sets.intersection(n1FacetSet, n2FacetSet).isEmpty()) {
						// Of course an ontology might contain two equivalent
						// classes; possible they are even asserted to be equal.
						// But this is nothing LOOM would detect.
						log.debug("Omitting LOOM mapping between " + id1 + " and " + id2
								+ " because both concepts appear in the same terminology. We assume that the terminology does not have two equal terms and that LOOM is wrong here.");
						continue;
					}
				}
				insertionReport.addExistingTerm(n1);
				insertionReport.addExistingTerm(n2);
				createRelationShipIfNotExists(n1, n2, EdgeTypes.IS_MAPPED_TO, insertionReport, Direction.BOTH,
						TermRelationConstants.PROP_MAPPING_TYPE, new String[] { mappingType });
			}
			tx.success();
			log.info(insertionReport.numRelationships + " of " + mappings.length()
					+ " new mappings successfully added.");
			return insertionReport.numRelationships;
		}
	}

	@Name(GET_FACET_ROOTS)
	@Description("Returns root terms for the facets with specified IDs. Can also be restricted to particular roots which is useful for facets that have a lot of roots.")
	@PluginTarget(GraphDatabaseService.class)
	public MappingRepresentation getFacetRoots(@Source GraphDatabaseService graphDb,
			@Description("An array of facet IDs in JSON format.") @Parameter(name = KEY_FACET_IDS) String facetIdsJson,
			@Description("An array of term IDs to restrict the retrieval to.") @Parameter(name = KEY_TERM_IDS, optional = true) String termIdsJson,
			@Description("Restricts the facets to those that have at most the specified number of roots.") @Parameter(name = KEY_MAX_ROOTS, optional = true) long maxRoots)
			throws JSONException {
		Map<String, Object> facetRoots = new HashMap<>();

		JSONArray facetIdsArray = new JSONArray(facetIdsJson);
		Set<String> requestedFacetIds = new HashSet<>();
		for (int i = 0; i < facetIdsArray.length(); i++)
			requestedFacetIds.add(facetIdsArray.getString(i));

		Map<String, Set<String>> requestedTermIds = null;
		if (!StringUtils.isBlank(termIdsJson) && !termIdsJson.equals("null")) {
			JSONObject termIdsObject = new JSONObject(termIdsJson);
			requestedTermIds = new HashMap<>();
			JSONArray facetIds = termIdsObject.names();
			for (int i = 0; null != facetIds && i < facetIds.length(); i++) {
				String facetId = facetIds.getString(i);
				JSONArray requestedRootIdsForFacet = termIdsObject.getJSONArray(facetId);
				Set<String> idSet = new HashSet<>();
				for (int j = 0; j < requestedRootIdsForFacet.length(); j++)
					idSet.add(requestedRootIdsForFacet.getString(j));
				requestedTermIds.put(facetId, idSet);
			}
		}

		try (Transaction tx = graphDb.beginTx()) {
			log.info("Returning roots for facets " + requestedFacetIds);
			Node facetGroupsNode = FacetManager.getFacetGroupsNode(graphDb);
			TraversalDescription facetTraversal = PredefinedTraversals.getFacetTraversal(graphDb, null, null);
			Traverser traverse = facetTraversal.traverse(facetGroupsNode);
			for (Path path : traverse) {
				Node facetNode = path.endNode();
				String facetId = (String) facetNode.getProperty(FacetConstants.PROP_ID);
				if (maxRoots > 0 && facetNode.hasProperty(FacetConstants.PROP_NUM_ROOT_TERMS)
						&& (long) facetNode.getProperty(FacetConstants.PROP_NUM_ROOT_TERMS) > maxRoots) {
					log.info("Skipping facet with ID " + facetId + " because it has more than " + maxRoots
							+ " root terms (" + facetNode.getProperty(FacetConstants.PROP_NUM_ROOT_TERMS) + ").");
				}
				Set<String> requestedIdSet = null;
				if (null != requestedTermIds)
					requestedIdSet = requestedTermIds.get(facetId);
				if (requestedFacetIds.contains(facetId)) {
					List<Node> roots = new ArrayList<>();
					Iterable<Relationship> relationships = facetNode.getRelationships(Direction.OUTGOING,
							EdgeTypes.HAS_ROOT_TERM);
					for (Relationship rel : relationships) {
						Node rootTerm = rel.getEndNode();
						boolean include = true;
						if (null != requestedIdSet) {
							String rootId = (String) rootTerm.getProperty(PROP_ID);
							if (!requestedIdSet.contains(rootId))
								include = false;
						}
						if (include)
							roots.add(rootTerm);
					}
					if (!roots.isEmpty() && (maxRoots <= 0 || roots.size() <= maxRoots))
						facetRoots.put(facetId, roots);
					else
						log.info("Skipping facet with ID " + facetId + " because it has more than " + maxRoots
								+ " root terms (" + roots.size() + ").");
				}
			}
			tx.success();
		}

		return new RecursiveMappingRepresentation(Representation.MAP, facetRoots);
	}

	@Name(ADD_TERM_VARIANTS)
	@Description("Returns root terms for the facets with specified IDs. Can also be restricted to particular roots which is useful for facets that have a lot of roots.")
	@PluginTarget(GraphDatabaseService.class)
	public void addWritingVariants(@Source GraphDatabaseService graphDb,
			@Description("A JSON object mapping term IDs to an array of writing variants to add to the existing writing variants.") @Parameter(name = KEY_TERM_VARIANTS, optional = true) String termVariants,
			@Description("A JSON object mapping term IDs to an array of acronyms to add to the existing term acronyms.") @Parameter(name = KEY_TERM_ACRONYMS, optional = true) String termAcronyms)
			throws JSONException {
		if (null != termVariants)
			addConceptVariant(graphDb, termVariants, "writingVariants");
		if (null != termAcronyms)
			addConceptVariant(graphDb, termAcronyms, "acronyms");
	}

	private void addConceptVariant(GraphDatabaseService graphDb, String termVariants, String type) {
		System.out.println(termVariants);
		Label variantsAggregationLabel;
		Label variantNodeLabel;
		// Neo4j doesn't allow null values so this will be our replacement
		final String nullElement = "<null>";
		EdgeTypes variantRelationshipType;
		if (type.equals("writingVariants")) {
			variantsAggregationLabel = MorphoLabel.WRITING_VARIANTS;
			variantNodeLabel = MorphoLabel.WRITING_VARIANT;
			variantRelationshipType = EdgeTypes.HAS_VARIANTS;
		} else if (type.equals("acronyms")) {
			variantsAggregationLabel = MorphoLabel.ACRONYMS;
			variantNodeLabel = MorphoLabel.ACRONYM;
			variantRelationshipType = EdgeTypes.HAS_ACRONYMS;
		} else
			throw new IllegalArgumentException("Unknown lexico-morphological type \"" + type + "\".");
		try (StringReader stringReader = new StringReader(termVariants)) {
			JsonReader jsonReader = new JsonReader(stringReader);
			try (Transaction tx = graphDb.beginTx()) {
				// object holding the term IDs mapped to their respective
				// writing variant object
				jsonReader.beginObject();
				while (jsonReader.hasNext()) {
					String termId = jsonReader.nextName();
					// Conceptually, this is a "Map<DocId, Map<Variants,
					// Count>>"
					Map<String, Map<String, Integer>> variantCountsInDocs = new HashMap<>();
					// object mapping document IDs to variant count objects
					jsonReader.beginObject();
					while (jsonReader.hasNext()) {
						String docId = jsonReader.nextName();

						// object mapping variants to counts
						jsonReader.beginObject();
						TreeMap<String, Integer> variantCounts = new TreeMap<>(new TermVariantComparator());
						while (jsonReader.hasNext()) {
							String variant = jsonReader.nextName();
							int count = jsonReader.nextInt();
							if (variantCounts.containsKey(variant)) {
								// may happen if the tree map comparator deems
								// the
								// variant equal to another variant (most
								// probably
								// due to case normalization)
								// add the count of the two variants deemed to
								// be
								// "equal"
								Integer currentCount = variantCounts.get(variant);
								variantCounts.put(variant, currentCount + count);
							} else {
								variantCounts.put(variant, count);
							}
						}
						jsonReader.endObject();
						variantCountsInDocs.put(docId, variantCounts);
					}
					jsonReader.endObject();

					if (variantCountsInDocs.isEmpty()) {
						log.debug("Term with ID " + termId + " has no writing variants / acronyms attached.");
						continue;
					}
					Node term = graphDb.findNode(TermLabel.TERM, PROP_ID, termId);
					if (null == term) {
						log.warn("Term with ID " + termId + " was not found, cannot add writing variants / acronyms.");
						continue;
					}

					// String prefName = (String)
					// term.getProperty(PROP_PREF_NAME);
					// String[] synonyms = null;
					// if (term.hasProperty(PROP_SYNONYMS))
					// synonyms = (String[]) term.getProperty(PROP_SYNONYMS);
					//
					// // Subtract the preferred name and the synonyms from the
					// // variant set
					// variantCounts.remove(prefName);
					// if (null != synonyms)
					// for (int j = 0; j < synonyms.length; j++)
					// variantCounts.remove(synonyms[j]);
					//
					// if (variantCounts.isEmpty())
					// // the new variants were synonyms or the preferred name,
					// // skip the rest
					// continue;

					// If we are this far, we actually got new variants.
					// Get or create a new node representing the variants for
					// the current term. We need this since we want to store the
					// variants as well as their counts.
					Relationship hasVariantsRel = term.getSingleRelationship(variantRelationshipType,
							Direction.OUTGOING);
					if (null == hasVariantsRel) {
						Node variantsNode = graphDb.createNode(variantsAggregationLabel);
						hasVariantsRel = term.createRelationshipTo(variantsNode, variantRelationshipType);
					}
					Node variantsNode = hasVariantsRel.getEndNode();
					for (String docId : variantCountsInDocs.keySet()) {
						Map<String, Integer> variantCounts = variantCountsInDocs.get(docId);
						for (String variant : variantCounts.keySet()) {
							String normalizedVariant = TermVariantComparator.normalizeVariant(variant);
							Node variantNode = graphDb.findNode(variantNodeLabel, MorphoConstants.PROP_ID,
									normalizedVariant);
							if (null == variantNode) {
								variantNode = graphDb.createNode(variantNodeLabel);
								variantNode.setProperty(NodeConstants.PROP_ID, normalizedVariant);
								variantNode.setProperty(MorphoConstants.PROP_NAME, variant);
							}
							// with 'specific' we mean the exact relationship
							// connecting the variant with the variants node
							// belonging to the current term (and no other term
							// -
							// ambiguity!)
							Relationship specificElementRel = null;
							for (Relationship elementRel : variantNode.getRelationships(Direction.INCOMING,
									EdgeTypes.HAS_ELEMENT)) {
								if (elementRel.getStartNode().equals(variantsNode)
										&& elementRel.getEndNode().equals(variantNode)) {
									specificElementRel = elementRel;
									break;
								}
							}
							if (null == specificElementRel) {
								specificElementRel = variantsNode.createRelationshipTo(variantNode,
										EdgeTypes.HAS_ELEMENT);
								specificElementRel.setProperty(MorphoRelationConstants.PROP_DOCS, new String[0]);
								specificElementRel.setProperty(MorphoRelationConstants.PROP_COUNTS, new int[0]);
							}
							String[] documents = (String[]) specificElementRel
									.getProperty(MorphoRelationConstants.PROP_DOCS);
							int[] counts = (int[]) specificElementRel.getProperty(MorphoRelationConstants.PROP_COUNTS);
							int docIndex = Arrays.binarySearch(documents, docId);
							Integer count = variantCounts.get(variant);

							// found the document, we can just set the new value
							if (docIndex >= 0) {
								counts[docIndex] = count;
							} else {
								int insertionPoint = -1 * (docIndex + 1);
								// we don't have a record for this document
								String[] newDocuments = new String[documents.length + 1];
								int[] newCounts = new int[newDocuments.length];

								if (insertionPoint > 0) {
									// copy existing values before the new
									// documents entry
									System.arraycopy(documents, 0, newDocuments, 0, insertionPoint);
									System.arraycopy(counts, 0, newCounts, 0, insertionPoint);
								}
								newDocuments[insertionPoint] = docId;
								newCounts[insertionPoint] = count;
								if (insertionPoint < documents.length) {
									// copy existing values after the new
									// document entry
									System.arraycopy(documents, insertionPoint, newDocuments, insertionPoint + 1,
											documents.length - insertionPoint);
									System.arraycopy(counts, insertionPoint, newCounts, insertionPoint + 1,
											counts.length - insertionPoint);
								}
								specificElementRel.setProperty(MorphoRelationConstants.PROP_DOCS, newDocuments);
								specificElementRel.setProperty(MorphoRelationConstants.PROP_COUNTS, newCounts);
							}
						}
					}

					// Map<String, Object> existingVariants =
					// variantsNode.getAllProperties();
					// // add the updates for variants already existing. Doing
					// this
					// // first stabilizes the set of variants: due to the
					// // normalization step in the TreeMap, the exact variant
					// // could vary if we just use the writing variant from the
					// // input map.
					// for (String variant : existingVariants.keySet()) {
					// Integer oldCount = (Integer)
					// existingVariants.get(variant);
					// Integer count = variantCounts.get(variant);
					// if (null == count)
					// continue;
					// if (null == oldCount)
					// oldCount = 0;
					// Integer newCount = oldCount + count;
					// variantsNode.setProperty(variant, newCount);
					// // remove this variant from the map so we end up only
					// // with new variants eventually
					// variantCounts.remove(variant);
					// }
					// // add the completely new variants
					// for (String variant : variantCounts.keySet()) {
					// Integer count = variantCounts.get(variant);
					// variantsNode.setProperty(variant, count);
					// }
				}
				jsonReader.endObject();
				jsonReader.close();
				tx.success();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Concatenates the values of the elements of <tt>aggregate</tt> and returns
	 * them as an array.
	 * 
	 * @param aggregate
	 *            The aggregate for whose elements properties are requested.
	 * @param property
	 *            The requested property.
	 * @return The values of property <tt>property</tt> in the elements of
	 *         <tt>aggregate</tt>
	 */
	public static String[] getPropertyValueOfElements(Node aggregate, String property) {
		if (!aggregate.hasLabel(TermLabel.AGGREGATE))
			throw new IllegalArgumentException(
					"Node " + NodeUtilities.getNodePropertiesAsString(aggregate) + " is not an aggregate.");
		Iterable<Relationship> elementRels = aggregate.getRelationships(Direction.OUTGOING, EdgeTypes.HAS_ELEMENT);
		List<String> elementValues = new ArrayList<>();
		for (Relationship elementRel : elementRels) {
			String[] value = NodeUtilities.getNodePropertyAsStringArrayValue(elementRel.getEndNode(), property);
			for (int i = 0; value != null && i < value.length; i++)
				elementValues.add(value[i]);
		}
		return elementValues.isEmpty() ? null : elementValues.toArray(new String[elementValues.size()]);
	}
}
