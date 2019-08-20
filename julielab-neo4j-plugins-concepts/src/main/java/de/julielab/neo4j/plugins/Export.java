package de.julielab.neo4j.plugins;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.julielab.neo4j.plugins.ConceptManager.ConceptLabel;
import de.julielab.neo4j.plugins.ConceptManager.EdgeTypes;
import de.julielab.neo4j.plugins.auxiliaries.JulieNeo4jUtilities;
import de.julielab.neo4j.plugins.auxiliaries.NodeUtilities;
import de.julielab.neo4j.plugins.auxiliaries.PropertyUtilities;
import de.julielab.neo4j.plugins.auxiliaries.semedico.PredefinedTraversals;
import de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import de.julielab.neo4j.plugins.datarepresentation.constants.MorphoConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.server.plugins.*;
import org.neo4j.server.rest.repr.RecursiveMappingRepresentation;
import org.neo4j.server.rest.repr.Representation;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import static de.julielab.neo4j.plugins.datarepresentation.constants.ConceptConstants.*;
import static de.julielab.neo4j.plugins.datarepresentation.constants.NodeConstants.PROP_ID;

public class Export extends ServerPlugin {

	private static final Logger log = Logger.getLogger(Export.class.getName());

	/**
	 * The REST context path to this plugin. This is for convenience for usage
	 * from external programs that make use of the plugin.
	 */
	public static final String EXPORT_ENDPOINT = "/db/data/ext/" + Export.class.getSimpleName() + "/graphdb/";

	public static final String HYPERNYMS = "hypernyms";
	public static final String LINGPIPE_DICT = "lingpipe_dictionary";
	public static final String TERM_TO_FACET = "term_facet_map";
	public static final String TERM_ID_MAPPING = "term_id_mapping";
	public static final String ELEMENT_TO_AGGREGATE_ID_MAPPING = "element_aggregate_id_mapping";

	public static final String PARAM_ID_PROPERTY = "id_property";
	public static final String PARAM_LABELS = "labels";
	public static final String PARAM_LABEL = "label";
	public static final String PARAM_EXCLUSION_LABEL = "exclusion_label";
	@Deprecated
	public static final String PARAM_FURTHER_PROPERTIES = "further_properties";

	public static final int OUTPUTSTREAM_INIT_SIZE = 200000000;
	public static final int HYPERNYMS_CACHE_SIZE = 100000;

	@Name(TERM_ID_MAPPING)
	@Description("Creates the ID mapping file data required for LuCas' replace filter. "
			+ "The returned data is a JSON array of bytes. Those bytes represent the "
			+ "GZIPed string data of id mapping data. That is, to read the actual file "
			+ "content, the JSON array is to be converted to a byte[] which then serves "
			+ "as input for a ByteArrayInputStream which in turn goes through a "
			+ "GZIPInputStream for decoding. The result is a stream from which the " + "mapping data can be read.")
	@PluginTarget(GraphDatabaseService.class)
	public Representation exportIdMapping(@Source GraphDatabaseService graphDb,
			@Parameter(name = PARAM_ID_PROPERTY) @Description("TODO") String idProperty,
			@Parameter(name = PARAM_LABELS) @Description("TODO") String labelStrings) throws Exception {
		final ObjectMapper om = new ObjectMapper();
		log.info("Exporting ID mapping data.");
		String[] labelsArray = null != labelStrings ? om.readValue(labelStrings, String[].class) : null;
		log.info("Creating mapping file content for property \"" + idProperty + "\" and facets " + labelsArray);
		ByteArrayOutputStream gzipBytes = createIdMapping(graphDb, idProperty, labelsArray);
		byte[] bytes = gzipBytes.toByteArray();
		log.info("Sending all " + bytes.length + " bytes of GZIPed ID mapping file data.");
		log.info("Done exporting ID mapping data.");
		return RecursiveMappingRepresentation.getObjectRepresentation(bytes);
	}

	private ByteArrayOutputStream createIdMapping(GraphDatabaseService graphDb, String idProperty,
			String[] labelsArray) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
		try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
			try (Transaction tx = graphDb.beginTx()) {
				int numWritten = 0;
				for (int i = 0; i < labelsArray.length; i++) {
					String labelString = labelsArray[i];
					Label label = Label.label(labelString);
					for (ResourceIterator<Node> terms = graphDb.findNodes(label); terms.hasNext();) {
						Node term = terms.next();
						String termId = (String) term.getProperty(ConceptConstants.PROP_ID);
						Object idObject = PropertyUtilities.getNonNullNodeProperty(term, idProperty);
						if (null == idObject)
							continue;
						if (idObject.getClass().isArray()) {
							Object[] idArray = JulieNeo4jUtilities.convertArray(idObject);
							for (int j = 0; j < idArray.length; j++) {
								Object id = idArray[j];
								IOUtils.write(id + "\t" + termId + "\n", os, "UTF-8");
								numWritten++;
							}
						} else {
							IOUtils.write(idObject + "\t" + termId + "\n", os, "UTF-8");
							numWritten++;
						}
						// }
					}
				}
				log.info("Num written: " + numWritten);
				tx.success();
			}
		}
		return baos;
	}

	@Name(HYPERNYMS)
	@Description("Creates the hypernym file data required for LuCas' hypernym filter. "
			+ "The returned data is a JSON array of bytes. Those bytes represent the "
			+ "GZIPed string data of hypernym data. That is, to read the actual file "
			+ "content, the JSON array is to be converted to a byte[] which then serves "
			+ "as input for a ByteArrayInputStream which in turn goes through a "
			+ "GZIPInputStream for decoding. The result is a stream from which the " + "hypernym data can be read.")
	@PluginTarget(GraphDatabaseService.class)
	public Representation exportHypernyms(@Source GraphDatabaseService graphDb,
			@Description("The facet labels indicating for which facets to create the hypernyms file") @Parameter(name = PARAM_LABELS, optional = true) String facetLabelStrings,
			@Description("A label restricting hypernym generation to terms with this label.") @Parameter(name = PARAM_LABEL, optional = true) String termLabel)
			throws Exception {
        ObjectMapper om = new ObjectMapper();
		String[] labelsArray = null != facetLabelStrings ? om.readValue(facetLabelStrings, String[].class) : null;
		if (null == labelsArray)
			log.info("Exporting hypernyms dictionary data for all facets.");
		else
			log.info("Exporting hypernyms dictionary data for the facets with labels " + labelsArray.toString() + ".");
		ByteArrayOutputStream hypernymsGzipBytes = writeHypernymList(graphDb, labelsArray, termLabel,
				HYPERNYMS_CACHE_SIZE);
		byte[] bytes = hypernymsGzipBytes.toByteArray();
		log.info("Sending all " + bytes.length + " bytes of GZIPed hypernym file data.");
		log.info("Done exporting hypernym data.");
		return RecursiveMappingRepresentation.getObjectRepresentation(bytes);
	}

	private ByteArrayOutputStream writeHypernymList(GraphDatabaseService graphDb, String[] labelsArray,
			String termLabelString, int cacheSize) throws IOException{

		String[] labels = labelsArray;
		if (null == labels) {
			labels = new String[]{FacetManager.FacetLabel.FACET.name()};
		}
		Label termLabel = null;
		if (!StringUtils.isBlank(termLabelString))
			termLabel = Label.label(termLabelString);

		Map<Node, Set<String>> cache = new HashMap<>(cacheSize);

		ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
		try (GZIPOutputStream os = new GZIPOutputStream(baos)) {

			try (Transaction tx = graphDb.beginTx()) {
				// This list will hold the relationship types that are used to
				// connect the terms that belong the facets
				// for which hypernyms should be created. If for all facets
				// hypernyms are to be created this will just
				// include the general IS_BROADER_THAN relationship type that
				// doesn't make a difference between facets.
				List<RelationshipType> relationshipTypeList = new ArrayList<>();
				// Only create the specific facet IDs set when we have not just
				// all facets
				if (labels.length > 1 || !labels[0].equals(FacetManager.FacetLabel.FACET.name())) {
					for (int i = 0; i < labels.length; i++) {
						String labelString = labels[i];
						Label label = Label.label(labelString);
						ResourceIterable<Node> facets = () -> graphDb.findNodes(label);
						for (Node facet : facets) {
							if (!facet.hasLabel(FacetManager.FacetLabel.FACET))
								throw new IllegalArgumentException("Label node " + facet + " with the label " + label
										+ " is no facet since it does not have the " + FacetManager.FacetLabel.FACET
										+ " label.");
							String facetId = (String) facet.getProperty(FacetConstants.PROP_ID);
							RelationshipType reltype = RelationshipType
									.withName(ConceptManager.EdgeTypes.IS_BROADER_THAN + "_" + facetId);
							relationshipTypeList.add(reltype);
						}
					}
				} else {
					relationshipTypeList.add(ConceptManager.EdgeTypes.IS_BROADER_THAN);
				}

				for (int i = 0; i < labels.length; i++) {
					String labelString = labels[i];
					Label label = Label.label(labelString);
					log.info("Now creating hypernyms for facets with label " + label);
					ResourceIterable<Node> facets = () -> graphDb.findNodes(label);
					Set<Node> visitedNodes = new HashSet<>();
					for (Node facet : facets) {
						Iterable<Relationship> rels = facet.getRelationships(Direction.OUTGOING,
								EdgeTypes.HAS_ROOT_CONCEPT);
						for (Relationship rel : rels) {
							Node rootTerm = rel.getEndNode();
							if (null != termLabel && !rootTerm.hasLabel(termLabel))
								continue;
							writeHypernyms(rootTerm, visitedNodes, cache, os,
									relationshipTypeList.toArray(new RelationshipType[relationshipTypeList.size()]));
						}
					}
				}
			}
		}
		return baos;
	}

	public Set<String> load(Node n, Map<Node, Set<String>> cache, RelationshipType[] relationshipTypes) {
		Set<String> hypernyms = cache.get(n);
		if (null == hypernyms) {
			hypernyms = new HashSet<>();
			cache.put(n, hypernyms);
		} else {
			return hypernyms;
		}

		Set<Node> visitedNodes = new HashSet<>();
		visitedNodes.add(n);
		for (Relationship rel : n.getRelationships(Direction.INCOMING, relationshipTypes)) {
			Node directHypernym = rel.getStartNode();
			boolean isHollow = false;
			for (Label l : directHypernym.getLabels())
				if (l.equals(ConceptLabel.HOLLOW))
					isHollow = true;
			if (isHollow)
				continue;
			if (visitedNodes.contains(directHypernym))
				continue;
			String directHypernymId = ((String) directHypernym.getProperty(ConceptConstants.PROP_ID)).intern();
			hypernyms.add(directHypernymId);
			hypernyms.addAll(load(directHypernym, cache, relationshipTypes));
		}
		visitedNodes.remove(n);
		return hypernyms;
	}

	private void writeHypernyms(Node n, Set<Node> visitedNodes, Map<Node, Set<String>> cache, GZIPOutputStream os,
			RelationshipType[] relationshipTypes) throws IOException {
		if (visitedNodes.contains(n))
			return;
		load(n, cache, relationshipTypes);
		visitedNodes.add(n);
		boolean isHollow = false;
		for (Label l : n.getLabels())
			if (l.equals(ConceptLabel.HOLLOW))
				isHollow = true;
		if (isHollow)
			return;
		Set<String> hypernyms = cache.get(n);
		if (hypernyms.size() > 0)
			IOUtils.write(n.getProperty(ConceptConstants.PROP_ID) + "\t" + StringUtils.join(hypernyms, "|") + "\n", os,
					"UTF-8");
		for (Relationship rel : n.getRelationships(Direction.OUTGOING, EdgeTypes.IS_BROADER_THAN)) {
			writeHypernyms(rel.getEndNode(), visitedNodes, cache, os, relationshipTypes);
		}
		if (visitedNodes.size() % 100000 == 0)
			log.info("Finished " + visitedNodes.size() + ".");
	}

	@Name(LINGPIPE_DICT)
	@Description("Creates a dictionary of all synonyms and writing variants " + "for terms with label " + PARAM_LABEL
			+ " and without label " + PARAM_EXCLUSION_LABEL + " in the database. The dictionary has two columns, "
			+ "the synonym/writing variant and the term's ID. This dictionary is "
			+ "used with the Lingpipe chunker to recognize database terms in user queries. "
			+ "The returned data is a Base64 encoded string. This string represents the "
			+ "GZIPed string data of the dictionary. That is, to read the actual file "
			+ "content, the Base64 encoded string so to be decoded into a byte[] which then serves "
			+ "as input for a ByteArrayInputStream which in turn goes through a "
			+ "GZIPInputStream for decoding. The result is a stream from which the " + "dictionary data can be read.")
	@PluginTarget(GraphDatabaseService.class)
	public String exportLingpipeDictionary(@Source GraphDatabaseService graphDb,
			@Description("The label to select the terms for which to create the dictionary.") @Parameter(name = PARAM_LABEL, optional = true) String labelString,
			@Description("A JSON list of labels that exclude terms from used for the dictionary.") @Parameter(name = PARAM_EXCLUSION_LABEL, optional = true) String exclusionLabelString,
			@Description("The node properties providing the lingpipe chunker category. May refer to array-valued property."
					+ " In case multiple properties are given, their values will be concatenated by two pipes (||)."
					+ " NOTE that it is expected that array-valued properties are or of the same size. "
					+ "The concatenation will be done for the same index in all value-arrays, i.e. not all combinations are built. For aggregates that not have a requested properties, their elements will be used instead.") @Parameter(name = PARAM_ID_PROPERTY, optional = true) String[] nodeCategories)
			throws IOException {
		Label label = StringUtils.isBlank(labelString) ? ConceptManager.ConceptLabel.CONCEPT : Label.label(labelString);
		List<String> propertiesToWrite = new ArrayList<>();
		if (nodeCategories == null || nodeCategories.length == 0) {
			propertiesToWrite.add(PROP_ID);
		} else {
			for (int i = 0; i < nodeCategories.length; i++) {
				String property = nodeCategories[i];
				propertiesToWrite.add(property);
			}
		}
		Label[] exclusionLabels = null;
		if (!StringUtils.isBlank(exclusionLabelString)) {
            final ObjectMapper om = new ObjectMapper();
            try {
                String[] exclusionLabelsJson = om.readValue(exclusionLabelString, String[].class);
				exclusionLabels = new Label[exclusionLabelsJson.length];
				for (int i = 0; i < exclusionLabelsJson.length; i++) {
					String string = exclusionLabelsJson[i];
					exclusionLabels[i] = Label.label(string);
				}
			} catch (JsonParseException e) {
				Label exclusionLabel = Label.label(exclusionLabelString);
				exclusionLabels = new Label[] { exclusionLabel };
			}
		}
		log.info("Exporting lingpipe dictionary data for nodes with label \"" + label.name()
				+ "\", mapping their names to their properties " + propertiesToWrite + ".");
		ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
		try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
			try (Transaction tx = graphDb.beginTx()) {
				ResourceIterator<Node> terms = graphDb.findNodes(label);
				int count = 0;
				while (terms.hasNext()) {
					Node term = (Node) terms.next();
					count++;
					boolean termHasExclusionLabel = false;
					for (int i = 0; null != exclusionLabels && i < exclusionLabels.length; i++) {
						Label exclusionLabel = exclusionLabels[i];
						if (term.hasLabel(exclusionLabel)) {
							termHasExclusionLabel = true;
							break;
						}
					}
					if (!termHasExclusionLabel && term.hasProperty(PROP_ID) && term.hasProperty(PROP_PREF_NAME)) {

						int arraySize;
						String idProperty = propertiesToWrite.get(0);
						// for array-valued properties we require that all
						// arrays are of the same length. Thus, to determine the
						// required number of iterations we just use the first
						// array since the others should have the same length.
						String[] value = NodeUtilities.getNodePropertyAsStringArrayValue(term, idProperty);

						if (null == value && term.hasLabel(ConceptLabel.AGGREGATE))
							// perhaps we have an aggregate term, then we can
							// try and retrieve the value from its elements
							value = ConceptManager.getPropertyValueOfElements(term, idProperty);
						if (null == value) {
							terms.close();
							throw new IllegalArgumentException("A concept occurred that does not have a value for the property \"" + idProperty + "\": " + NodeUtilities.getNodePropertiesAsString(term));
						}
						arraySize = value.length;

						List<String> categoryStrings = new ArrayList<>();
						for (int i = 0; i < arraySize; ++i) {
							StringBuilder sb = new StringBuilder();
							for (int j = 0; j < propertiesToWrite.size(); ++j) {
								String property = propertiesToWrite.get(j);
								value = NodeUtilities.getNodePropertyAsStringArrayValue(term, property);
								if (null == value && term.hasLabel(ConceptLabel.AGGREGATE))
									// perhaps we have an aggregate term, then
									// we can try and retrieve the value from
									// its elements
									value = ConceptManager.getPropertyValueOfElements(term, idProperty);
								if (null == value || value.length == 0) {
									terms.close();
									throw new IllegalArgumentException("The property \"" + property
											+ "\" does not contain a value for node " + term + " (properties: "
											+ PropertyUtilities.getNodePropertiesAsString(term) + ")");
								}
								if (value.length != arraySize) {
									terms.close();
									throw new IllegalArgumentException("The properties \"" + propertiesToWrite
											+ "\" on term " + PropertyUtilities.getNodePropertiesAsString(term)
											+ " do not have all the same number of value elements which is required for dictionary creation by this method.");
								}
								sb.append(value[i]);
								if (j < propertiesToWrite.size() - 1)
									sb.append("||");
							}
							categoryStrings.add(sb.toString());
						}

						for (String categoryString : categoryStrings) {
							String preferredName = (String) term.getProperty(PROP_PREF_NAME);
							String[] synonyms = new String[0];
							if (term.hasProperty(PROP_SYNONYMS))
								synonyms = (String[]) term.getProperty(PROP_SYNONYMS);
							// String[] writingVariants = new String[0];
							// if (term.hasProperty(PROP_WRITING_VARIANTS))
							// writingVariants = (String[]) term
							// .getProperty(PROP_WRITING_VARIANTS);

							writeNormalizedDictionaryEntry(preferredName, categoryString, os);
							for (String synonString : synonyms)
								writeNormalizedDictionaryEntry(synonString, categoryString, os);
							TraversalDescription acronymsTraversal = PredefinedTraversals.getAcronymsTraversal(graphDb);
							Traverser traverse = acronymsTraversal.traverse(term);
							for (Node acronymNode : traverse.nodes()) {
								String acronym = (String) acronymNode.getProperty(MorphoConstants.PROP_NAME);
								writeNormalizedDictionaryEntry(acronym, categoryString, os);
							}
							// for (String variant : writingVariants)
							// writeNormalizedDictionaryEntry(variant,
							// categoryString, os);
						}
					}
					if (count % 100000 == 0)
						log.info(count + " terms processed.");
				}
			}
		}
		log.info("Done exporting Lingpipe term dictionary.");
		byte[] bytes = baos.toByteArray();
		String encoded = DatatypeConverter.printBase64Binary(bytes);
		return encoded;
	}

	private void writeNormalizedDictionaryEntry(String name, String termId, OutputStream os) throws IOException {
		String normalizedName = StringUtils.normalizeSpace(name);
		if (normalizedName.length() > 2)
			IOUtils.write(normalizedName + "\t" + termId + "\n", os, "UTF-8");
	}

	@Name(TERM_TO_FACET)
	@Description("Creates a map <term id>=<facet id> "
			+ "for all terms in the database. For terms that belong to multiple facets, several lines are created. "
			+ "The returned data is a JSON array of bytes. Those bytes represent the "
			+ "GZIPed string data of the dictionary. That is, to read the actual file "
			+ "content, the JSON array is to be converted to a byte[] which then serves "
			+ "as input for a ByteArrayInputStream which in turn goes through a "
			+ "GZIPInputStream for decoding. The result is a stream from which the " + "dictionary data can be read.")
	@PluginTarget(GraphDatabaseService.class)
	public static Representation exportTermFacetMapping(@Source GraphDatabaseService graphDb,
			@Description("The term label to create the ID map for. Defaults to TERM.") @Parameter(name = PARAM_LABEL, optional = true) String labelString)
			throws IOException {
		log.info("Exporting lingpipe dictionary data.");
		Label label = !StringUtils.isBlank(labelString) ? Label.label(labelString) : ConceptManager.ConceptLabel.CONCEPT;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
		try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
			try (Transaction tx = graphDb.beginTx()) {
				ResourceIterable<Node> terms = () -> graphDb.findNodes(label);
				int count = 0;
				for (Node term : terms) {
					count++;
					if (term.hasProperty(PROP_ID) && term.hasProperty(PROP_FACETS)) {
						String termId = (String) term.getProperty(PROP_ID);
						String[] facetIds = (String[]) term.getProperty(PROP_FACETS);
						IOUtils.write(termId + "\t" + StringUtils.join(facetIds, "|") + "\n", os, "UTF-8");
					}
					if (count % 100000 == 0)
						log.info(count + " terms processed.");
				}
				log.info("Done exporting mapping from term ID to corresponding facet IDs.");
			}
		}
		return RecursiveMappingRepresentation.getObjectRepresentation(baos.toByteArray());
	}

	@Name(ELEMENT_TO_AGGREGATE_ID_MAPPING)
	@Description("WARN: This method is easily replaceable with a Cypher query and might me removed in the future. " +
            "Creates a mapping file from aggregate element IDs to their respective aggregate ID."
			+ " Currently, only non-aggregate elements are eligible. Also, this works only correctly if each element has" +
			" exactly one aggregate. If there are multiple aggregates, only one mapping will be returned.")
	@PluginTarget(GraphDatabaseService.class)
	public String exportElementToAggregateIdMapping(@Source GraphDatabaseService graphDb,
			@Parameter(name = PARAM_LABELS) @Description("The aggregate labels for which to create the mapping") String aggLabelStrings)
			throws Exception {
        ObjectMapper om = new ObjectMapper();
		log.info("Exporting element-aggregate ID mapping data.");
        String[] labelsArray = om.readValue(aggLabelStrings, String[].class);
		log.info("Creating element-aggregate ID mapping file content for aggregate labels \"" + labelsArray + "\"");
		ByteArrayOutputStream gzipBytes = createElementAggregateIdMapping(graphDb, labelsArray);
		byte[] bytes = gzipBytes.toByteArray();
		log.info("Sending all " + bytes.length + " bytes of GZIPed ID element-aggregate ID mapping file data.");
		String encoded = DatatypeConverter.printBase64Binary(bytes);
		log.info("Done exporting element-aggregate ID mapping data.");
		return encoded;
	}

	/**
	 * Writes a mapping from all nodes that are element of an aggregate that has
	 * a label in <tt>aggLabelsArray</tt>. The final mapping will always map to
	 * the "highest" aggregate, i.e. one with maximum distance from the element
	 * node.
	 * 
	 * @param graphDb
	 * @param aggLabelsArray
	 * @return
	 * @throws IOException
	 */
	private ByteArrayOutputStream createElementAggregateIdMapping(GraphDatabaseService graphDb,
			String[] aggLabelsArray) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(OUTPUTSTREAM_INIT_SIZE);
		try (GZIPOutputStream os = new GZIPOutputStream(baos)) {
			try (Transaction tx = graphDb.beginTx()) {
				Map<String, String> ele2Agg = new HashMap<>();
				Set<String> visitedAggregates = new HashSet<>();
				for (int i = 0; i < aggLabelsArray.length; ++i) {
					Label label = Label.label(aggLabelsArray[i]);
					ResourceIterator<Node> aggregates = graphDb.findNodes(label);
					TraversalDescription td = PredefinedTraversals.getNonAggregateAggregateElements(graphDb);
					while (aggregates.hasNext()) {
						Node aggregate = aggregates.next();
						String aggregateId = (String) aggregate.getProperty(ConceptConstants.PROP_ID);
						if (!visitedAggregates.add(aggregateId))
							continue;
						Traverser traverse = td.traverse(aggregate);
						for (Path elementPath : traverse) {
							for (Node n : elementPath.nodes()) {
								if (n.hasLabel(ConceptManager.ConceptLabel.AGGREGATE))
									visitedAggregates.add((String) n.getProperty(ConceptConstants.PROP_ID));
							}
							Node element = elementPath.endNode();
							if (!element.hasProperty(ConceptConstants.PROP_ID)) {
								log.warning("Node " + element.getId() + " does not have the ID property "
										+ ConceptConstants.PROP_ID
										+ " and is discarded for the creation of the element aggregate ID mapping.");
								continue;
							}
							String elementId = (String) element.getProperty(ConceptConstants.PROP_ID);
							ele2Agg.put(elementId, aggregateId);
						}

					}
				}
				int numWritten = 0;
				for (String elementId : ele2Agg.keySet()) {
					String aggregateId = ele2Agg.get(elementId);
					IOUtils.write(elementId + "\t" + aggregateId + "\n", os, "UTF-8");
					numWritten++;
				}
				log.info("Num written: " + numWritten);
				tx.success();
			}
		}
		return baos;
	}

}
