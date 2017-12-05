package de.julielab.neo4j.plugins.auxiliaries.semedico;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import de.julielab.neo4j.plugins.FacetManager;
import de.julielab.neo4j.plugins.FacetManager.EdgeTypes;
import de.julielab.neo4j.plugins.ConceptManager;
import de.julielab.neo4j.plugins.evaluators.FacetGroupPathEvaluator;
import de.julielab.neo4j.plugins.evaluators.HasRelationShipEvaluator;
import de.julielab.neo4j.plugins.evaluators.NodeLabelEvaluator;
import de.julielab.neo4j.plugins.evaluators.PropertyEvaluator;

public class PredefinedTraversals {
	/**
	 * Beginning at the facet groups node, this traversal returns all facets.
	 * 
	 * @param graphDb
	 *            The graph database to traverse on.
	 * 
	 * @return Traversal to get all facet nodes.
	 */
	public static TraversalDescription getFacetTraversal(GraphDatabaseService graphDb) {
		return getFacetTraversal(graphDb, null, null);
	}

	/**
	 * Beginning at the facet groups node, this traversal returns all facets
	 * matching the given property key and property value. More precisely, the
	 * traversal returns nodes connected by {@link EdgeTypes#HAS_FACET_GROUP} or
	 * {@link EdgeTypes#HAS_FACET} edge types that are located at a depth of at
	 * most 2.
	 * 
	 * @param graphDb
	 *            The graph database to traverse on.
	 * @param propertyKey
	 *            The property on which to filter for <tt>propertyValue</tt>
	 * @param propertyValue
	 *            The value for which is looked in <tt>property</tt> for facet
	 *            nodes
	 * 
	 * @return Traversal to get all facet nodes.
	 */
	public static TraversalDescription getFacetTraversal(GraphDatabaseService graphDb, String propertyKey,
			Object propertyValue) {
		if (StringUtils.isBlank(propertyKey))
			propertyKey = "*";
		if (null == propertyValue || "".equals(propertyValue))
			propertyValue = "*";
		TraversalDescription td = graphDb.traversalDescription().depthFirst()
				.relationships(FacetManager.EdgeTypes.HAS_FACET_GROUP).relationships(FacetManager.EdgeTypes.HAS_FACET)
				// facetGroups --> facetGroupX --> facet
				// Thus, traverse until depth 2.
				.evaluator(Evaluators.atDepth(2));
		if (!"*".equals(propertyKey) || !"*".equals(propertyValue))
			td = td.evaluator(new PropertyEvaluator(propertyKey, propertyValue));

		return td;
	}

	/**
	 * This traversal returns two nodes, viz. the facet groups node and the
	 * particular facet group node
	 * 
	 * @param graphDb
	 *            The graph database to work on.
	 * @param facetGroupName
	 *            The name of the requested facet group, e.g. "BioMed".
	 * @return A traversal to get the requested facet group.
	 */
	public static TraversalDescription getFacetGroupTraversal(GraphDatabaseService graphDb, String facetGroupName) {
		TraversalDescription td = graphDb.traversalDescription().depthFirst().relationships(EdgeTypes.HAS_FACET_GROUP)
				.evaluator(new FacetGroupPathEvaluator(facetGroupName));

		return td;
	}

	public static TraversalDescription getAcronymsTraversal(GraphDatabaseService graphDb) {
		TraversalDescription td = graphDb.traversalDescription().depthFirst()
				.relationships(ConceptManager.EdgeTypes.HAS_ACRONYMS, Direction.OUTGOING)
				.relationships(ConceptManager.EdgeTypes.HAS_ELEMENT, Direction.OUTGOING)
				.evaluator(new NodeLabelEvaluator(ConceptManager.MorphoLabel.ACRONYM));

		return td;
	}

	public static TraversalDescription getWritingVariantsTraversal(GraphDatabaseService graphDb) {
		TraversalDescription td = graphDb.traversalDescription().depthFirst()
				.relationships(ConceptManager.EdgeTypes.HAS_VARIANTS, Direction.OUTGOING)
				.relationships(ConceptManager.EdgeTypes.HAS_ELEMENT, Direction.OUTGOING)
				.evaluator(new NodeLabelEvaluator(ConceptManager.MorphoLabel.WRITING_VARIANT));

		return td;
	}

	public static TraversalDescription getNonAggregateAggregateElements(GraphDatabaseService graphDb) {
		TraversalDescription td = graphDb.traversalDescription().depthFirst()
				.relationships(ConceptManager.EdgeTypes.HAS_ELEMENT)
				.evaluator(new NodeLabelEvaluator(ConceptManager.ConceptLabel.AGGREGATE, true));
		return td;
	}

	public static TraversalDescription getTopAggregates(GraphDatabaseService graphDb) {
		TraversalDescription td = graphDb.traversalDescription().breadthFirst()
				.relationships(ConceptManager.EdgeTypes.HAS_ELEMENT)
				.evaluator(new HasRelationShipEvaluator(Direction.INCOMING,
						new RelationshipType[] { de.julielab.neo4j.plugins.ConceptManager.EdgeTypes.HAS_ELEMENT }, false));
		return td;
	}
}
