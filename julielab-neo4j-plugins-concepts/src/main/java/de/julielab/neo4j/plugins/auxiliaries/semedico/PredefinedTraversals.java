package de.julielab.neo4j.plugins.auxiliaries.semedico;

import de.julielab.neo4j.plugins.FacetManager;
import de.julielab.neo4j.plugins.FacetManager.EdgeTypes;
import de.julielab.neo4j.plugins.concepts.ConceptEdgeTypes;
import de.julielab.neo4j.plugins.concepts.ConceptLabel;
import de.julielab.neo4j.plugins.concepts.MorphoLabel;
import de.julielab.neo4j.plugins.evaluators.FacetGroupPathEvaluator;
import de.julielab.neo4j.plugins.evaluators.HasRelationShipEvaluator;
import de.julielab.neo4j.plugins.evaluators.NodeLabelEvaluator;
import de.julielab.neo4j.plugins.evaluators.PropertyEvaluator;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

public class PredefinedTraversals {
    /**
     * Beginning at the facet groups node, this traversal returns all facets.
     *
     * @param tx The graph database to traverse on.
     * @return Traversal to get all facet nodes.
     */
    public static TraversalDescription getFacetTraversal(Transaction tx) {
        return getFacetTraversal(tx, null, null);
    }

    /**
     * Beginning at the facet groups node, this traversal returns all facets
     * matching the given property key and property value. More precisely, the
     * traversal returns nodes connected by {@link EdgeTypes#HAS_FACET_GROUP} or
     * {@link EdgeTypes#HAS_FACET} edge types that are located at a depth of at
     * most 2.
     *
     * @param tx            The graph database to traverse on.
     * @param propertyKey   The property on which to filter for <tt>propertyValue</tt>
     * @param propertyValue The value for which is looked in <tt>property</tt> for facet
     *                      nodes
     * @return Traversal to get all facet nodes.
     */
    public static TraversalDescription getFacetTraversal(Transaction tx, String propertyKey,
                                                         Object propertyValue) {
        if (StringUtils.isBlank(propertyKey))
            propertyKey = "*";
        if (null == propertyValue || "".equals(propertyValue))
            propertyValue = "*";
        TraversalDescription td = tx.traversalDescription().depthFirst()
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
     * @param tx             The graph database to work on.
     * @param facetGroupName The name of the requested facet group, e.g. "BioMed".
     * @return A traversal to get the requested facet group.
     */
    public static TraversalDescription getFacetGroupTraversal(Transaction tx, String facetGroupName) {

        return tx.traversalDescription().depthFirst().relationships(EdgeTypes.HAS_FACET_GROUP)
                .evaluator(new FacetGroupPathEvaluator(facetGroupName));
    }

    public static TraversalDescription getAcronymsTraversal(Transaction tx) {

        return tx.traversalDescription().depthFirst()
                .relationships(ConceptEdgeTypes.HAS_ACRONYMS, Direction.OUTGOING)
                .relationships(ConceptEdgeTypes.HAS_ELEMENT, Direction.OUTGOING)
                .evaluator(new NodeLabelEvaluator(MorphoLabel.ACRONYM));
    }

    public static TraversalDescription getWritingVariantsTraversal(Transaction tx) {

        return tx.traversalDescription().depthFirst()
                .relationships(ConceptEdgeTypes.HAS_VARIANTS, Direction.OUTGOING)
                .relationships(ConceptEdgeTypes.HAS_ELEMENT, Direction.OUTGOING)
                .evaluator(new NodeLabelEvaluator(MorphoLabel.WRITING_VARIANT));
    }

    public static TraversalDescription getNonAggregateAggregateElements(Transaction tx) {
        return tx.traversalDescription().depthFirst()
                .relationships(ConceptEdgeTypes.HAS_ELEMENT)
                .evaluator(new NodeLabelEvaluator(ConceptLabel.AGGREGATE, true));
    }

    public static TraversalDescription getTopAggregates(Transaction tx) {
        return tx.traversalDescription().breadthFirst()
                .relationships(ConceptEdgeTypes.HAS_ELEMENT)
                .evaluator(new HasRelationShipEvaluator(Direction.INCOMING,
                        new RelationshipType[]{ConceptEdgeTypes.HAS_ELEMENT}, false));
    }
}
