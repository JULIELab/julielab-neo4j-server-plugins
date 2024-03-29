package de.julielab.neo4j.plugins.auxiliaries;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NodeUtilities extends PropertyUtilities {

	/**
	 * Returns the only node contained in <tt>iterable</tt> or <tt>null</tt>, if no such node exists. If the
	 * <tt>iterable</tt> contains more than one node, an exception will be thrown.
	 * 
	 * @param iterable
	 * @return The only node in <tt>iterable</tt> or <tt>null</tt> if there is no node.
	 * @throws IllegalStateException
	 *             If <tt>iterable</tt> contains more than one node.
	 */
	public static Node getSingleNode(ResourceIterable<Node> iterable) {
		return getSingleNode(iterable.iterator());
	}

	public static Node getSingleNode(ResourceIterator<Node> iterator) {
		Node node = null;
		if (iterator.hasNext()) {
			node = iterator.next();
			if (iterator.hasNext())
				throw new IllegalStateException("ResourceIterator has more than one element.");
		}
		return node;
	}

	public static Node getSingleOtherNode(Node node, RelationshipType type) {
		Node otherNode = null;
		Iterator<Relationship> it = node.getRelationships(type).iterator();
		if (it.hasNext()) {
			Relationship relationship = it.next();
			otherNode = relationship.getOtherNode(node);
			if (it.hasNext()) {
				List<String> properties = new ArrayList<>();
				for (String key : node.getPropertyKeys())
					properties.add(key + "=" + node.getProperty(key).toString());
				throw new IllegalStateException("Node " + node
						+ " (properties: "
						+ StringUtils.join(properties, " ; ")
						+ ") is incident on more than one relationship of type  "
						+ type
						+ ".");
			}
		}
		return otherNode;
	}

	public static Node copyNode(Transaction tx, Node source) {
		Node copy = tx.createNode();
		for (String key : source.getPropertyKeys()) {
			copy.setProperty(key, source.getProperty(key));
		}
		return copy;
	}

	/**
	 * Gets the string or string array value of <tt>property</tt> from <tt>node</tt>. Always returns an array. If the
	 * value is a single string, it will be returned as array of size one, containing only this value.
	 * 
	 * @param node
	 * @param property
	 * @return
	 */
	public static String[] getNodePropertyAsStringArrayValue(Node node, String property) {
		if (!node.hasProperty(property))
			return null;
		String[] ret = null;
		Object propertyValue = node.getProperty(property);
		if (propertyValue.getClass().equals(String.class))
			ret = new String[] { (String) propertyValue };
		else if (propertyValue.getClass().equals(String[].class))
			ret = (String[]) propertyValue;
		return ret;
	}
	
	public static String getString(Node term, String property) {
		if (term.hasProperty(property))
			return (String) term.getProperty(property);
		return null;
	}
}
