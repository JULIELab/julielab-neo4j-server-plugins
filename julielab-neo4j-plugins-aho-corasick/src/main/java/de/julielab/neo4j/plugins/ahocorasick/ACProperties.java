package de.julielab.neo4j.plugins.ahocorasick;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class ACProperties{

		// EDGE TYPS
		public static enum EdgeTypes implements RelationshipType {
			FAIL,
			NEXT
		}

		// LABEL TYPS
		public static enum LabelTypes implements Label {
			DICTIONARY
		}

		// CONSTANT FOR INDEX IN DATABASE
		public static final String INDEX_DIC = "index_dic";
		public static final String LETTER = "letter";

		// ENTRIES IN THE DICT-TREE
		public static final String ENTRY = "entry";
		public static final String ATTRIBUTES = "attributes";

		// PROPERTIES FOR A NODE
		public static final String PROPERTY = "property_";
		/**
		 * Which state in the tree this node represents.
		 */
		public static final String STATE = PROPERTY + "state";
		public static final String DEPTH = PROPERTY + "depth";
		public static final String NUMBER_OUTPUT = PROPERTY + "number_Output";
		public static final String OUTPUT = PROPERTY + "Output";
		public static final String ORIGINAL = PROPERTY + "original";
		public static final String RELATIONSHIP = PROPERTY + "relationship";
		public static final String NUMBER_NEXT = PROPERTY + "number_next";
		
		// PROPERTIES FOR THE ROOT OF THE DICT-TREE
		public static final String NODES_IN_TREE = "nodes_in_Tree";
		public static final String DICTIONARY_NAME = "dict_name";
		public static final String NUMBER_OF_ENTRIES = "number_of_entries";
		public static final String PREPARED = "prepared";
		public static final String MODECREATE = "modeCreate";
		public static final String MODESEARCH = "modeSearch";

		// NODE BEFORE RELATION TYP
		public static final String RELATION_TYPE = "relation_typ";
		
		// INDEX
		static String getFailName(){
			return EdgeTypes.FAIL.name();
		}
		
}
