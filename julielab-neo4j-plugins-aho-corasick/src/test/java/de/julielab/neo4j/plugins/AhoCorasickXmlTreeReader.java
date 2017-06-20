package de.julielab.neo4j.plugins;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.julielab.neo4j.plugins.ahocorasick.ACSearch;
import de.julielab.neo4j.plugins.ahocorasick.property.ACEntry;

public class AhoCorasickXmlTreeReader {

	// MODUS
	public final int FULL_MOD = 1;
	public final int WITHOUT_MOD = 2;
	
	// Constants
	private final String TREE = "tree";
	private final String ENTRIES ="entries";
	private final String ENTRY = "entry";
	
	private final String PROPERTIES = "properties";
	public final String ALLNODES = "allNodes";
	public final String FINALNODES = "finalNodes";
	public final String FAILRELINROOTS = "failRelInRoot";
	
	private final String QUERIES = "queries";
	private final String QUERY = "query";
	private final String WITHOUT = "without";
	private final String VALUE = "value";
	private final String BEGIN = "begin";
	private final String END = "end";
	private final String MATCH = "match";
	
	// Global Variables
	private Document doc;
	private NodeList treeList;
	
	public AhoCorasickXmlTreeReader(String fileName) throws ParserConfigurationException, SAXException, IOException {
		
		File xmlFile = new File("src/test/resources/trees.xml");
		
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		doc = dBuilder.parse(xmlFile);
		
		treeList = doc.getElementsByTagName(TREE);
	}
	
	/**
	 * Number of Trees in File
	 * @return
	 */
	public int numbersOfTrees(){
		return treeList.getLength();
	}
	
	/**
	 * Creates a List of all Entries 
	 * @param i - id of tree
	 * @return
	 */
	public List<ACEntry> getListOfEntriesByTree(int i, boolean tokens){
		List<ACEntry> list = new ArrayList<ACEntry>();
		
		Node treeNode = treeList.item(i);
		if(treeNode.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE){

			Element el = (Element) treeNode;
			Node entries = el.getElementsByTagName(ENTRIES).item(0);
			
			Element elEntries = (Element) entries;
			NodeList listEntry = elEntries.getElementsByTagName(ENTRY);
			
			for(int x = 0; x<listEntry.getLength(); x++){
				// Entry
				String entryStr = listEntry.item(x).getTextContent();
				if(tokens){
					String[] toAdd = entryStr.split("");
					toAdd = Arrays.copyOfRange(toAdd, 1, toAdd.length);
					entryStr = "";
					for(int y = 0; y<toAdd.length; y++){
						entryStr += toAdd[y]+" ";
					}
				}
				ACEntry entry = new ACEntry(entryStr);

				// Attribute
				NamedNodeMap attriMap = listEntry.item(x).getAttributes();
				
				for(int y = 0; y < attriMap.getLength(); y++){
					entry.addAttribute(attriMap.item(y).getNodeName(), attriMap.item(y).getTextContent());
				}
				
				list.add(entry);
			}
		}
		
		return list;
	}
	
	/**
	 * Map with properties of the tree
	 * @param i - id of tree
	 * @return
	 */
	public Map<String, String> getPropertiesOfTree(int i){
		
		Map<String, String> properties = new HashMap<String, String>();
		
		Node treeNode = treeList.item(i);
		if(treeNode.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE){

			Element el = (Element) treeNode;
			
			Node nodeProperties = el.getElementsByTagName(PROPERTIES).item(0);
			Element elProperties = (Element) nodeProperties;
			
			properties.put(ALLNODES, elProperties.getElementsByTagName(ALLNODES).item(0).getTextContent());
			properties.put(FINALNODES, elProperties.getElementsByTagName(FINALNODES).item(0).getTextContent());
			properties.put(FAILRELINROOTS, elProperties.getElementsByTagName(FAILRELINROOTS).item(0).getTextContent());
			
		}
		
		return properties;
	}
	
	/**
	 * Map with Query and List of Matches
	 * @param i - id of tree
	 * @return
	 */
	public Map<String, Object> queryOfTree(int i, int modus){
		
		Map<String, Object> queriesMap = new HashMap<String, Object>();
		
		Node treeNode = treeList.item(i);
		if(treeNode.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE){

			Element el = (Element) treeNode;
			Node queries = el.getElementsByTagName(QUERIES).item(0);
			
			Element elQueries = (Element) queries;
			NodeList listQueries = elQueries.getElementsByTagName(QUERY);
			
			for(int x = 0; x<listQueries.getLength(); x++){
				Element elQuery = (Element)listQueries.item(x);
				String query = elQuery.getAttribute(VALUE);
				
				NodeList listMatches = elQuery.getElementsByTagName(MATCH);
				
				Map<String, Object> matchProperties = new HashMap<String,Object>();
				
				for(int y = 0; y<listMatches.getLength(); y++){
					Map<String, Object> mapMatch = new HashMap<String, Object>();
					Element elMatch = (Element) listMatches.item(y);
					
					if( modus == FULL_MOD || Boolean.valueOf(elMatch.getAttribute(WITHOUT))){
						mapMatch.put(ACSearch.BEGIN, elMatch.getAttribute(BEGIN));
						mapMatch.put(ACSearch.END, elMatch.getAttribute(END));
						
						matchProperties.put(elMatch.getTextContent(), mapMatch);
					}
				}
				
				queriesMap.put(query, matchProperties);
			}
		}
		
		return queriesMap;
	}
	
}
