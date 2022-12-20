# Changelog

## v3.2.0 (20/12/2022)
- [**enhancement**] Export: Allow to add source prefix to lingpipe dictionary creation [#31](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/31)
- [**enhancement**] Allow the specification of the name property for the creation of equal_name aggregates [#30](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/30)

---

## v3.1.0 (11/12/2022)
This release contains a  number of bug fixes and API enhancements not mentioned in the issues below (unfortunately).

- [**closed**] Boost Neo4j version to 4.4.2. [#23](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/23)
- [**closed**] Add a single endpoint to create schema indexes [#9](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/9)
- [**closed**] Raise Neo4j version to 4.x [#8](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/8)
- [**closed**] Remove legacy indexes [#7](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/7)

---

## v3.0.0 (19/10/2020)
This release brings compatibility of the plugin mechanisms with Neo4j 4.
In Neo4j , the legacy server plugin extension mechanism was removed. What had been a server plugin in this project earlier has been converted to an unmanaged extension.

- [**closed**] Fix Fulltext index performance issues [#16](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/16)
- [**closed**] Make the primary concept source in `ImportConcepts` a stream [#15](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/15)
- [**closed**] Add a temporary properties map to `ImportConcept` [#14](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/14)
- [**closed**] Add the total number of concepts to `ImportConcepts`. [#13](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/13)
- [**closed**] Add an endpoint for IE-based relation insertion [#12](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/12)
- [**closed**] Insert concepts from a stream [#11](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/11)
- [**closed**] Let insertMappings use ImportMapping objects [#10](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/10)
- [**closed**] Convert ServerPlugins to Unmanaged Server Extensions [#6](https://github.com/JULIELab/julielab-neo4j-server-plugins/issues/6)

---

## v2.1.0 (14/05/2020)
Works with Neo4j version 3.5.17.
---

## v.1.7.5 (11/02/2019)
Library versions match those of Neo4j 3.4.12.
This is the last version of this plugin in the 1.7.x series.
From Neo4j 3.5 onwards server plugins are deprecated and one should use unmanaged extensions instead.
---

## v2.0.0 (30/11/2018)
This release removes a lot of deprecated elements like constants and classes and thus is not downwards compatible to the 1.x series of releases.
Some quality-of-life utility methods have been added to the NodeUtilities.
Update to Neo4j 3.4.7.
---

## v1.7.4 (25/09/2018)
Synced dependencies with Neo4j 3.2.12.
Exchanged deprecated Neo4j classes with recommended replacements.
---

## v1.9.0 (17/09/2018)
Pushed Neo4j version to 3.4.7.
---

## v1.8.1 (17/09/2018)
Included IBM RuleBasedCollator in the plug-in to correctly load TermNameAndSynonymComparator.
Included Jackson (FasterXML) to correctly load ConceptsJsonSerializer.
---

## v1.7.3 (17/09/2018)
Included IBM RuleBasedCollator in the plug-in to correctly load TermNameAndSynonymComparator.
The ConceptManager now logs into Neo4j's log file.
---

## v1.8.0 (07/02/2018)
Supports Neo4j v3.3.1.
Also, the import representation classes for concepts, facets etc. have been moved into a subproject of their own, julielab-neo4j-plugins-concepts-representation. This allows the lightweight usage of those classes in other projects, e.g. in the [JULIE Lab Concept DB Manager](https://github.com/JULIELab/julielab-concept-db-manager) which makes heavy use of these plugins.

---

## v1.7.0 (03/11/2017)
Support for Neo4j 3.2.3.
The Aho-Corasick subproject has been removed. For details refer to the [respective commit](https://github.com/JULIELab/julielab-neo4j-server-plugins/commit/0d01f9e825bf87d88b482f649a579efa38abe768).
---

## v1.6.0 (02/08/2017)
This release brings a lot of order into IDs and sources of concepts. Concepts - which inacurrately have been called 'terms' before - may come from various sources. It is even possible that the very same concept is included in multiple sources that should be imported into the database. This is actually the main reason why the concept plugin has been created in the first place. The following issues may arise:
* Different concepts from different sources (e.g. databases) may have the same IDs
  * while the IDs will be unique in the source database, they might not be across databases; an example for this is NCBI Gene and the NCBI Taxonomy which both use plain numbers as IDs
* The same concepts from different sources may have different IDs in those respective sources
  * occurs, for example, when importing BioPortal ontologies that are just a reformulation of a database originally existing without being an ontology. Examples are all the UMLS ontologies that have been imported into BioPortal.

Within the plugin, each concept may have an original ID paired with the original source. This should be the unique ID from the respective original database the concept came from. Then, there is the source ID paired with a source. Each database containing a concept may be a source of it. Thus, in Neo4j, each concept may have multiple source IDs and multiple sources. All these items (original ID / original source, [secondary] source ID / [secondary] source) have been taken together to form the **concept coordinates**. Now, each imported concept must have coordinates. It is not required to have original source coordinates since they might not be known. The rule that checks if a concept already exist works as follows:

    Two terms are equal, iff 
    * they have the same original source ID assigned from the same original source or
    * both have no contradicting original ID and original source but the same source ID and source.
    * Contradicting means two non-null values that are not equal.

Coordinates are now used always when a particular concept should be addressed. That means that also for the connections the a parent concept, coordinates are used.
Currently there exist two different coordinates classes, the `ConceptCoordinates` and the `TermCoordinates`. The `TermCoordinates` are older and only represent an `(ID,source)` pair without specifying if it is an original ID and source or a secondary source. The `TermCoordinates` are marked as deprecated and will be removed in future versions. There are currently used in some places and should be replaced by `ConceptCoordinates`.

Also, the internal process of term insertion has been restructured and should be more efficient now.
---

## v1.5.0 (23/06/2017)
This is the first GitHub-based release of the JULIE Lab Neo4j server plugins.
The plugins are mainly two-fold: One plugin for ontological concept handling, the other for a persistent realization of the Aho-Corasick algorithm. 

The concept related plugin can be used with the output of the [julielab-bioportal-ontology-tools](https://github.com/JULIELab/julielab-bioportal-ontology-tools) or any other graph-data in the required JSON format. The concepts are then arranged within Neo4j as a graph that arranges the concepts in "facets" where a facet may just be one ontology or terminology. The main achievement of the plugin is the automatic merging of concepts that have the same ID. When importing two ontologies that share concepts, the concepts won't be added twice but just get different relationships corresponding to the facet they belong to. Also concept mappings may be integrated into the graph, creating mapping relationships between concepts and resulting in aggregate nodes (roughly equivalent to blank nodes in RDF) aggregating all concepts together that have been mapped to each other (assuming the mapping to be impose an equivalence relation which actually might not be true!).

The Aho-Corasick algorithm is a TRIE-based string matching algorithm that is typically performed by an in-memory data structure. For extremely large dictionaries this might be an issue, however. This plugin was created to allow string tagging from Neo4j, resulting in a much slower algorithm but allowing much larger dictionaries. Thus, the plugin might of use if there is very large dictionary for which the TRIE does not fit into the memory any more but for which only few or not time critical requests are issued. Please note that the plugin is currently not working because it has been developed with an older version of Neo4j and hasn't been updated to the 2.3 APIs.