# JULIELab Neo4j Server Plugins
![Java CI with Maven](https://github.com/JULIELab/julielab-neo4j-server-plugins/workflows/Java%20CI%20with%20Maven/badge.svg?branch=neo4j4.0)
[![Automated Release Notes by gren](https://img.shields.io/badge/%F0%9F%A4%96-release%20notes-00B2EE.svg)](https://github-tools.github.io/github-release-notes/)

# Introduction

This repository contains the "Concepts" Neo4j Plugin used in the JULIE Lab for the semantic search engine [Semedico](https://github.com/JULIELab/semedico) and the molecular event retrieval search engine [GePI](https://github.com/JULIELab/gepi).

The main code is `julielab-neo4j-plugins-concepts` sub-project. The projects `julielab-neo4j-plugins-concepts-representation` and `julielab-neo4j-plugins-utilities` serve as modules to be imported into the main code.

The purpose of this project is to realize hierarchies of semantic concepts from the life sciences (genes, gene families, gene groups, chemicals, species, experimental methods etc.) with semantic relationships (`IS_BROADER_THAN` `INTERACTS_WITH` etc.) as Neo4j nodes als relationships. An application can use this to realize functionality like hierarchical faceted search, navigating the concept hierarchy or objects related to the objects (like articles or mentions of molecular events that include those objects). This is done, for example, in the applications Semedico and GePI mentioned at the beginning of this README.

The repository is called "plugins" because in previous Neo4j versions, there were so-called "Server Plugins". Those were managed plugins (in contrast to the more powerful but unrestricted [unmanaged server extensions](https://neo4j.com/docs/java-reference/4.4/extending-neo4j/unmanaged-extensions/). When Server Plugins became deprecated in Neo4j, this project was refactored to use unmanaged server extensions.

The main plugin is found in `julielab-neo4j-plugins-concepts/src/main/java/de/julielab/neo4j/plugins/concepts/ConceptManager.java`. It contains endpoints to add concepts and their relationships in a specific JSON format that is modeled in the `julielab-neo4j-plugins-concepts-representation/src/main/java/de/julielab/neo4j/plugins/datarepresentation/ImportConcepts` class. Concepts have a preferred name, synonyms, descriptions and belong to a concept group called `facet` (named in the sense of faceted search where the different concept groups serve as navigable facets in Semedico). Facets are modeled as Neo4j nodes on their own.

There is logic to allow multiple imports of the same concepts but with different sets of information that is then merged. For this purpose, each concept has an `originalId` an  an `originalSource` that are unique in the Neo4j database. This makes sense for ontologies, for example, where some ontologies import concepts from other ontologies but leave out information or augment on the information of the original ontology.

All this logic does have some performance penalty. This project should not be used without necessity. If the raw data is well-structured and can be imported with Neo4j's built-in data import mechanisms, those should be preferred.
This code is highly opinionated and closely adapted to the needs of Semedico and GePI which also includes the export of dictionaries, concept ID hierarchies and much more.

# Installation

Check the changelog or the GitHub release page for the latest compatible Neo4j version.

Build the plugins with Maven in the root directory of the repository:

`mvn clean package`

or, if you are not interested in running the tests,

`mvn clean package -DskipTests=true`

You will need the JAR file that has all the required dependencies packaged within. After a successful Maven build, this file is found at

`julielab-neo4j-plugins-concepts/target/julielab-neo4j-plugins-concepts-3.2.1-assembly.jar`

Copy this file into the `plugins` directory of your Neo4j installation. Then, add this line in the `config/neo4j.conf` file:

`dbms.unmanaged_extension_classes=de.julielab.neo4j.plugins=/concepts`
After a restart of Neo4j, the plugin should work. At startup, the Neo4j logs in `logs/neo4j.log` should show a line like

`INFO  Mounted unmanaged extension [de.julielab.neo4j.plugins] at [/concepts]`

