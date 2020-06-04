package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;

public class ImportIERelation extends ArrayList<ImportIERelationArgument> {
    public ImportIERelation() {
        super(2);
    }
    public ImportIERelation(ImportIERelationArgument arg1, ImportIERelationArgument arg2) {
        super(2);
        add(arg1);
        add(arg2);
    }
}
