package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;

public class ImportIERelationArgument extends ArrayList<String> {
    public String getId() {
        return get(0);
    }

    public ImportIERelationArgument() {
    }

    public ImportIERelationArgument(String id, String source) {
        super(2);
        add(id);
        add(source);
    }

    public String getSource() {
        return size() > 1 ? get(1) : null;
    }
}
