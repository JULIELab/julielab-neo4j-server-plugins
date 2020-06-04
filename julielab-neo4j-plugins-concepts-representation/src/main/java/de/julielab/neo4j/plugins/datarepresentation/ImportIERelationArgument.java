package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;

public class ImportIERelationArgument extends ArrayList<String> {
    public ImportIERelationArgument() {
        super(2);
    }

    public ImportIERelationArgument(int initialCapacity) {
        super(initialCapacity);
    }

    public static ImportIERelationArgument of(String id, String source) {
        ImportIERelationArgument a = new ImportIERelationArgument();
        a.add(id);
        a.add(source);
        return a;
    }

    public static ImportIERelationArgument of(String id) {
        ImportIERelationArgument a = new ImportIERelationArgument(1);
        a.add(id);
        return a;
    }

    public String getId() {
        return get(0);
    }

    public String getSource() {
        return size() > 1 ? get(1) : null;
    }
}
