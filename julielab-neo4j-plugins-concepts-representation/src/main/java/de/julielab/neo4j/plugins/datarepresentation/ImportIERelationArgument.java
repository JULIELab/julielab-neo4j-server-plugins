package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;

public class ImportIERelationArgument extends ArrayList<String> {
    private String[] idSplit;

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

    /**
     * <p>Returns the portion of the ID up to the first ':'. Since the idProperty specification is optional, the result
     * might be the whole ID or some random prefix like 'http' when the ID is an URL. The calling code needs to check
     * if the resulting string is actually an ID property.</p>
     *
     * @return The ID property prefix of this argument's ID.
     */
    public String getIdProperty() {
        splitID();
        return idSplit.length > 1 ? idSplit[0] : null;
    }

    private void splitID() {
        if (idSplit == null)
            idSplit = get(0).split(":");
    }

    public String getId() {
        splitID();
        return idSplit.length > 1 ? idSplit[1] : idSplit[0];
    }

    public boolean hasIdProperty() {
        splitID();
        return getIdProperty() != null && !getId().equals(getIdProperty());
    }

    public String getSource() {
        return size() > 1 ? get(1) : null;
    }

    public boolean hasSource() {
        return getSource() != null;
    }
}
