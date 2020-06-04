package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;
import java.util.List;

public class ImportIERelation {
    private int count;
    private List<ImportIERelationArgument> args = new ArrayList<>(2);

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public List<ImportIERelationArgument> getArgs() {
        return args;
    }

    public void setArgs(List<ImportIERelationArgument> args) {
        this.args = args;
    }

    public static ImportIERelation of(ImportIERelationArgument arg1, ImportIERelationArgument arg2, int count) {
        ImportIERelation r = new ImportIERelation();
        r.setCount(count);
        r.addArgument(arg1);
        r.addArgument(arg2);
        return r;
    }

    private void addArgument(ImportIERelationArgument argument) {
        args.add(argument);
    }
}
