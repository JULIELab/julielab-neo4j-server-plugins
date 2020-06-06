package de.julielab.neo4j.plugins.datarepresentation;

import java.util.ArrayList;
import java.util.List;

public class ImportIERelation {
    private int count;
    private List<ImportIERelationArgument> args = new ArrayList<>(2);

    public static ImportIERelation of(int count, ImportIERelationArgument arg1, ImportIERelationArgument arg2) {
        ImportIERelation r = new ImportIERelation();
        r.setCount(count);
        r.addArgument(arg1);
        r.addArgument(arg2);
        return r;
    }

    public static ImportIERelation of(int count, ImportIERelationArgument... args) {
        ImportIERelation r = new ImportIERelation();
        r.setCount(count);
        for (ImportIERelationArgument arg : args)
            r.addArgument(arg);
        return r;
    }

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

    private void addArgument(ImportIERelationArgument argument) {
        args.add(argument);
    }
}
