package edu.internet2.middleware.changelogconsumer.googleapps;

import edu.internet2.middleware.grouper.Group;

/**
 * Is a Google named index with a back link to the Grouper object. This allows for set comparisons with Google group objects.
 */
public class ComparableGroupItem {
    private String name;
    private Group grouperGroup;

    public ComparableGroupItem(String name) {
        this.name = name;
    }

    public ComparableGroupItem(String name, Group group) {
        this.name = name;
        this.grouperGroup = group;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return name.hashCode() == obj.hashCode();
    }

    public Group getGrouperGroup() {
        return grouperGroup;
    }
}
