package edu.internet2.middleware.changelogconsumer.googleapps.utils;

import edu.internet2.middleware.grouper.Member;

/**
 * Is a Google named index with a back link to the Grouper subject object. This allows for set comparisons with Google user objects.
 *
 * @author John Gasper, Unicon
 */
public class ComparableMemberItem {
    private String email;
    private Member grouperMember;

    public ComparableMemberItem(String email) {
        this.email = email;
    }

    public ComparableMemberItem(String name, Member member) {
        this.email = name;
        this.grouperMember = member;
    }

    public String getEmail() {
        return email;
    }

    @Override
    public String toString() {
        return email;
    }

    @Override
    public int hashCode() {
        return email.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return email.hashCode() == obj.hashCode();
    }

    public Member getGrouperMember() {
        return grouperMember;
    }
}
