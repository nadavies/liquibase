package liquibase.structure;

import liquibase.AbstractExtensibleObject;
import liquibase.util.StringUtils;

import java.util.*;

public class ObjectName extends AbstractExtensibleObject implements Comparable<ObjectName> {

    public String name;
    public ObjectName container;
    public boolean virtual;

    /**
     * Construct an ObjectName from the given string. If the string contains dots, it will be split into containers on the dots.
     * If null is passed, return an empty ObjectName
     */
    public static ObjectName parse(String string) {
        if (string == null) {
            return new ObjectName(null);
        }

        String[] split = string.split("\\.");
        return new ObjectName(split);
    }

    public ObjectName(ObjectName container, String... names) {
        this.container = container;
        if (names != null && names.length > 0) {
            if (names.length == 1) {
                this.name = names[0];
            } else {
                this.container = new ObjectName(container, Arrays.copyOfRange(names, 0, names.length - 1));
                this.name = names[names.length-1];
            }
        }
    }

    /**
     * Construct a new ObjectName, from a passed list of container names.
     * Name list goes from most general to most specific: new ObjectName("catalogName", "schemaName", "tablenName")
     */
    public ObjectName(String... names) {
        if (names == null || names.length == 0) {
            this.name = null;
        } else {
            ObjectName container = null;
            for (int i = 0; i < names.length - 1; i++) {
                container = new ObjectName(container, names[i]);
            }
            this.name = names[names.length - 1];
            this.container = container;
        }
    }

    public String toShortString() {
        return StringUtils.defaultIfEmpty(name, "#UNSET");
    }

    @Override
    public String toString() {
        List<String> list = asList();
        if (list.size() == 0) {
            return "#UNSET";
        }
        return StringUtils.join(list, ".", new StringUtils.StringUtilsFormatter<String>() {
            @Override
            public String toString(String obj) {
                return StringUtils.defaultIfEmpty(obj, "#UNSET");
            }
        });
    }

    @Override
    public int compareTo(ObjectName o) {
        if (this.name == null) {
            if (o.name == null) {
                return 0;
            } else {
                return -1;
            }
        }
        return this.name.compareTo(o.name);
    }

    public boolean equalsIgnoreCase(ObjectName name) {
        return this.name.equalsIgnoreCase(name.name);
    }


    /**
     * Same logic as {@link #equals(ObjectName, boolean)} with true for ignoreLengthDifferences
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ObjectName) {
            return equals(((ObjectName) obj), true);
        } else {
            return false;
        }
    }

    public boolean equals(ObjectName obj, boolean ignoreLengthDifferences) {
        if (ignoreLengthDifferences) {
            List<String> thisNames = this.asList();
            List<String> otherNames = obj.asList();
            int precision = Math.min(thisNames.size(), otherNames.size());

            thisNames = thisNames.subList(thisNames.size() - precision, thisNames.size());
            otherNames = otherNames.subList(otherNames.size() - precision, otherNames.size());

            for (int i=0; i<thisNames.size(); i++) {
                String thisName = thisNames.get(i);
                String otherName = otherNames.get(i);

                if (thisName == null) {
                    return otherName == null;
                }
                if (!thisName.equals(otherName)) {
                    return false;
                }
            }
            return true;
        } else {
            return this.toString().equals(obj.toString());
        }

    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }


    /**
     * Returns the {@link #asList()} result, but either null-padded out to the passed length, or truncated the the passed length
     */
    public List<String> asList(int length) {
        List<String> list = asList();
        if (length == list.size()) {
            return list;
        }
        if (length < list.size()) {
            return Collections.unmodifiableList(list.subList(list.size()-length, list.size()));
        }

        List<String> newList = new ArrayList<>(list);
        while (newList.size() < length) {
            newList.add(0, null);
        }
        return Collections.unmodifiableList(newList);
    }

    public List<String> asList() {
        if (name == null && container == null) {
            return new ArrayList<>();
        }

        List<String> returnList = new ArrayList<>();
        ObjectName name = this;
        while (name != null) {
            returnList.add(0, name.name);
            name = name.container;
        }

        if (returnList.get(0) == null) {
            boolean sawNonNull = false;
            ListIterator<String> it = returnList.listIterator();
            while (it.hasNext()) {
                String next = it.next();
                if (next == null && !sawNonNull) {
                    it.remove();
                } else {
                    sawNonNull = true;
                }
            }
        }

        return Collections.unmodifiableList(returnList);
    }



    /**
     * Return the number of parent containers in this ObjectName.
     * Top-level containers with a null name are not counted in the depth, but null-named containers between named containers are counted.
     */
    public int depth() {
        List<String> array = asList();
        if (array.size() == 0) {
            return 0;
        }
        return array.size() - 1;
    }


    /**
     * Returns true if the names are equivalent, not counting null-value positions in either name
     */
    public boolean matches(ObjectName objectName) {
        if (objectName == null) {
            return true;
        }

        List<String> thisList = this.asList();
        List<String> otherList = objectName.asList();

        if (otherList.size() == 0) {
            return true;
        }

        int length = Math.max(thisList.size(), otherList.size());

        thisList = this.asList(length);
        otherList = objectName.asList(length);

        for (int i=0; i<length; i++) {
            String thisName = thisList.get(i);
            String otherName = otherList.get(i);
            if (thisName != null && otherName != null) {
                if (!thisName.equals(otherName)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns an objectName that is truncated to the given max length
     */
    public ObjectName truncate(int maxLength) {
        List<String> names = this.asList();
        int length = Math.min(maxLength, names.size());

        return new ObjectName(names.subList(names.size()-length, names.size()).toArray(new String[length]));
    }
}
