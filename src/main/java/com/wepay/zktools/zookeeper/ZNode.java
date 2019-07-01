package com.wepay.zktools.zookeeper;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * An abstraction of a znode in ZooKeeper
 */
public class ZNode implements Comparable<ZNode> {

    public final String path;

    public ZNode(ZNode parent, String name) throws IllegalArgumentException {
        this(parent.path + "/" + checkName(name));
    }

    public ZNode(String path) throws IllegalArgumentException {
        this.path = checkPath(path);
    }

    public String name() {
        int lastSlash = path.lastIndexOf('/');
        return path.substring(lastSlash + 1);
    }

    public ZNode parent() {
        try {
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash > 0) {
                return new ZNode(path.substring(0, lastSlash));
            } else {
                return null;
            }
        } catch (IllegalArgumentException ex) {
            throw new IllegalStateException();
        }
    }

    public List<ZNode> ancestors() {
        List<ZNode> list = new LinkedList<>();

        ZNode znode = this.parent();
        while (znode != null) {
            list.add(znode);
            znode = znode.parent();
        }
        Collections.reverse(list);

        return list;
    }


    public static String checkPath(String path) throws IllegalArgumentException {
        if (!path.equals(path.trim())) {
            throw new IllegalArgumentException("path contains a leading or trailing space: " + path);
        }

        if (!path.startsWith("/"))
            throw new IllegalArgumentException("not an absolute path: " + path);

        if (path.endsWith("/"))
            throw new IllegalArgumentException("trailing slash not allowed: " + path);

        if (path.contains("//"))
            throw new IllegalArgumentException("empty node name: " + path);

        return path;
    }

    public static String checkName(String name) throws IllegalArgumentException {
        if (!name.equals(name.trim())) {
            throw new IllegalArgumentException("node name contains a leading or trailing space: " + name);
        }
        if (name.contains("/")) {
            throw new IllegalArgumentException("node name contains a slash: " + name);
        }
        return name;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ZNode) {
            ZNode other = (ZNode) obj;
            return this.path.equals(other.path);
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(ZNode other) {
        return this.path.compareTo(other.path);
    }

    @Override
    public String toString() {
        return path;
    }
}
