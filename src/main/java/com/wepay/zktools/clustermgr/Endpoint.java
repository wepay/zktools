package com.wepay.zktools.clustermgr;

/**
 * This class represents the endpoint of a server
 */
public class Endpoint {

    /**
     * Host name
     */
    public final String host;
    /**
     * Port number
     */
    public final int port;

    public Endpoint(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Compares with the other Endpoint
     * @param other
     * @return true if the other endpoint has the same host and port, false otherwise.
     */
    public boolean eq(Endpoint other) {
        return other != null && this.host.equals(other.host) && this.port == other.port;
    }

    @Override
    public boolean equals(Object obj) {
        try {
            return eq((Endpoint) obj);
        } catch (ClassCastException ex) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return host.hashCode() ^ port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

}
