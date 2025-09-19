package org.qortal.network;

import org.qortal.repository.DataException;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public interface PeerAddress {

    //default PeerAddress fromSocket(Socket socket) { return null; }
    //default PeerAddress fromString(String address) { return null; }
    InetSocketAddress toSocketAddress() throws UnknownHostException;

    default String getHost() { return null; }
    default int getPort() { return -1; }
}

