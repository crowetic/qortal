package org.qortal.network;

import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import org.qortal.settings.Settings;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.net.*;

/**
 * Convenience class for encapsulating/parsing/rendering/converting IP peer addresses
 * including late-stage resolving before actual use by a socket.
 */
// All properties to be converted to JSON via JAXB
@XmlAccessorType(XmlAccessType.FIELD)
public class IPPeerAddress {

	// Properties
	private String host;
	private int port;

  @PeerAddressCtor("host-port")
	private IPPeerAddress(String host, int port) {
		this.host = host;
		this.port = port;
	}

    @PeerAddressCtor("address")
    public IPPeerAddress(String address) {
      IPPeerAddress ip = IPPeerAddress.fromString(address);
      this.host = ip.host;
      this.port = ip.port;
    }

	// Constructors

	// For JAXB
	protected IPPeerAddress() {
	}

	/** Constructs new IPPeerAddress using remote address from passed connected socket. */
	public static IPPeerAddress fromSocket(Socket socket) {
		InetSocketAddress socketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
		InetAddress address = socketAddress.getAddress();

		String host = InetAddresses.toAddrString(address);

		// Make sure we encapsulate IPv6 addresses in brackets
		if (address instanceof Inet6Address)
			host = "[" + host + "]";

		return new IPPeerAddress(host, socketAddress.getPort());
	}

	/**
	 * Constructs new IPPeerAddress using hostname or literal IP address and optional port.<br>
	 * Literal IPv6 addresses must be enclosed within square brackets.
	 * <p>
	 * Examples:
	 * <ul>
	 * <li>peer.example.com
	 * <li>peer.example.com:9084
	 * <li>192.0.2.1
	 * <li>192.0.2.1:9084
	 * <li>[2001:db8::1]
	 * <li>[2001:db8::1]:9084
	 * </ul>
	 * <p>
	 * Not allowed:
	 * <ul>
	 * <li>2001:db8::1
	 * <li>2001:db8::1:9084
	 * </ul>
	 */
	public static IPPeerAddress fromString(String addressString) throws IllegalArgumentException {
		boolean isBracketed = addressString.startsWith("[");

		// Attempt to parse string into host and port
		HostAndPort hostAndPort = HostAndPort.fromString(addressString).withDefaultPort(Settings.getInstance().getDefaultListenPort()).requireBracketsForIPv6();

		String host = hostAndPort.getHost();
		if (host.isEmpty())
			throw new IllegalArgumentException("Empty host part");

		// Validate IP literals by attempting to convert to InetAddress, without DNS lookups
		if (host.contains(":") || host.matches("[0-9.]+"))
			InetAddresses.forString(host);

		// If we've reached this far then we have a valid address

		// Make sure we encapsulate IPv6 addresses in brackets
		if (isBracketed)
			host = "[" + host + "]";

		return new IPPeerAddress(host, hostAndPort.getPort());
	}

	// Getters

	/** Returns hostname or literal IP address, bracketed if IPv6 */
	public String getHost() {
		return this.host;
	}

	public int getPort() {
		return this.port;
	}

	// Conversions

	/** Returns InetSocketAddress for use with Socket.connect(), or throws UnknownHostException if address could not be resolved by DNS lookup. */
	public InetSocketAddress toSocketAddress() throws UnknownHostException {
		// Attempt to construct new InetSocketAddress with DNS lookups.
		// There's no control here over whether IPv6 or IPv4 will be used.
		InetSocketAddress socketAddress = new InetSocketAddress(this.host, this.port);

		// If we couldn't resolve then return null
		if (socketAddress.isUnresolved())
			throw new UnknownHostException();

		return socketAddress;
	}

	@Override
	public String toString() {
		return this.host + ":" + this.port;
	}

	// Utilities

	/** Returns true if other IPPeerAddress has same port and same case-insensitive host part, without DNS lookups */
	public boolean equals(IPPeerAddress other) {
		// Ports must match
		if (this.port != other.port)
			return false;

		// Compare host parts but without DNS lookups
		return this.host.equalsIgnoreCase(other.host);
	}

}
