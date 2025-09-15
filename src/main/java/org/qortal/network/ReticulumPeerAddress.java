package org.qortal.network;

//import lombok.Data;
//import lombok.extern.slf4j.Slf4j;
//import org.qortal.settings.Settings;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.net.InetSocketAddress;

/**
 * Convenience class for encapsulating/parsing/rendering/converting IP peer addresses
 * including late-stage resolving before actual use by a socket.
 */
// All properties to be converted to JSON via JAXB
//@Data
//@Slf4j
@XmlAccessorType(XmlAccessType.FIELD)
public class ReticulumPeerAddress implements PeerAddress {

    byte[] dhash;

	// Constructors

	// For JAXB
	protected ReticulumPeerAddress() {
	}

    @PeerAddressCtor("destination")
	public ReticulumPeerAddress(byte[] dhash) {
      this.dhash = dhash;
  }

    public static ReticulumPeerAddress fromString(String addressString) throws IllegalArgumentException {
        return null;
    }

    @Override
    public InetSocketAddress toSocketAddress() {
        return null;
    }

}
