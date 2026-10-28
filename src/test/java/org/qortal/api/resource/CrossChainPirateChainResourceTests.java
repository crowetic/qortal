package org.qortal.api.resource;

import org.junit.Test;
import org.bitcoinj.core.Bech32;

import static org.junit.Assert.*;

public class CrossChainPirateChainResourceTests {

	@Test
	public void publicValidatorAcceptsBothShieldedPoolsWithoutInitializingAWallet() {
		CrossChainPirateChainResource resource = new CrossChainPirateChainResource();
		String sapling = "zs1mj80tkyjqjhc84aquetymwgrasxjdkkft0qcn5xtp2p9r42vs5ernyv4nzgufmz6xzuqvw20w9z";
		String ironwood = "pirate1lyp0g5438qkp9s5me5epku9ak9k0dxqsfyt9z894zym8qw38sps488ljyee27yw29vc2cyj8mse";
		assertTrue(resource.validateAddress(sapling));
		assertTrue(resource.validateAddress(ironwood));
		assertFalse(resource.validateAddress(ironwood.substring(0, 81) + "q"));
		assertFalse(resource.validateAddress("P" + ironwood.substring(1)));
		assertFalse(resource.validateAddress(Bech32.encode("pirate-test", Bech32.decode(ironwood).data)));
		assertFalse(resource.validateAddress(null));
		assertFalse(resource.validateAddress(""));
	}

	@Test
	public void testRequestLogRedactsApiKeyQueryParameter() {
		assertEquals("json=true&apiKey=<redacted>",
				CrossChainPirateChainResource.redactQueryString("json=true&apiKey=test-key"));
		assertEquals("foo=bar&apikey=<redacted>&empty=",
				CrossChainPirateChainResource.redactQueryString("foo=bar&apikey=another-key&empty="));
		assertEquals("json=true", CrossChainPirateChainResource.redactQueryString("json=true"));
	}
}
