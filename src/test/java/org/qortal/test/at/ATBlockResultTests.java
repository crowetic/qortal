package org.qortal.test.at;

import org.junit.Test;
import org.qortal.at.ATBlockResult;
import org.qortal.at.ATStateBundle;
import org.qortal.crypto.Crypto;
import org.qortal.data.at.ATData;
import org.qortal.data.at.ATExecutionState;
import org.qortal.data.at.ATStateData;
import org.qortal.repository.DataException;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ATBlockResultTests {

	private ATBlockResult.Entry entry() {
		byte[] stateHash = new byte[32];
		byte[] parentHash = new byte[32];
		stateHash[0] = 1;
		parentHash[0] = 2;
		return new ATBlockResult.Entry("AT-address", stateHash, 12L, false, 34L,
				7, parentHash, true, 9, false, false, true, 56L, 78L);
	}

	@Test
	public void parentMetadataIsCapturedBeforeExecutionMutation() {
		long parentCursor = 123456789L;
		ATData parent = new ATData("AT-address", new byte[32], 1L, 2, 0L,
				new byte[]{1}, Crypto.digest(new byte[]{1}), true, 99,
				false, false, true, 25L, parentCursor);
		ATBlockResult.ParentMetadata snapshot = new ATBlockResult.ParentMetadata(parent);

		// Waking an AT mutates this same ATData instance during candidate execution.
		parent.setIsSleeping(false);
		parent.setSleepUntilHeight(0);
		parent.setSleepUntilMessageTimestamp(null);

		byte[] parentHash = Crypto.digest(new byte[]{2});
		byte[] stateHash = Crypto.digest(new byte[]{3});
		ATStateData state = new ATStateData("AT-address", 8, new byte[]{3}, stateHash,
				1L, false, null);
		ATBlockResult.Entry receipt = new ATBlockResult.Entry(state, snapshot,
				new ATExecutionState("AT-address", 7, parentHash, 100L));

		assertTrue(receipt.prevIsSleeping);
		assertEquals(Integer.valueOf(99), receipt.prevSleepUntilHeight);
		assertEquals(Long.valueOf(parentCursor), receipt.prevSleepUntilMessageTimestamp);
	}

	@Test
	public void versionedEnvelopeRoundTripsAndRejectsCorruption() throws Exception {
		byte[] encoded = ATBlockResult.serialize(Collections.singletonList(entry()));
		ATBlockResult.Entry decoded = ATBlockResult.deserialize(encoded).get(0);
		assertEquals("AT-address", decoded.address);
		assertEquals(12L, decoded.fees);
		assertEquals(7, decoded.prevHeight);
		assertTrue(decoded.prevIsSleeping);

		byte[] corrupted = encoded.clone();
		corrupted[corrupted.length - 1] ^= 1;
		try {
			ATBlockResult.deserialize(corrupted);
			fail("corrupt block result accepted");
		} catch (DataException expected) {
			assertTrue(expected.getMessage().contains("checksum"));
		}

		byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
		try {
			ATBlockResult.deserialize(trailing);
			fail("block result with trailing data accepted");
		} catch (DataException expected) {
			assertTrue(expected.getMessage().contains("length"));
		}
	}

	@Test
	public void stateBundleRoundTripsAndRejectsCorruption() throws Exception {
		byte[] first = {1, 2, 3};
		byte[] second = {4, 5, 6, 7};
		Map<String, ATStateBundle.Entry> entries = new LinkedHashMap<>();
		entries.put("AT-one", new ATStateBundle.Entry(Crypto.digest(first), first));
		entries.put("AT-two", new ATStateBundle.Entry(Crypto.digest(second), second));

		byte[] encoded = ATStateBundle.serialize(entries);
		Map<String, ATStateBundle.Entry> decoded = ATStateBundle.deserialize(encoded);
		assertArrayEquals(first, decoded.get("AT-one").data);
		assertArrayEquals(Crypto.digest(second), decoded.get("AT-two").hash);

		byte[] corrupted = encoded.clone();
		corrupted[corrupted.length - 1] ^= 1;
		try {
			ATStateBundle.deserialize(corrupted);
			fail("corrupt state bundle accepted");
		} catch (DataException expected) {
			assertTrue(expected.getMessage().contains("checksum"));
		}
	}
}
