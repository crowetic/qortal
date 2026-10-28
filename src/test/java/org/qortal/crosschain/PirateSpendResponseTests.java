package org.qortal.crosschain;

import org.json.JSONObject;
import org.junit.Test;
import org.qortal.api.model.crosschain.PirateChainSpendability;
import org.qortal.test.common.Common;

import static org.junit.Assert.*;

/** Offline fixtures for the pinned JNI's compatibility and typed response contracts. */
public class PirateSpendResponseTests extends Common {

	@Test
	public void sendAndTradeFundingRecognizeActualInsufficientFunds() throws Exception {
		for (String command : new String[] { "send", "sendp2sh" }) {
			String error = "Insufficient funds: need 500020000 arrrtoshis, have 400000000 arrrtoshis";
			ForeignBlockchainException failure = failure(new JSONObject().put("error", error).toString(), command);
			assertTrue(failure instanceof ForeignBlockchainException.InsufficientFundsException);
			assertEquals(error, failure.getMessage());
		}
	}

	@Test
	public void rescanRepairAndBroadcastFailuresAreNotBalanceFailures() throws Exception {
		for (String error : new String[] {
				"ERR_RESCAN_REQUIRED: Wallet requires a full rescan before spending.",
				"ERR_ANCHOR_NOT_VALIDATED: Wallet anchor is not validated",
				"Wallet isn't synchronized yet",
				"Input address pirate1example belongs to a key group with no spendable shielded notes",
				"Broadcast failed: server unavailable" }) {
			for (String command : new String[] { "send", "sendp2sh" }) {
				ForeignBlockchainException failure = failure(new JSONObject().put("error", error).toString(), command);
				assertFalse(failure instanceof ForeignBlockchainException.InsufficientFundsException);
				assertEquals(error, failure.getMessage());
			}
		}
	}

	@Test
	public void acceptsSuccessfulTransactionIds() throws Exception {
		String txId = "0123456789abcdef".repeat(4);
		for (String command : new String[] { "send", "sendp2sh" })
			assertEquals(txId, PirateChain.parseSpendResponse(new JSONObject().put("txid", txId).toString(), command));
	}

	@Test
	public void malformedAndAmbiguousResponsesNeverClaimSuccess() throws Exception {
		for (String response : new String[] { "", "not JSON", "[]", "{}", "{\"txid\":null}",
				"{\"txid\":123}", "{\"txid\":\"\"}", "{\"txid\":\"abc\",\"error\":\"failed\"}",
				"{\"txid\":\"abc\",\"error\":{\"message\":\"failed\"}}" }) {
			assertFalse(failure(response, "send") instanceof ForeignBlockchainException.InsufficientFundsException);
		}
	}

	@Test
	public void exposesRescanRequiredEvenWhenThereIsVerifiedBalance() throws Exception {
		// Observed with the historical loopback fixture: balance is verified, but its anchor is not spend-ready.
		JSONObject result = status(false, true, "ERR_RESCAN_REQUIRED");
		PirateChainSpendability parsed = PirateChain.parseSpendabilityResponse(envelope(result));
		assertFalse(parsed.spendable);
		assertTrue(parsed.rescan_required);
		assertEquals("ERR_RESCAN_REQUIRED", parsed.reason_code);
		assertEquals(152859L, parsed.target_height);
		assertEquals(152858L, parsed.anchor_height);
		assertEquals(0L, parsed.validated_anchor_height);
		assertFalse(parsed.repair_queued);
	}

	@Test
	public void exposesReadyAndQueuedRepairStatesIndependently() throws Exception {
		JSONObject ready = status(true, false, "READY").put("validated_anchor_height", 152858L);
		assertTrue(PirateChain.parseSpendabilityResponse(envelope(ready)).spendable);
		JSONObject repairing = status(false, false, "ERR_ANCHOR_NOT_VALIDATED").put("repair_queued", true);
		PirateChainSpendability parsed = PirateChain.parseSpendabilityResponse(envelope(repairing));
		assertFalse(parsed.spendable);
		assertFalse(parsed.rescan_required);
		assertTrue(parsed.repair_queued);
	}

	@Test
	public void incompleteOrFailedReadinessDoesNotDefaultToReady() throws Exception {
		JSONObject incomplete = status(true, false, "READY");
		incomplete.remove("rescan_required");
		for (String response : new String[] { "{}", "not JSON", envelope(incomplete),
				"{\"ok\":true,\"result\":null}", "{\"ok\":true,\"result\":[]}",
				"{\"ok\":false,\"error\":\"wallet is locked\"}" }) {
			try {
				PirateChain.parseSpendabilityResponse(response);
				fail("Incomplete native readiness must fail closed");
			} catch (ForeignBlockchainException expected) {
				assertFalse(expected instanceof ForeignBlockchainException.InsufficientFundsException);
				if (response.contains("wallet is locked"))
					assertTrue(expected.getMessage().contains("wallet is locked"));
			}
		}
	}

	private static ForeignBlockchainException failure(String response, String command) throws Exception {
		try {
			PirateChain.parseSpendResponse(response, command);
			throw new AssertionError("Expected native failure");
		} catch (ForeignBlockchainException expected) {
			return expected;
		}
	}

	private static JSONObject status(boolean spendable, boolean rescanRequired, String reasonCode) {
		return new JSONObject().put("spendable", spendable).put("rescan_required", rescanRequired)
				.put("target_height", 152859L).put("anchor_height", 152858L).put("validated_anchor_height", 0L)
				.put("repair_queued", false).put("reason_code", reasonCode);
	}

	private static String envelope(JSONObject result) {
		return new JSONObject().put("ok", true).put("result", result).toString();
	}
}
