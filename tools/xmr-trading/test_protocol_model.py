import unittest
from protocol_model import (ClaimPolicy, Model, State, counterexamples,
    seller_gets_both, stranded_xmr, unbound_claim, unfunded_qort_claim)


class ProtocolModelTests(unittest.TestCase):
    def test_readiness_alone_strands_funded_xmr_when_seller_disappears(self):
        model = Model(ClaimPolicy.REQUIRE_READY)
        trace, final, _ = model.find(stranded_xmr)
        self.assertIn("buyer_locks_xmr", trace)
        self.assertIn("seller_disappears", trace)
        self.assertIn("at_refunds_qort", trace)
        self.assertEqual([], list(model.successors(final)))

    def test_timeout_fallback_can_pay_an_unfunded_buyer(self):
        trace, final, _ = Model(ClaimPolicy.AFTER_FIRST_TIMEOUT).find(unfunded_qort_claim)
        self.assertNotIn("buyer_locks_xmr", trace)
        self.assertFalse(final.ready)
        self.assertEqual("buyer", final.qort)

    def test_readiness_blocks_that_specific_unfunded_claim(self):
        self.assertIsNone(Model(ClaimPolicy.REQUIRE_READY).find(unfunded_qort_claim))

    def test_public_mempool_secret_is_not_undone_by_failed_claim(self):
        trace, final, _ = Model(ClaimPolicy.REQUIRE_READY).find(seller_gets_both)
        self.assertIn("buyer_publishes_claim", trace)
        self.assertNotIn("claim_confirms", trace)
        self.assertIn("at_refunds_qort", trace)
        self.assertTrue(final.buyer_share_public)

    def test_secret_binding_is_a_separate_requirement(self):
        self.assertIsNone(Model(ClaimPolicy.REQUIRE_READY).find(unbound_claim))
        self.assertIsNotNone(Model(ClaimPolicy.REQUIRE_READY, False).find(unbound_claim))

    def test_successful_honest_path_exists_but_does_not_establish_safety(self):
        for policy in ClaimPolicy:
            found = Model(policy).find(lambda s: s.qort == "buyer" and s.xmr == "seller")
            self.assertIsNotNone(found)
            self.assertIn("claim_confirms", found[0])

    def test_early_abort_can_return_both_funded_assets(self):
        found = Model(ClaimPolicy.REQUIRE_READY).find(lambda s: s.qort == "returned" and s.xmr == "returned")
        self.assertIsNotNone(found)
        self.assertIn("seller_aborts_early", found[0])
        self.assertIn("buyer_recovers_xmr", found[0])

    def test_claim_windows_do_not_overlap_refund_window(self):
        for policy in ClaimPolicy:
            model = Model(policy)
            self.assertFalse(model.can_claim(State(qort="escrow", phase=2, ready=True)))
            self.assertTrue(model.can_claim(State(qort="escrow", phase=0, ready=True)))

    def test_search_fails_explicitly_if_exploration_is_truncated(self):
        with self.assertRaisesRegex(RuntimeError, "bound exceeded"):
            Model(ClaimPolicy.REQUIRE_READY).find(lambda s: False, max_states=1)

    def test_all_reported_counterexamples_replay_through_transition_rules(self):
        for item in counterexamples():
            model = Model(ClaimPolicy(item["policy"]), item["case"] != "hash_preimage_not_bound_to_spend_share")
            current = State()
            for event in item["trace"]:
                transitions = dict(model.successors(current))
                self.assertIn(event, transitions)
                current = transitions[event]
            self.assertEqual(item["final"], current.__dict__)


if __name__ == "__main__":
    unittest.main()
