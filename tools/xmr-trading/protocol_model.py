#!/usr/bin/env python3
"""Finite, non-cryptographic counterexample model for candidate QORT/XMR trades.

No wallet, network, signing or broadcasting code. This is deliberately NOT an AT
implementation or a proof of atomicity. See README.md for its abstractions.
"""
from collections import deque
from dataclasses import asdict, dataclass, replace
from enum import Enum
import json
from typing import Callable, Iterator, Optional, Tuple


class ClaimPolicy(str, Enum):
    REQUIRE_READY = "require-ready"
    AFTER_FIRST_TIMEOUT = "after-first-timeout"


@dataclass(frozen=True)
class State:
    # QORT: seller -> escrow -> buyer OR returned. XMR: buyer -> joint -> seller OR returned.
    qort: str = "seller"
    xmr: str = "buyer"
    phase: int = 0  # before first timeout, claim window, refund window
    ready: bool = False
    seller_online: bool = True
    seller_share_public: bool = False
    buyer_share_public: bool = False
    claim_pending: bool = False


@dataclass(frozen=True)
class Model:
    claim_policy: ClaimPolicy
    claim_bound_to_spend_share: bool = True

    def can_claim(self, state: State) -> bool:
        return state.qort == "escrow" and state.phase < 2 and (
            state.ready or (
                self.claim_policy == ClaimPolicy.AFTER_FIRST_TIMEOUT and state.phase == 1
            )
        )

    def successors(self, s: State) -> Iterator[Tuple[str, State]]:
        if s.qort == "seller" and s.phase == 0 and s.seller_online:
            yield "seller_locks_qort", replace(s, qort="escrow")
        if s.seller_online:
            yield "seller_disappears", replace(s, seller_online=False)
        if s.qort == "escrow" and s.xmr == "buyer" and s.phase == 0:
            yield "buyer_locks_xmr", replace(s, xmr="joint")
        if s.seller_online and s.qort == "escrow" and s.xmr == "joint" and s.phase == 0 and not s.ready:
            yield "seller_marks_ready", replace(s, ready=True)
        if s.qort == "escrow" and s.phase == 0 and not s.ready and s.seller_online:
            yield "seller_aborts_early", replace(s, qort="returned", seller_share_public=True)
        if s.phase < 2:
            yield "first_timeout" if s.phase == 0 else "second_timeout", replace(s, phase=s.phase + 1)
        if self.can_claim(s) and not s.claim_pending:
            # A published witness is public even if its transaction is never confirmed.
            yield "buyer_publishes_claim", replace(s, claim_pending=True,
                buyer_share_public=self.claim_bound_to_spend_share)
        if s.claim_pending and self.can_claim(s):
            yield "claim_confirms", replace(s, qort="buyer", claim_pending=False)
        if s.qort == "escrow" and s.phase == 2:
            # Autonomous QORT refund cannot invent a private spend share.
            yield "at_refunds_qort", replace(s, qort="returned", claim_pending=False)
        if s.qort == "returned" and s.seller_online and not s.seller_share_public:
            yield "seller_releases_refund_share", replace(s, seller_share_public=True)
        if s.xmr == "joint" and s.buyer_share_public and s.seller_online:
            yield "seller_sweeps_xmr", replace(s, xmr="seller")
        if s.xmr == "joint" and s.seller_share_public:
            yield "buyer_recovers_xmr", replace(s, xmr="returned")

    def find(self, predicate: Callable[[State], bool], max_states: int = 10000) -> Optional[Tuple[Tuple[str, ...], State, int]]:
        """Breadth-first shortest trace, with an explicit finite exploration bound."""
        start = State()
        queue = deque([(start, ())])
        seen = {start}
        while queue:
            state, trace = queue.popleft()
            if predicate(state):
                return trace, state, len(seen)
            for event, child in self.successors(state):
                if child in seen:
                    continue
                if len(seen) >= max_states:
                    raise RuntimeError("Model exploration bound exceeded")
                seen.add(child)
                queue.append((child, trace + (event,)))
        return None


def stranded_xmr(s: State) -> bool:
    return (s.qort == "returned" and s.xmr == "joint" and not s.seller_online
            and not s.seller_share_public and not s.buyer_share_public)


def unfunded_qort_claim(s: State) -> bool:
    return s.qort == "buyer" and s.xmr == "buyer" and not s.seller_online


def seller_gets_both(s: State) -> bool:
    return s.qort == "returned" and s.xmr == "seller"


def unbound_claim(s: State) -> bool:
    return s.qort == "buyer" and s.xmr == "joint" and not s.buyer_share_public


def counterexamples():
    cases = [
        ("readiness_without_unilateral_xmr_recovery", Model(ClaimPolicy.REQUIRE_READY), stranded_xmr),
        ("timeout_claim_without_xmr_funding", Model(ClaimPolicy.AFTER_FIRST_TIMEOUT), unfunded_qort_claim),
        ("unconfirmed_witness_survives_refund", Model(ClaimPolicy.REQUIRE_READY), seller_gets_both),
        ("hash_preimage_not_bound_to_spend_share", Model(ClaimPolicy.REQUIRE_READY, False), unbound_claim),
    ]
    result = []
    for name, model, predicate in cases:
        found = model.find(predicate)
        if found is None:
            raise RuntimeError("Expected rejected-candidate counterexample not found: " + name)
        trace, state, count = found
        result.append({"case": name, "policy": model.claim_policy.value,
            "trace": trace, "final": asdict(state), "states_discovered": count})
    return result


if __name__ == "__main__":
    print(json.dumps({"status": "rejected candidate counterexamples, NOT a safe protocol",
        "counterexamples": counterexamples()}, indent=2))
