from __future__ import annotations

import secrets
from datetime import timedelta
from uuid import uuid4

from common import utc_now

from .models import RelayChallenge, RelayChannel, RelayRegistration


class RelayService:
    """Estado local e efemero dos registros e canais atendidos por este relay."""

    def __init__(self) -> None:
        self.challenge_ttl_seconds = 60
        self.registration_ttl_seconds = 30 * 60
        self.channel_ttl_seconds = 10 * 60
        self._challenges_by_nonce: dict[str, RelayChallenge] = {}
        self._registrations_by_target_id: dict[str, RelayRegistration] = {}
        self._channels_by_id: dict[str, RelayChannel] = {}

    def create_challenge(
        self,
        *,
        target_physical_node_id: str,
        target_session_id: str,
        relay_physical_node_id: str,
    ) -> RelayChallenge:
        self._prune_expired()
        now = utc_now()
        challenge = RelayChallenge(
            target_physical_node_id=target_physical_node_id,
            target_session_id=target_session_id,
            relay_physical_node_id=relay_physical_node_id,
            nonce=secrets.token_hex(32),
            created_at=now,
            expires_at=now + timedelta(seconds=self.challenge_ttl_seconds),
        )
        self._challenges_by_nonce[challenge.nonce] = challenge
        return challenge

    def get_active_challenge(self, nonce: str) -> RelayChallenge | None:
        self._prune_expired()
        return self._challenges_by_nonce.get(nonce)

    def register_target(
        self,
        *,
        target_physical_node_id: str,
        target_public_key: str,
        target_session_id: str,
        challenge_nonce: str,
        signature_hex: str,
    ) -> RelayRegistration:
        self._prune_expired()
        now = utc_now()
        registration = RelayRegistration(
            target_physical_node_id=target_physical_node_id,
            target_public_key=target_public_key,
            target_session_id=target_session_id,
            challenge_nonce=challenge_nonce,
            signature_hex=signature_hex,
            registered_at=now,
            expires_at=now + timedelta(seconds=self.registration_ttl_seconds),
        )
        self._registrations_by_target_id[target_physical_node_id] = registration
        self._challenges_by_nonce.pop(challenge_nonce, None)
        return registration

    def get_active_registration(self, target_physical_node_id: str) -> RelayRegistration | None:
        self._prune_expired()
        return self._registrations_by_target_id.get(target_physical_node_id)

    def create_channel(
        self,
        *,
        target_physical_node_id: str,
        requester_session_id: str,
        target_session_id: str,
    ) -> RelayChannel:
        self._prune_expired()
        now = utc_now()
        channel = RelayChannel(
            relay_channel_id=str(uuid4()),
            target_physical_node_id=target_physical_node_id,
            requester_session_id=requester_session_id,
            target_session_id=target_session_id,
            created_at=now,
            expires_at=now + timedelta(seconds=self.channel_ttl_seconds),
        )
        self._channels_by_id[channel.relay_channel_id] = channel
        return channel

    def get_active_channel(self, relay_channel_id: str) -> RelayChannel | None:
        self._prune_expired()
        return self._channels_by_id.get(relay_channel_id)

    def close_channel(self, relay_channel_id: str) -> RelayChannel | None:
        return self._channels_by_id.pop(relay_channel_id, None)

    def _prune_expired(self) -> None:
        now = utc_now()
        expired_challenges = [
            nonce
            for nonce, challenge in self._challenges_by_nonce.items()
            if challenge.expires_at <= now
        ]
        for nonce in expired_challenges:
            self._challenges_by_nonce.pop(nonce, None)

        expired_registrations = [
            target_id
            for target_id, registration in self._registrations_by_target_id.items()
            if registration.expires_at <= now
        ]
        for target_id in expired_registrations:
            self._registrations_by_target_id.pop(target_id, None)

        expired_channels = [
            channel_id
            for channel_id, channel in self._channels_by_id.items()
            if channel.expires_at <= now
        ]
        for channel_id in expired_channels:
            self._channels_by_id.pop(channel_id, None)
