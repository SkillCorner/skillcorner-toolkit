from __future__ import annotations

from dataclasses import dataclass

EventId = int | str
ProviderId = int | str


@dataclass
class Event:
    id: EventId
    period: int
    timestamp: float
    generic_event_type: str
    player_id: int | None
    provider_player_id: ProviderId | None
    team_id: int | None
    provider_team_id: ProviderId | None
    x: float | str
    y: float | str
    to_refine: bool = False
    is_head: bool = False
    touch_type: str | None = None
    event_type_name: str | None = None
    force_to_refine: bool = False
    is_matched_applicable: bool = True
    offset_refine: int | None = None

    def __init__(  # noqa: PLR0913
        self,
        event_id: EventId,
        period: int,
        timestamp: float,
        generic_event_type: str,
        player_id: int | None,
        provider_player_id: ProviderId | None,
        team_id: int | None,
        provider_team_id: ProviderId | None,
        x: float | str,
        y: float | str,
        to_refine: bool = False,
        is_head: bool = False,
        touch_type: str | None = None,
        event_type_name: str | None = None,
        force_to_refine: bool = False,
        is_matched_applicable: bool = True,
        offset_refine: int | None = None,
    ) -> None:
        self.id = event_id
        self.period = period
        self.timestamp = timestamp
        self.generic_event_type = generic_event_type
        self.player_id = player_id
        self.provider_player_id = provider_player_id
        self.team_id = team_id
        self.provider_team_id = provider_team_id
        self.x = x
        self.y = y
        self.to_refine = to_refine
        self.is_head = is_head
        self.touch_type = touch_type
        self.event_type_name = event_type_name
        self.force_to_refine = force_to_refine
        self.is_matched_applicable = is_matched_applicable
        self.offset_refine = offset_refine
