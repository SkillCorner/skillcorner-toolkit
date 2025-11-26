from __future__ import annotations

from datetime import datetime, timezone

import numpy as np

from event_synchronization.constants import GENERIC_EVENT, PASS_EVENT, SHOT_EVENT
from event_synchronization.events_utils.event import Event

PASS_PRIMARY_TYPES = ['pass']
SHOT_PRIMARY_TYPES = ['shot']
FIRST_TOUCH_PRIMARY_TYPES = ['clearance', 'interception', 'touch']
FIRST_TOUCH_PRIMARY_AND_SECONDARY_TYPES = {'duel': ['sliding_tackle'], 'shot_against': ['save']}
PERIOD_START_MINUTE = {
    1: 0,
    2: 45,
    3: 90,
    4: 105,
    5: 120,
}
MAPPING_PERIOD_NAME = {'1H': 1, '2H': 2, '1E': 3, '2E': 4}
TH_KICK_OFF_DIST = 9.15 / 2
NO_IS_MATCHED_APPLICABLE_LIST = ['game_interruption']
OFFSET_REFINE = 10  # offset to refine for events to refine (in frame)


EXPECTED_WYSCOUT_TEAMS = 2


class WyscoutEvents:
    def __init__(
        self,
        raw_wyscout_events: dict,
        match_data: dict,
    ) -> None:
        self.raw_events = raw_wyscout_events['events']
        self.match_data = match_data
        self.wyscout_id_to_skc_id = {int(ply['wyscout_id']): ply['id'] for ply in match_data['players']}
        self.wyscout_team_id_to_skc_team_id = get_wyscout_team_id_to_skc_team_id(match_data, self.raw_events)
        self.event_provider = 'wyscout'

    def wyscout_player_id(self, raw_event: dict) -> int:
        """Get wyscout player id

        Args:
            event: raw impect event

        Returns:
            wyscout_player_id (int): wyscout player id
        """

        return int(raw_event['player']['id'])

    def get_player_id(self, wyscout_player_id: int) -> int | None:
        """Get SKC player id

        Args:
            event: raw impect event

        Returns:
            player_id (int): player id
        """

        return self.wyscout_id_to_skc_id.get(wyscout_player_id)

    def get_wyscout_team_id(self, raw_event: dict) -> int | None:
        """Get wyscout team id

        Args:
            event: raw wyscout event

        Returns:
            team_id (int): team id
        """

        team = raw_event.get('team') or {}
        wyscout_team_id = team.get('id')
        return int(wyscout_team_id) if wyscout_team_id is not None else None

    def get_team_id(self, wyscout_team_id: int | None) -> int | None:
        """Get SKC team id

        Args:
            event: raw wyscout event

        Returns:
            team_id (int): team id
        """

        if wyscout_team_id is None:
            return None
        return self.wyscout_team_id_to_skc_team_id.get(wyscout_team_id)

    def get_timestamp(
        self,
        raw_event: dict,
        offset_period: float,
        period: int,
        use_match_timestamp: bool,
    ) -> float:
        """Get event timestamp

        Args:
            event: raw wyscout event

        Returns:
            timestamp (float): timestamp of the event
        """

        if use_match_timestamp:
            event_datetime = datetime.strptime(  # type: ignore[arg-type]
                raw_event['matchTimestamp'], '%H:%M:%S.%f'
            ).replace(tzinfo=timezone.utc)
            return (
                event_datetime.hour * 3600
                + event_datetime.minute * 60
                + event_datetime.second
                + event_datetime.microsecond / 10**6
                - PERIOD_START_MINUTE[period] * 60
            ) - offset_period
        return float(raw_event['videoTimestamp']) - offset_period

    def get_event_location(self, raw_event: dict) -> tuple[float, float] | tuple[str, str]:
        """Get event location

        Args:
            event: raw wyscout event

        Returns:
            x_event (float): x coordinate of the event in SKC coordinates system
            y_event (float): y coordinate of the event in SKC coordinates system
        """

        location = raw_event.get('location')

        if location is None:
            return 'unknown', 'unknown'

        x_event = location.get('x')
        y_event = location.get('y')

        if x_event is None or y_event is None:
            return 'unknown', 'unknown'

        x_event = (float(x_event) - 50) * self.match_data['pitch_length'] / 100
        y_event = -(float(y_event) - 50) * self.match_data['pitch_width'] / 100
        return x_event, y_event

    def get_generic_event_type(self, raw_event: dict) -> str:
        """Get generic event type

        Args:
            event: raw impect event

        Returns:
            event_type (str): event type
        """

        # get event type
        if raw_event['type']['primary'] in PASS_PRIMARY_TYPES or is_potential_pass(raw_event):
            return PASS_EVENT
        if raw_event['type']['primary'] in SHOT_PRIMARY_TYPES:
            return SHOT_EVENT
        return GENERIC_EVENT

    def get_is_first_touch_event(self, raw_event: dict) -> bool:
        """Get whether the event is a first touch event or not
        Args:
            raw_event: raw wyscout event

        Returns:
            is_first_touch_event (bool): whether the event is a first touch event or not
        """

        primary_type = raw_event['type']['primary']
        secondary_types = raw_event['type']['secondary']
        is_primary = primary_type in FIRST_TOUCH_PRIMARY_TYPES
        is_primary_and_secondary = primary_type in FIRST_TOUCH_PRIMARY_AND_SECONDARY_TYPES and bool(
            set(FIRST_TOUCH_PRIMARY_AND_SECONDARY_TYPES[primary_type]).intersection(secondary_types)
        )
        return is_primary or is_primary_and_secondary

    def standardize_events(self) -> list[Event]:
        """Convert raw wyscout events to standardized events

        Returns:
            standardized_events (list[event_synchronization.events_utils.events.Event])
        """

        offsets_periods, use_match_timestamp = get_offsets_periods(self.raw_events)

        standardized_events = []
        for raw_event in self.raw_events:
            # ignore penalty shootout events
            if raw_event['matchPeriod'][0] == 'P':
                continue

            # wyscout player and team id
            wyscout_player_id = self.wyscout_player_id(raw_event)
            wyscout_team_id = self.get_wyscout_team_id(raw_event)

            # get period
            period = MAPPING_PERIOD_NAME[raw_event['matchPeriod']]

            # get event location
            x_event, y_event = self.get_event_location(raw_event)

            # get standardized event
            standardized_event = Event(
                event_id=raw_event['id'],
                period=period,
                timestamp=self.get_timestamp(raw_event, offsets_periods[period], period, use_match_timestamp),
                generic_event_type=self.get_generic_event_type(raw_event),
                player_id=self.get_player_id(wyscout_player_id),
                provider_player_id=wyscout_player_id,
                team_id=self.get_team_id(wyscout_team_id),
                provider_team_id=wyscout_team_id,
                x=x_event,
                y=y_event,
                touch_type='first' if self.get_is_first_touch_event(raw_event) else 'last',
                event_type_name=raw_event['type']['primary'],
                to_refine=(self.get_generic_event_type(raw_event) in [PASS_EVENT, SHOT_EVENT]),
                is_matched_applicable=raw_event['type']['primary'] not in NO_IS_MATCHED_APPLICABLE_LIST,
            )
            standardized_event.offset_refine = get_offset_refine(standardized_event)

            standardized_events.append(standardized_event)
        return standardized_events


def get_wyscout_team_id_to_skc_team_id(match_data: dict, raw_events: dict) -> dict[int, int]:
    wyscout_ply_id_to_ply = {int(ply['wyscout_id']): ply for ply in match_data['players']}
    wyscout_team_id_to_skc_team_id: dict[int, int] = {}
    for raw_event in raw_events:
        team = raw_event.get('team') or {}
        wyscout_team_id = team.get('id')
        if wyscout_team_id is None:
            continue

        player = raw_event.get('player') or {}
        wyscout_player_id = player.get('id')
        if wyscout_player_id is None:
            continue

        if wyscout_player_id not in wyscout_ply_id_to_ply:
            continue

        wyscout_team_id_to_skc_team_id[int(wyscout_team_id)] = wyscout_ply_id_to_ply[int(wyscout_player_id)]['team_id']
        if len(wyscout_team_id_to_skc_team_id) == EXPECTED_WYSCOUT_TEAMS:
            break

    return wyscout_team_id_to_skc_team_id


def video_match_timestamp_info(raw_events: dict) -> tuple[list[float], bool]:
    video_timestamp_list = []
    use_match_timestamp = True
    for raw_event in raw_events:
        video_timestamp_list.append(float(raw_event['videoTimestamp']))
        if '-' in raw_event['matchTimestamp']:
            use_match_timestamp = False
    return video_timestamp_list, use_match_timestamp


def timestamp_for_period(raw_event: dict, period: int, use_match_timestamp: bool) -> float:
    if use_match_timestamp:
        event_datetime = datetime.strptime(  # type: ignore[arg-type]
            raw_event['matchTimestamp'], '%H:%M:%S.%f'
        ).replace(tzinfo=timezone.utc)
        return (
            event_datetime.hour * 3600
            + event_datetime.minute * 60
            + event_datetime.second
            + event_datetime.microsecond / 10**6
            - PERIOD_START_MINUTE[period] * 60
        )
    return float(raw_event['videoTimestamp'])


def get_offsets_periods(raw_events: dict) -> tuple[dict[int, float], bool]:
    offset_period: dict[int, float] = {}
    video_timestamp_list, use_match_timestamp = video_match_timestamp_info(raw_events)
    idx_order = np.argsort(video_timestamp_list)
    sorted_raw_events = np.array(list(raw_events))[idx_order]
    for period in [1, 2, 3, 4]:
        period_kick_off = True
        for sorted_raw_event in sorted_raw_events:
            if period == MAPPING_PERIOD_NAME[sorted_raw_event['matchPeriod']] and period_kick_off:
                offset_period[period] = timestamp_for_period(sorted_raw_event, period, use_match_timestamp)
                period_kick_off = False
    return offset_period, use_match_timestamp


def is_potential_pass(raw_event: dict) -> bool:
    pass_in_secondary = 'pass' in raw_event['type']['secondary'] or 'head_pass' in raw_event['type']['secondary']
    return raw_event['type']['primary'] == 'interception' and pass_in_secondary


def get_offset_refine(standardized_event: Event) -> int | None:
    return OFFSET_REFINE if standardized_event.to_refine else None
