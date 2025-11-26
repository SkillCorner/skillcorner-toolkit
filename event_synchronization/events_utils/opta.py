from __future__ import annotations

from datetime import datetime, timezone

from event_synchronization.constants import GENERIC_EVENT, PASS_EVENT, SHOT_EVENT
from event_synchronization.events_utils.event import Event
from event_synchronization.events_utils.format_utils.opta import OptaFormatStandardizer

START_PERIOD_TYPE_ID = 32
TEAM_SET_UP_ID = 34
PENALTY_SHOOTOUT_PERIOD_ID = 14
PASS_TYPE_IDS = [1, 2]
SHOT_TYPE_IDS = [13, 14, 15, 16]
FIRST_TOUCH_TYPE_IDS = [7, 8, 10, 12, 49, 52, 59]
OFFSET_REFINE = 10  # offset to refine for events to refine (in frame)
POSSIBLE_PERIOD_ID = [1, 2, 3, 4]

MAPPING_EVENTS = {
    '1': 'Pass',
    '2': 'Offside Pass',
    '3': 'Take On',
    '4': 'Foul',
    '5': 'Out',
    '6': 'Corner Awarded',
    '7': 'Tackle',
    '8': 'Interception',
    '10': 'Save Goalkeeper',
    '11': 'Claim Goalkeeper',
    '12': 'Clearance',
    '13': 'Miss',
    '14': 'Post',
    '15': 'Attempt Saved',
    '16': 'Goal',
    '17': 'Card Bookings',
    '18': 'Player off',
    '19': 'Player on',
    '20': 'Player retired',
    '21': 'Player returns',
    '22': 'Player becomes goalkeeper',
    '23': 'Goalkeeper becomes player',
    '24': 'Condition change',
    '25': 'Official change',
    '27': 'Start delay',
    '28': 'End delay',
    '30': 'End',
    '32': 'Start',
    '34': 'Team set up',
    '35': 'Player changed position',
    '36': 'Player changed Jersey',
    '37': 'Collection End',
    '38': 'Temp_Goal',
    '39': 'Temp_Attempt',
    '40': 'Formation change',
    '41': 'Punch',
    '42': 'Good Skill',
    '43': 'Deleted event',
    '44': 'Aerial',
    '45': 'Challenge',
    '47': 'Rescinded card',
    '49': 'Ball recovery',
    '50': 'Dispossessed',
    '51': 'Error',
    '52': 'Keeper pick-up',
    '53': 'Cross not claimed',
    '54': 'Smother',
    '55': 'Offside provoked',
    '56': 'Shield ball opp',
    '57': 'Foul throw-in',
    '58': 'Penalty faced',
    '59': 'Keeper Sweeper',
    '60': 'Chance missed',
    '61': 'Ball touch',
    '63': 'Temp_Save',
    '64': 'Resume',
    '65': 'Contentious referee decision',
}
NO_IS_MATCHED_APPLICABLE_LIST = [
    'Start',
    'Start delay',
    'End delay',
    'End',
    'Team set up',
    'Formation change',
    'Deleted event',
    'Player off',
    'Player on',
    'Player changed position',
    'Player changed Jersey',
    'Player retired',
    'Player returns',
    'Player becomes goalkeeper',
    'Goalkeeper becomes player',
    'Official change',
    'Condition change',
    'Collection End',
    'Temp_Goal',
    'Temp_Attempt',
    'Resume',
    'Contentious referee decision',
    'Card Bookings',
]


class OptaEvents:
    def __init__(
        self,
        raw_opta_events: dict,
        opta_match_data: dict,
        match_data: dict,
        fmt: str,
    ) -> None:
        opta_format_standardizer = OptaFormatStandardizer(raw_opta_events, opta_match_data)
        self.raw_events = opta_format_standardizer.get_opta_standardized_events(fmt)
        self.opta_match_data = opta_match_data
        self.match_data = match_data
        self.skc_ply_id_to_skc_ply = {ply['id']: ply for ply in match_data['players']}

        opta_team_id_to_skc_team_id, opta_ply_id_to_skc_ply_id = (
            opta_format_standardizer.get_opta_standardized_match_data(match_data, fmt)
        )
        self.opta_team_id_to_skc_team_id, self.opta_ply_id_to_skc_ply_id = process_opta_dict_mapping(
            opta_team_id_to_skc_team_id, opta_ply_id_to_skc_ply_id
        )
        self.skc_team_id_to_opta_team_id = {v: k for k, v in self.opta_team_id_to_skc_team_id.items()}
        self.period_starts_datetimes = self.get_period_starts_datetimes()
        self.event_provider = 'opta'

    def get_opta_player_id(self, raw_event: dict) -> int | None:
        """Get opta player id

        Args:
            event: raw opta event

        Returns:
            opta_player_id (int): opta player id
        """

        return raw_event.get('player_id')

    def get_player_id(self, opta_player_id: int | None) -> int | None:
        """Get SKC player id

        Args:
            opta_player_id: opta player id

        Returns:
            player_id (int): player id
        """

        return self.opta_ply_id_to_skc_ply_id.get(opta_player_id, None)

    def get_opta_team_id(self, raw_event: dict) -> int | None:
        """Get opta team id

        Args:
            event: raw opta event

        Returns:
            opta_team_id (int): opta team id
        """

        opta_player_id = self.get_opta_player_id(raw_event)
        opta_team_id = raw_event.get('team_id')
        if opta_team_id is None:
            skc_ply_id = self.get_player_id(opta_player_id)
            skc_team_id = self.skc_ply_id_to_skc_ply.get(skc_ply_id, {}).get('team_id', None)
            return self.skc_team_id_to_opta_team_id.get(skc_team_id, None)
        return opta_team_id

    def get_team_id(self, opta_team_id: int | None) -> int | None:
        """Get SKC team id

        Args:
            opta_team_id: opta team id

        Returns:
            team_id (int): team id
        """

        return self.opta_team_id_to_skc_team_id.get(opta_team_id, None)

    def get_period_starts_datetimes(self) -> dict[int, datetime]:
        """Get period starts datetimes

        Returns:
            period_starts_datetimes (dict): period starts datetimes
        """

        return {
            int(event.get('period_id')): datetime.strptime(event.get('timestamp'), '%Y-%m-%dT%H:%M:%S.%f').replace(
                tzinfo=timezone.utc
            )
            for event in filter(lambda e: int(e.get('type_id')) == START_PERIOD_TYPE_ID, self.raw_events)
        }

    def get_timestamp(self, raw_event: dict) -> float:
        """Get event timestamp

        Args:
            event: raw opta event

        Returns:
            timestamp (float): timestamp of the event
        """

        period = int(raw_event.get('period_id'))
        event_datetime = datetime.strptime(raw_event.get('timestamp'), '%Y-%m-%dT%H:%M:%S.%f').replace(
            tzinfo=timezone.utc
        )
        return event_datetime.timestamp() - self.period_starts_datetimes[period].timestamp()

    def get_event_location(self, raw_event: dict) -> tuple[float, float] | tuple[str, str]:
        """Get event location

        Args:
            event: raw opta event

        Returns:
            x_event (float): x coordinate of the event in SKC coordinates system
            y_event (float): y coordinate of the event in SKC coordinates system
        """

        x_event = raw_event.get('x')
        y_event = raw_event.get('y')

        if x_event is None or y_event is None:
            return 'unknown', 'unknown'

        x_event, y_event = float(x_event), float(y_event)
        x_event = (x_event - 50) * self.match_data['pitch_length'] / 100
        y_event = (y_event - 50) * self.match_data['pitch_width'] / 100
        return x_event, y_event

    def get_generic_event_type(self, raw_event: dict) -> str:
        """Get generic event type

        Args:
            event: raw opta event

        Returns:
            event_type (str): event type
        """

        type_id = int(raw_event.get('type_id'))
        if type_id in PASS_TYPE_IDS:
            return PASS_EVENT
        if type_id in SHOT_TYPE_IDS:
            return SHOT_EVENT
        return GENERIC_EVENT

    def get_event_type_name(self, raw_event: dict) -> str:
        """Get event type name

        Args:
            event: raw opta event

        Returns:
            event_type_name (str): event type name
        """

        return MAPPING_EVENTS.get(str(raw_event.get('type_id')), 'unknown')

    def get_is_first_touch_event(self, raw_event: dict) -> str:
        """Get whether the event is a first touch event or not
        Args:
            raw_event: raw wyscout event

        Returns:
            is_first_touch_event (bool): whether the event is a first touch event or not
        """

        return 'first' if int(raw_event.get('type_id')) in FIRST_TOUCH_TYPE_IDS else 'last'

    def standardize_events(self) -> list[Event]:
        """Convert raw opta events to standardized events

        Returns:
            standardized_events (list[event_synchronization.events_utils.events.Event])
        """

        standardized_events = []
        for raw_event in self.raw_events:
            # ignore TEAM_SET_UP events
            if (
                int(raw_event.get('type_id')) == TEAM_SET_UP_ID
                or int(raw_event.get('period_id')) == PENALTY_SHOOTOUT_PERIOD_ID
                or int(raw_event.get('period_id')) not in POSSIBLE_PERIOD_ID
            ):
                continue

            # opta player and team id
            opta_player_id = self.get_opta_player_id(raw_event)
            opta_team_id = self.get_opta_team_id(raw_event)

            # get event location
            x_event, y_event = self.get_event_location(raw_event)

            # get standardized event
            standardized_event = Event(
                event_id=raw_event.get('id'),
                period=int(raw_event.get('period_id')),
                timestamp=self.get_timestamp(raw_event),
                generic_event_type=self.get_generic_event_type(raw_event),
                player_id=self.get_player_id(opta_player_id),
                provider_player_id=opta_player_id,
                team_id=self.get_team_id(opta_team_id),
                provider_team_id=opta_team_id,
                x=x_event,
                y=y_event,
                event_type_name=self.get_event_type_name(raw_event),
                touch_type=self.get_is_first_touch_event(raw_event),
                to_refine=(self.get_generic_event_type(raw_event) in [PASS_EVENT, SHOT_EVENT]),
                is_matched_applicable=self.get_event_type_name(raw_event) not in NO_IS_MATCHED_APPLICABLE_LIST,
            )
            standardized_event.offset_refine = get_offset_refine(standardized_event)

            standardized_events.append(standardized_event)
        return standardized_events


def process_dict_mapping(mapping_dict: dict) -> dict[str, object]:
    return {str(k): v for k, v in mapping_dict.items()}


def process_opta_dict_mapping(
    opta_team_id_to_skc_team_id: dict,
    opta_ply_id_to_skc_ply_id: dict,
) -> tuple[dict[str, object], dict[str, object]]:
    return process_dict_mapping(opta_team_id_to_skc_team_id), process_dict_mapping(opta_ply_id_to_skc_ply_id)


def get_offset_refine(standardized_event: Event) -> int | None:
    return OFFSET_REFINE if standardized_event.to_refine else None
