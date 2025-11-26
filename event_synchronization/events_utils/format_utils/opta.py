from __future__ import annotations

from typing import TYPE_CHECKING

from event_synchronization.events_utils.players_mapping_manager import SkcPlayersMapping

if TYPE_CHECKING:
    from collections.abc import Iterable

KEY_JNO_XML = 'ShirtNumber'
KEY_JNO_JSON = 'shirtNumber'


class OptaFormatStandardizer:
    def __init__(self, raw_opta_events: object, opta_match_data: object) -> None:
        self.raw_opta_events = raw_opta_events
        self.opta_match_data = opta_match_data

    def get_events_from_xml(self) -> object:
        return self.raw_opta_events.findall('Game/Event')

    def get_events_from_json(self) -> list[dict[str, object]]:
        return [
            {
                'id': event.get('id'),
                'player_id': event.get('playerId'),
                'period_id': event.get('periodId'),
                'timestamp': get_timestamp(event),
                'type_id': event.get('typeId'),
                'x': event.get('x'),
                'y': event.get('y'),
            }
            for event in self.raw_opta_events['liveData']['event']
        ]

    def get_opta_standardized_events(self, fmt: str) -> Iterable[object]:
        if fmt == 'xml':
            return self.get_events_from_xml()
        if fmt == 'json':
            return self.get_events_from_json()
        msg = f"Unsupported format '{fmt}'. Expected 'xml' or 'json'."
        raise ValueError(msg)

    def get_opta_ply_id_to_ply_from_xml(self) -> dict[int, dict[str, int]]:
        return {
            int(player.get('PlayerRef')[1:]): {
                'team_id': int(team_data.get('TeamRef')[1:]),
                KEY_JNO_XML: int(player.get(KEY_JNO_XML)),
            }
            for team_data in self.opta_match_data.findall('SoccerDocument/MatchData/TeamData')
            for player in team_data.findall('PlayerLineUp/MatchPlayer')
        }

    def get_skc_team_id_to_opta_team_id_from_xml(self, match_data: dict) -> dict[int, int]:
        opta_team_type_to_opta_team_id = {
            team_data.get('Side').lower(): int(team_data.get('TeamRef')[1:])
            for team_data in self.opta_match_data.findall('SoccerDocument/MatchData/TeamData')
        }
        return {
            match_data['home_team']['id']: opta_team_type_to_opta_team_id['home'],
            match_data['away_team']['id']: opta_team_type_to_opta_team_id['away'],
        }

    def get_opta_id_to_skc_id_info_from_xml(self, match_data: dict) -> tuple[dict[int, int], dict[int, int]]:
        opta_ply_id_to_ply = self.get_opta_ply_id_to_ply_from_xml()
        skc_team_id_to_opta_team_id = self.get_skc_team_id_to_opta_team_id_from_xml(match_data)
        opta_team_id_to_skc_team_id = {v: k for k, v in skc_team_id_to_opta_team_id.items()}
        opta_ply_id_to_skc_ply_id = SkcPlayersMapping(
            match_data
        ).get_provider_ply_id_to_skc_ply_id_with_known_team_id_mapping(
            opta_ply_id_to_ply, skc_team_id_to_opta_team_id, key_jno=KEY_JNO_XML
        )
        return opta_team_id_to_skc_team_id, opta_ply_id_to_skc_ply_id

    def get_opta_ply_id_to_ply_from_json(self) -> dict[int, dict[str, object]]:
        return {
            player_info['playerId']: {**player_info, 'team_id': team_info['contestantId']}
            for team_info in self.opta_match_data['liveData']['lineUp']
            for player_info in team_info['player']
        }

    def get_skc_team_id_to_opta_team_id_from_json(self, match_data: dict) -> dict[int, int]:
        opta_team_type_to_opta_team_id = {
            team_info['position']: team_info for team_info in self.opta_match_data['matchInfo']['contestant']
        }
        return {
            match_data[f'{team_type}_team']['id']: opta_team_type_to_opta_team_id[team_type]['id']
            for team_type in ['home', 'away']
        }

    def get_opta_id_to_skc_id_info_from_json(self, match_data: dict) -> tuple[dict[int, int], dict[int, int]]:
        opta_ply_id_to_ply = self.get_opta_ply_id_to_ply_from_json()
        skc_team_id_to_opta_team_id = self.get_skc_team_id_to_opta_team_id_from_json(match_data)
        opta_team_id_to_skc_team_id = {v: k for k, v in skc_team_id_to_opta_team_id.items()}
        opta_ply_id_to_skc_ply_id = SkcPlayersMapping(
            match_data
        ).get_provider_ply_id_to_skc_ply_id_with_known_team_id_mapping(
            opta_ply_id_to_ply, skc_team_id_to_opta_team_id, key_jno=KEY_JNO_JSON
        )
        return opta_team_id_to_skc_team_id, opta_ply_id_to_skc_ply_id

    def get_opta_standardized_match_data(self, match_data: dict, fmt: str) -> tuple[dict[int, int], dict[int, int]]:
        if fmt == 'xml':
            return self.get_opta_id_to_skc_id_info_from_xml(match_data)
        if fmt == 'json':
            return self.get_opta_id_to_skc_id_info_from_json(match_data)
        msg = f"Unsupported format '{fmt}'. Expected 'xml' or 'json'."
        raise ValueError(msg)


def get_timestamp(event: dict) -> str:
    timestamp = event.get('timeStamp')[:-1] if event.get('timeStamp')[-1] == 'Z' else event.get('timeStamp')
    return timestamp if '.' in timestamp else timestamp + '.000'
