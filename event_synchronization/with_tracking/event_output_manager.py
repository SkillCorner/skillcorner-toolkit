import copy
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from event_synchronization.constants import IDX_ACC, IDX_SPEED, IDX_VX, IDX_VY
from event_synchronization.events_utils.event import Event
from event_synchronization.with_tracking.formatted_data_manager import FormattedTrackingManager
from event_synchronization.with_tracking.utils import AttackingSideManager

NAN_TO_NUM = -9999.0
COLS_LIST = [
    'nb_events',
    'is_matched',
    '%_is_matched',
    'is_matched_is_player_detected',
    'is_not_matched',
    'is_not_matched_is_player_detected',
    'is_not_matched_has_provider_player_id',
    'is_not_matched_frame_tracking_data_available',
    'is_matched_applicable',
]
LIST_SPEED_ACC_INFO = [(IDX_SPEED, 'speed_norm'), (IDX_VX, 'vx'), (IDX_VY, 'vy'), (IDX_ACC, 'acc_norm')]


class EventsOutputManager:
    """Events output manager
    A manager class to generate reports and freeze frame format mixing tracking data and events info

    Attributes:
    -----------
        events (List[Event]): events
        formatted_data_manager (FormattedTrackingManager): formatted data manager
    """

    def __init__(self, events: List[Event], formatted_data_manager: FormattedTrackingManager) -> None:
        self.events = events
        self.formatted_data_manager = formatted_data_manager
        self.tracking_data = formatted_data_manager.tracking_data

        self.ply_id_to_ply = {ply['id']: ply for ply in formatted_data_manager.match_data['players']}
        self.team_id_to_team = get_team_id_to_team(formatted_data_manager.match_data)

        # attacking side manager to invert x, y (provider event_xy) if attacking side is right_to_left
        self.attacking_side_manager = AttackingSideManager(self.formatted_data_manager)

        # update tracking by adding speed, acc info
        self.add_speed_acc_in_tracking_data()

    def add_speed_acc_in_tracking_data(self) -> None:
        """
        Add speed, acc info in tracking data
        """

        # Convert nan to specific value (number not in data) to speed up the process
        self.formatted_data_manager.ply_formatted_data_speed_acc = np.nan_to_num(
            self.formatted_data_manager.ply_formatted_data_speed_acc, nan=NAN_TO_NUM
        )

        for data in self.tracking_data:
            # add speed, acc info by player
            for player_data in data['player_data']:
                ply_idx = self.formatted_data_manager.ply_id_to_idx[player_data['player_id']]
                ply_speed_acc_data = self.formatted_data_manager.ply_formatted_data_speed_acc[data['frame'], ply_idx]
                player_data.update(
                    {
                        keyname: None if value == NAN_TO_NUM else value
                        for idx, keyname in LIST_SPEED_ACC_INFO
                        for value in [ply_speed_acc_data[idx]]
                    }
                )

    def get_freeze_frame_format(self, event: Event, event_provider: str) -> Dict:
        """Get freeze frame format
        For each event, get the freeze frame format that contains the tracking data for all players at the event frame

        Args:
            event (Event): event
            event_provider (str): event provider

        Returns:
            freeze_frame_format (dict): freeze frame format
        """

        frame = int(event.skc_frame)
        if frame >= 0 and frame < len(self.tracking_data):
            self.tracking_data[frame][f'{event_provider}_event_id'] = event.id
            self.tracking_data[frame][f'{event_provider}_event_type'] = event.event_type_name
            self.tracking_data[frame]['player_id'] = event.player_id
            self.tracking_data[frame][f'{event_provider}_player_id'] = event.provider_player_id
            self.tracking_data[frame]['team_id'] = self.ply_id_to_ply.get(event.player_id, {}).get('team_id', None)
            self.tracking_data[frame][f'{event_provider}_team_id'] = event.provider_team_id
            self.tracking_data[frame]['is_matched'] = event.is_matched
            self.tracking_data[frame]['is_player_detected'] = bool(event.is_player_detected)
            self.tracking_data[frame][f'projected_{event_provider}_event_x'] = self.attacking_side_manager.get_event_x(
                event
            )
            self.tracking_data[frame][f'projected_{event_provider}_event_y'] = self.attacking_side_manager.get_event_y(
                event
            )
            return self.tracking_data[frame]
        return None

    def get_event_info(self, event: Event, event_provider: str) -> Dict:
        """Get event info
        Store event info in a dictionary (player and tracking metadata)

        Args:
            event (Event): event
            event_provider (str): event provider

        Returns:
            event_info (dict): event info
        """

        player_id_info = self.ply_id_to_ply.get(event.player_id, {})
        team_id = player_id_info.get('team_id', None)
        team_id_info = self.team_id_to_team.get(team_id, {})
        return {
            f'{event_provider}_event_id': event.id,
            f'{event_provider}_event_type': event.event_type_name,
            'period': event.period,
            'frame': event.skc_frame,
            'player_id': event.player_id,
            f'{event_provider}_player_id': event.provider_player_id,
            'player_name': player_id_info.get('short_name', None),
            'player_role': player_id_info.get('player_role', {}).get('acronym', None),
            'player_number': player_id_info.get('number', None),
            'starting': player_id_info.get('start_time', None) == '00:00:00',
            'team_id': team_id,
            f'{event_provider}_team_id': event.provider_team_id,
            'team_type': team_id_info.get('team_type', None),
            'team_name': team_id_info.get('name', None),
            'is_matched': event.is_matched,
            'is_player_detected': event.is_player_detected,
            f'has_{event_provider}_player_id_attached': event.has_provider_player_id,
            'frame_tracking_data_available': event.frame_tracking_data_available,
            'is_matched_applicable': event.is_matched_applicable,
        }

    def get_report_by_event_type(self, report_by_event: pd.DataFrame, event_provider: str) -> pd.DataFrame:
        """Get report by event type

        Args:
            report_by_event (pd.DataFrame): report_by_event
            event_provider (str): event provider

        Returns:
            report_by_event_type (pd.DataFrame): report by event type
        """

        report_by_event = report_by_event.copy()
        report_by_event['nb_events'] = 1
        report_by_event['is_matched_is_player_detected'] = (
            report_by_event['is_matched'].values & report_by_event['is_player_detected'].values
        )

        report_by_event['is_not_matched'] = ~report_by_event['is_matched']
        report_by_event['is_not_matched_is_player_detected'] = (
            ~report_by_event['is_matched'] & report_by_event['is_player_detected']
        )
        report_by_event['is_not_matched_frame_tracking_data_available'] = (
            ~report_by_event['is_matched'] & report_by_event['frame_tracking_data_available']
        )
        report_by_event[f'is_not_matched_has_{event_provider}_player_id_attached'] = (
            ~report_by_event['is_matched'] & report_by_event[f'has_{event_provider}_player_id_attached']
        )

        # group by event_type - avoid warning .sum(numeric_only=True)
        report_by_event_type = report_by_event.groupby([f'{event_provider}_event_type'], as_index=False).sum(
            numeric_only=True
        )
        report_by_event_type['%_is_matched'] = round(
            report_by_event_type['is_matched'] / report_by_event_type['nb_events'] * 100, 1
        )
        report_by_event_type['is_matched_applicable'] = report_by_event_type['is_matched_applicable'].astype(bool)

        # get the event_provider for player_id_attached
        COLS_LIST[6] = f'is_not_matched_has_{event_provider}_player_id_attached'
        return report_by_event_type[[f'{event_provider}_event_type', *COLS_LIST]]

    def get_reports_and_freeze_frame_format(self, event_provider: str) -> Tuple:
        """Get reports and freeze frame format

        Args:
            event_provider (str): event provider

        Returns:
            event_report_df (pd.DataFrame): event report dataframe
            report_by_event_type (pd.DataFrame): report by event type
            freeze_frame_format_list (List[Dict]): freeze frame format list
        """

        freeze_frame_format_list = []
        event_report_list = []

        for event in self.events:
            freeze_frame_format = self.get_freeze_frame_format(event, event_provider)
            if freeze_frame_format is not None:
                freeze_frame_format_list.append(copy.deepcopy(freeze_frame_format))
            event_report_list.append(self.get_event_info(event, event_provider))

        report_by_event = pd.DataFrame(event_report_list)
        report_by_event_type = self.get_report_by_event_type(report_by_event, event_provider)
        return report_by_event, report_by_event_type, freeze_frame_format_list


def get_team_id_to_team(match_data: Dict) -> Dict:
    """Get team id to team info dictionary

    Args:
        match_data (dict): match data
    """

    team_id_to_team = {}
    for team_type in ['home_team', 'away_team']:
        match_data[team_type]['team_type'] = team_type
        team_id_to_team[match_data[team_type]['id']] = match_data[team_type]
    return team_id_to_team
