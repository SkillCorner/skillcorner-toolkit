from typing import Dict, Union

import numpy as np

from event_synchronization.events_utils.event import Event
from event_synchronization.with_tracking.formatted_data_manager import FormattedTrackingManager

LEFT_TO_RIGHT = 'left_to_right'
RIGHT_TO_LEFT = 'right_to_left'
OPP_TEAM_SIDE_MAPPING = {
    LEFT_TO_RIGHT: RIGHT_TO_LEFT,
    RIGHT_TO_LEFT: LEFT_TO_RIGHT,
}


class AttackingSideManager:
    """A manager class responsible for handling the attacking side of each team

    Attributes:
    -----------
        team_id_to_attacking_side (Dict): team_id_to_attacking_side
    """

    def __init__(self, formatted_tracking_manager: FormattedTrackingManager) -> None:
        self.team_id_to_attacking_side = get_team_id_to_attacking_side(formatted_tracking_manager)

    def get_event_x(self, event: Event) -> float:
        """Get the x position of the event

        Args:
            event (Event): event

        Returns:
            float: x position of the event
        """

        if event.x == 'unknown' or event.y == 'unknown':
            return 'unknown'
        if self.team_id_to_attacking_side[event.period].get(event.team_id) == RIGHT_TO_LEFT:
            return round(float(-event.x), 2)
        if self.team_id_to_attacking_side[event.period].get(event.team_id) == LEFT_TO_RIGHT:
            return round(float(event.x), 2)
        return 'unknown'

    def get_event_y(self, event: Event) -> Union[float, str]:
        """Get the y position of the event

        Args:
            event (Event): event

        Returns:
            float: y position of the event
        """

        if event.x == 'unknown' or event.y == 'unknown':
            return 'unknown'
        if self.team_id_to_attacking_side[event.period].get(event.team_id) == RIGHT_TO_LEFT:
            return round(float(-event.y), 2)
        if self.team_id_to_attacking_side[event.period].get(event.team_id) == LEFT_TO_RIGHT:
            return round(float(event.y), 2)
        return 'unknown'


def get_team_id_to_attacking_side_using_match_data(match_data: Dict) -> Dict:
    """Get team_id_to_attacking_side using match data
    The attacking side of each team is defined in the match data

    Args:
        match_data (Dict): match data

    Returns:
        Dict: team_id_to_attacking_side
    """

    home_team_side_list = match_data.get('home_team_side')
    home_team_id = match_data['home_team']['id']
    away_team_id = match_data['away_team']['id']

    team_id_to_attacking_side = {}
    for idx, home_team_side in enumerate(home_team_side_list):
        period = idx + 1
        team_id_to_attacking_side[period] = {
            home_team_id: home_team_side,
            away_team_id: OPP_TEAM_SIDE_MAPPING[home_team_side],
        }
    return team_id_to_attacking_side


def get_team_id_to_attacking_side_using_tracking(formatted_tracking_manager: FormattedTrackingManager) -> Dict:
    """Get team_id_to_attacking_side using tracking data
    Using average x position of each team, determine the attacking side of each team

    Args:
        formatted_tracking_manager (FormattedTrackingManager): formatted tracking manager

    Returns:
        Dict: team_id_to_attacking_side
    """

    home_team_id = formatted_tracking_manager.match_data['home_team']['id']
    away_team_id = formatted_tracking_manager.match_data['away_team']['id']

    team_id_to_attacking_side = {}
    for period, (period_start, period_end) in formatted_tracking_manager.periods_start_end_dict.items():
        home_idx_list = formatted_tracking_manager.team_id_to_idx_list[home_team_id]
        away_idx_list = formatted_tracking_manager.team_id_to_idx_list[away_team_id]

        home_avg_x_pos = np.nanmean(
            formatted_tracking_manager.ply_formatted_data[period_start:period_end, home_idx_list, 0]
        )
        away_avg_x_pos = np.nanmean(
            formatted_tracking_manager.ply_formatted_data[period_start:period_end, away_idx_list, 0]
        )

        home_team_side = LEFT_TO_RIGHT if home_avg_x_pos < away_avg_x_pos else RIGHT_TO_LEFT

        team_id_to_attacking_side[period] = {
            home_team_id: home_team_side,
            away_team_id: OPP_TEAM_SIDE_MAPPING[home_team_side],
        }

    return team_id_to_attacking_side


def get_team_id_to_attacking_side(formatted_tracking_manager: FormattedTrackingManager) -> Dict:
    """
    Get team_id_to_attacking_side

    Args:
        formatted_tracking_manager (FormattedTrackingManager): formatted tracking manager

    Returns:
        Dict: team_id_to_attacking_side
    """

    if formatted_tracking_manager.match_data.get('home_team_side', []):
        return get_team_id_to_attacking_side_using_match_data(formatted_tracking_manager.match_data)
    return get_team_id_to_attacking_side_using_tracking(formatted_tracking_manager)
