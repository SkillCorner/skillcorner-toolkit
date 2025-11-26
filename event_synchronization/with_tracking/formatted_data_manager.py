from collections import defaultdict
from typing import Dict, List, Tuple

import numpy as np

from event_synchronization.constants import (
    FPS,
    IDX_ACC,
    IDX_SPEED,
    IDX_VX,
    IDX_VY,
    IMPOSSIBLE_SPEED_TH,
    SMOOTHING_ACC,
    SMOOTHING_ACC_TO_REFINE,
    SMOOTHING_SPEED,
)
from event_synchronization.with_tracking.warning_manager import WarningManager


class FormattedTrackingManager:
    """
    A manager class responsible for handling the formatted tracking data.

    Attributes:
    -----------
        tracking_data (List[Dict]): tracking data
        match_data (Dict): match data
    """

    def __init__(self, tracking_data: List[Dict], match_data: Dict) -> None:
        self.tracking_data = tracking_data
        self.match_data = match_data

        self.ply_id_to_ply = self.get_ply_id_to_ply()
        self.ply_id_to_idx = self.get_ply_id_to_idx()
        self.set_meta_info()
        self.team_id_to_idx_list = self.get_team_id_to_idx_list()
        self.set_tracking_formatted_data()
        self.formatted_dist_ply_ball = self.get_dist_ply_ball()
        self.set_speed_acc_formatted_data()

    def get_ply_id_to_ply(self) -> Dict[int, Dict]:
        """Generates a mapping of player IDs to their respective player dictionaries.

        Returns:
            dict: A dictionary where keys are player IDs and values are corresponding player dictionaries.
        """

        return {ply['id']: ply for ply in self.match_data['players']}

    def get_active_ply_id_list_from_tracking(self) -> List:
        return list({player_data['player_id'] for data in self.tracking_data for player_data in data['player_data']})

    def get_active_ply_id_list_from_match_data(self) -> List[int]:
        """Generates a list of active player IDs from the match data.

        Returns:
            list: A list of active player IDs.
        """

        return [player['id'] for player in self.match_data['players'] if player['start_time'] is not None]

    def get_ply_id_to_idx(self) -> Dict[int, int]:
        """Generates a mapping of active player IDs to their respective indices in the match data.

        Returns:
            dict: A dictionary where keys are active player IDs and values are corresponding idx for formatted_data
        """

        # get active player id list from tracking and match data
        active_ply_id_list_from_tracking = self.get_active_ply_id_list_from_tracking()
        active_ply_id_list_from_match_data = self.get_active_ply_id_list_from_match_data()

        # check if active player id list from tracking and match data are the same
        WarningManager().warning_player_id_in_tracking_not_in_match_data(
            active_ply_id_list_from_match_data, active_ply_id_list_from_tracking, self.ply_id_to_ply
        )

        # get the union of active player id list from tracking and match data
        active_player_id_match_list = list(
            set(active_ply_id_list_from_tracking) | set(active_ply_id_list_from_match_data)
        )
        return {ply_id: idx for idx, ply_id in enumerate(active_player_id_match_list)}

    def get_team_id_to_idx_list(self) -> Dict[int, List[int]]:
        """Generates a mapping of team IDs to a list of player indices in the match data.

        Returns:
            dict: A dictionary where keys are team IDs and values are lists of player indices.
        """

        team_id_to_idx_list = defaultdict(list)
        for ply_id, ply_idx in self.ply_id_to_idx.items():
            team_id = self.ply_id_to_ply.get(ply_id, {}).get('team_id')
            if team_id is not None:
                team_id_to_idx_list[self.ply_id_to_ply[ply_id]['team_id']].append(ply_idx)
        return team_id_to_idx_list

    def set_meta_info(self) -> None:
        """Sets metadata information about the number of players and frames in the tracking data.

        The metadata information includes:
        - `self.nb_players`: The number of active players in the match.
        - `self.nb_frames`: The number of frames in the tracking data.
        """

        self.nb_players = len(self.ply_id_to_idx)
        self.nb_frames = len(self.tracking_data)

    def set_tracking_formatted_data(self) -> None:
        """
        Initializes and fills formatted numpy arrays for storing player and ball tracking information.

        The formatted data arrays are:
        - `self.ply_formatted_data`: Stores the (x, y) of players with shape (nb_frames, nb_players, 2)
        - `self.ball_formatted_data`: Stores the (x, y, z) of the ball with shape (nb_frames, 1, 3)
        - `self.ply_formatted_is_detected`: Stores boolean values indicating whether players were detected.

        The `self.periods_start_end_dict` stores  the start and end frame for each period.
        """

        # init formatted np array to store players and ball tracking info
        self.ply_formatted_data = np.full((self.nb_frames, self.nb_players, 2), np.nan, dtype=np.float32)
        self.ball_formatted_data = np.full((self.nb_frames, 1, 3), np.nan, dtype=np.float32)
        self.ply_formatted_is_detected = np.full((self.nb_frames, self.nb_players), 0, dtype=bool)

        # dict to store start and end frame for each period
        self.periods_start_end_dict = {}

        # loop to fill the formatted np array
        for data in self.tracking_data:
            # players
            for player_data in data['player_data']:
                ply_idx = self.ply_id_to_idx[player_data['player_id']]
                self.ply_formatted_data[data['frame'], ply_idx] = player_data['x'], player_data['y']
                self.ply_formatted_is_detected[data['frame'], ply_idx] = player_data['is_detected']

            # ball
            ball_data = data['ball_data']
            if ball_data['x'] is not None:
                self.ball_formatted_data[data['frame']] = ball_data['x'], ball_data['y'], ball_data['z']

            if data['period'] is not None and (data['period'] not in self.periods_start_end_dict):
                # fill period_start (default end)
                self.periods_start_end_dict[data['period']] = (data['frame'], data['frame'])

            if data['period'] is not None:
                # fill period_end with the last value of data['frame'] for each data['period']
                self.periods_start_end_dict[data['period']] = (
                    self.periods_start_end_dict[data['period']][0],
                    data['frame'],
                )

    def get_dist_ply_ball(self) -> np.ndarray:
        """Computes the Euclidean distance between each player and the ball for each frame.

        Returns:
            np.ndarray: A 2D array of shape (nb_frames, nb_players) where each element represents
                        the distance between a player and the ball at a specific frame.
        """

        return np.linalg.norm(self.ply_formatted_data[:, :, :2] - self.ball_formatted_data[:, :, :2], axis=-1)

    def set_speed_acc_formatted_data(self) -> None:
        """Computes and sets the speed and acceleration data for players based on their tracking data.

        The formatted data arrays are:
        - `self.ply_formatted_data_speed_acc`: Stores the speed norms, speed vectors (vx, vy), and acc for players
        - `self.ball_formatted_data_speed_acc`: Stores the speed norms, speed vectors (vx, vy), and acc for the ball
        """

        # players
        speed_norm, speed_vect, acc, _ = compute_speeds_acc(self.ply_formatted_data[:, :, :2])
        self.ply_formatted_data_speed_acc = np.concatenate(
            (speed_norm[:, :, None], speed_vect, acc[:, :, None]), axis=2
        )
        self.ply_formatted_data_speed_acc = np.round(self.ply_formatted_data_speed_acc.astype(float), 2)
        self.ply_formatted_data_speed_acc = apply_physical_criterion(self.ply_formatted_data_speed_acc)

        # ball
        ball_speed_norm, ball_speed_vect, ball_acc, ball_acc_to_refine = compute_speeds_acc(
            self.ball_formatted_data[:, :, :2]
        )
        self.ball_formatted_data_speed_acc = np.concatenate(
            (ball_speed_norm[:, :, None], ball_speed_vect, ball_acc[:, :, None], ball_acc_to_refine[:, :, None]), axis=2
        )


def compute_speeds_acc(formatted_data: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Computes speed norms, speed vectors (vx, vy), and acceleration.

    Args:
        formatted_data (np.ndarray): The tracking data of players with shape (nb_frames, nb_players, 2).

    Returns:
        Tuple[np.ndarray, np.ndarray, np.ndarray]:
            - speeds (np.ndarray): The speed norms for each player.
            - speed_vect (np.ndarray): The speed vectors (vx, vy) for each player.
            - acc (np.ndarray): The accelerations for each player.
    """

    diff_speed = formatted_data[SMOOTHING_SPEED:] - formatted_data[:-SMOOTHING_SPEED]

    # vx, vy
    speed_vect = diff_speed / (SMOOTHING_SPEED / FPS)
    add = np.zeros((SMOOTHING_SPEED // 2, speed_vect.shape[1], 2), dtype=np.float32) + np.nan
    speed_vect = np.concatenate((add, speed_vect, add))

    # speed norm
    distances = np.linalg.norm(diff_speed, axis=2)
    speed_norm = distances / (SMOOTHING_SPEED / FPS)
    add = np.zeros((SMOOTHING_SPEED // 2, speed_norm.shape[1]), dtype=np.float32) + np.nan
    speed_norm = np.concatenate((add, speed_norm, add))

    # acc to provide
    diff_acc = speed_norm[SMOOTHING_ACC:] - speed_norm[:-SMOOTHING_ACC]
    acc = diff_acc / (SMOOTHING_ACC / FPS)
    add_acc = np.zeros((SMOOTHING_ACC // 2, acc.shape[1]), dtype=np.float32) + np.nan
    acc = np.concatenate((add_acc, acc, add_acc))

    # acc to refine
    diff_acc = speed_norm[SMOOTHING_ACC_TO_REFINE:] - speed_norm[:-SMOOTHING_ACC_TO_REFINE]
    acc_to_refine = diff_acc / (SMOOTHING_ACC_TO_REFINE / FPS)
    add_acc = np.zeros((SMOOTHING_ACC_TO_REFINE // 2, acc_to_refine.shape[1]), dtype=np.float32) + np.nan
    acc_to_refine = np.concatenate((add_acc, acc_to_refine, add_acc))
    return speed_norm, speed_vect, acc, acc_to_refine


def apply_physical_criterion(ply_formatted_data_speed_acc: np.ndarray) -> np.ndarray:
    """Apply physical criterion to ply formatted data
    Avoid impossible speed and acc values using a physical criterion

    Args:
        ply_formatted_data_speed_acc (np.ndarray): ply formatted data with speed, acc info

    Returns:
        np.ndarray: ply formatted data with speed, acc info after applying physical
    """

    physical_criterion = (
        -0.6354 * ply_formatted_data_speed_acc[:, :, IDX_SPEED] + 9.1 - ply_formatted_data_speed_acc[:, :, IDX_ACC]
    )
    mask = (physical_criterion <= 0) + (ply_formatted_data_speed_acc[:, :, IDX_SPEED] > IMPOSSIBLE_SPEED_TH)
    for idx in [IDX_SPEED, IDX_VX, IDX_VY, IDX_ACC]:
        ply_formatted_data_speed_acc[mask, idx] = np.nan
    return ply_formatted_data_speed_acc
