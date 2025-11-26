from typing import Dict, List

import numpy as np
from scipy import signal

from event_synchronization.constants import DEFAULT_START, MIN_PASS_PER_PERIOD, PASS_EVENT, TH_DIST_PLY_BALL
from event_synchronization.events_utils.event import Event
from event_synchronization.with_tracking.formatted_data_manager import FormattedTrackingManager

SEARCH_OFFSET = 25


class OffsetSyncManager:
    """
    A manager class responsible for handling the period start offset synchronization process.

    Attributes:
    -----------
        events (List[Event]): events
        formatted_data_manager (FormattedTrackingManager): formatted data manager
        ply_id_to_idx (Dict[int, int]): mapping of player IDs to their respective indices in the match data
    """

    def __init__(self, events: List[Event], formatted_data_manager: FormattedTrackingManager) -> None:
        self.events = events
        self.formatted_data_manager = formatted_data_manager
        self.ply_id_to_idx = formatted_data_manager.ply_id_to_idx

    def get_is_players_close_to_ball(self) -> np.array:
        """Get binary np array where each value is 1 if a player is close to the ball, 0 otherwise

        Build a binary vector if a player is involved in an event he is involved
        if distance with the ball is lower than a TH (TH_DIST_PLY_BALL)

        Returns:
            is_players_close_to_ball (np.array): binary np array
        """

        is_players_close_to_ball = self.formatted_data_manager.formatted_dist_ply_ball.copy()
        mask_nan = np.isnan(is_players_close_to_ball)
        is_players_close_to_ball[mask_nan] = 1e4  # replace nan to avoid warning
        is_players_close_to_ball[is_players_close_to_ball < TH_DIST_PLY_BALL] = 1

        is_players_close_to_ball[mask_nan] = np.nan
        is_players_close_to_ball[np.isnan(is_players_close_to_ball)] = 0
        is_players_close_to_ball[is_players_close_to_ball >= TH_DIST_PLY_BALL] = 0
        return is_players_close_to_ball

    def get_ply_id_to_list_of_event_frame(self, period: int, default_start: int) -> Dict[int, List[int]]:
        """Creates a dictionary mapping player IDs to a list of event frames for a given period.

        Args:
            period (int): The period number to filter events.
            default_start (int): The default frame start value used in the get_event_frame function

        Returns:
            Dict[int, List[int]]: A dictionary where the keys are player IDs and the values
                                are lists of event frames corresponding to those player IDs
        """

        is_ply_event_dict = {ply_id: [] for ply_id in self.ply_id_to_idx}
        for event in self.events:
            if (
                event.period == period
                and event.generic_event_type == PASS_EVENT
                and event.player_id is not None
                and event.player_id in self.ply_id_to_idx
            ):
                is_ply_event_dict[event.player_id].append(get_event_frame(event, default_start))
        return is_ply_event_dict

    def get_first_period_start_estimation(self, period: int) -> int:
        """Get first estimation of period start using convolution alignment

        Args:
            period (int): period to sync

        Returns:
            period_start (int): period start
        """

        # get is_players_close_to_ball binary np array
        is_players_close_to_ball = self.get_is_players_close_to_ball()

        # get default frame start
        default_start = DEFAULT_START[period]

        # get events frame by ply_id
        ply_id_to_list_of_event_frame = self.get_ply_id_to_list_of_event_frame(period, default_start)

        # apply convolution to estimate period start
        estimated_start_by_player = []
        for ply_id, idx_ply in self.ply_id_to_idx.items():
            idx_events = ply_id_to_list_of_event_frame[ply_id]

            # filter-only take ply with more than MIN_PASS_PER_PERIOD
            if len(idx_events) > MIN_PASS_PER_PERIOD[period]:
                is_ply_event = np.bincount(idx_events)
                is_ply_event = is_ply_event[default_start:]

                # apply convolution
                discrete_linear_convolution_arr = signal.fftconvolve(
                    is_players_close_to_ball[default_start:, idx_ply], is_ply_event[::-1], mode='full'
                )

                # get offset
                offset = np.argmax(discrete_linear_convolution_arr) - len(is_ply_event) + 1
                estimated_start_by_player.append(offset + default_start)
        return int(np.percentile(estimated_start_by_player, 50))

    def get_matched_passes_list(self, period: int, estimated_period_start_frame: int) -> List:
        """Extract only matched passes and to store
        It will be use to have the refinment of period start

        Args:
            period (int): period to sync

        Returns:
            matched_passes (list): list of matched passes
        """

        # Search for player possession in a range possession_range around event frame
        possession_indices_range = np.arange(-SEARCH_OFFSET, SEARCH_OFFSET)

        # loop on event pass that will help to find the most accurate period_start
        matched_passes_list = []
        for event in self.events:
            # ignore if event is not a pass or not happen during the period to sync or no ply_id informed
            if event.period != period or event.generic_event_type != PASS_EVENT or event.player_id is None:
                continue

            # get event frame (according to estimated period start)
            event_frame = get_event_frame(event, estimated_period_start_frame)

            # get limits to focus on (window for matching)
            frame_start_window = event_frame - SEARCH_OFFSET
            frame_end_window = event_frame + SEARCH_OFFSET

            # get ply and ball seq xy
            if event.player_id in self.ply_id_to_idx:
                dist_ply_ball = self.formatted_data_manager.formatted_dist_ply_ball[
                    frame_start_window:frame_end_window, self.ply_id_to_idx[event.player_id]
                ]

                # If the ball to player distance criterion is met at least once, set matched frame
                # to the last player possession frame, otherwise event is not taken into account
                short_distances_indexes = np.where(dist_ply_ball[:, np.newaxis] < TH_DIST_PLY_BALL)[0]
                if short_distances_indexes.size > 0:
                    matched_frame = event_frame + possession_indices_range[short_distances_indexes[-1]]
                    matched_passes_list.append((matched_frame, event))
        return matched_passes_list

    def get_refined_period_start(self, period: int) -> int:
        """Refine period start using the mean deviation between event frame and matched frame on the matched passes

        Args:
            period (int): period to sync

        Returns:
            refined_period_start_frame (int): refined period start frame
        """

        # first step: estimation of period_start on huge range
        estimated_period_start_frame = self.get_first_period_start_estimation(period=period)

        # get matched passes and estimated start frame
        matched_passes_list = self.get_matched_passes_list(period, estimated_period_start_frame)

        # Range of period start frames to test
        period_start_frame_to_test_arr = range(
            estimated_period_start_frame - SEARCH_OFFSET, estimated_period_start_frame + SEARCH_OFFSET
        )

        # loop through period start frames and compute, for the matched passes,
        # the mean deviation between event frame and matched frame
        mean_deviations_matched_frames_events_frames = []

        for period_start_frame_to_test in period_start_frame_to_test_arr:
            deltas_matched_frames_events_frames = []

            # Loop through matched passes
            for matched_frame, event in matched_passes_list:
                # get event frame with current tested period start frame
                event_frame = get_event_frame(event, period_start_frame_to_test)

                # add delta between matched frame and event frame
                deltas_matched_frames_events_frames.append(abs(matched_frame - event_frame))

            if deltas_matched_frames_events_frames:
                # compute the mean deviation for matched passes
                deltas_matched_frames_events_frames = np.array(deltas_matched_frames_events_frames)
                mean_deviation = np.mean(deltas_matched_frames_events_frames)
                mean_deviations_matched_frames_events_frames.append(mean_deviation)

        if not mean_deviations_matched_frames_events_frames:
            return estimated_period_start_frame

        # get period start frame with lowest mean deviation, minus 1
        idx_min = np.argmin(mean_deviations_matched_frames_events_frames)
        return period_start_frame_to_test_arr[idx_min] - 1

    def add_event_skc_frame_attr(self, period: int, refined_period_start_frame: int) -> None:
        """
        Add skc_frame attr to each event

        Args:
            period (int): period to sync
            refined_period_start_frame (int): refined period start frame
        """

        for event in self.events:
            if event.period == period:
                provider_frame = get_event_frame(event, refined_period_start_frame)
                event.provider_frame = int(provider_frame)
                event.skc_frame = int(provider_frame)


def get_event_frame(event: Event, event_period_first_frame: int) -> int:
    """Get the SkillCorner frame of the given event
    Args:
        event (event_synchronization.events_utils.events.Event): standardized event
        event_period_first_frame (int, optional). Defaults to 0.

    Returns:
        event_frame (int): SkillCorner frame of the given event
    """

    rounded_timestamp = round(event.timestamp * 10)
    return event_period_first_frame + int(rounded_timestamp)
