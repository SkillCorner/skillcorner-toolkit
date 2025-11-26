from typing import List

import numpy as np

from event_synchronization.constants import TH_IS_MATCHED
from event_synchronization.events_utils.event import Event
from event_synchronization.with_tracking.formatted_data_manager import FormattedTrackingManager

NAN_DIST = 100.0  # to avoid warning
OFFSET = 5  # frame offset to focus on


class EventsMatchingManager:
    """
    A manager class responsible for handling the is_matched flag of events
    is_matched helps to understand if the event is correctly matched with the tracking data
    distance ply - ball is used to determine if the event is matched around the event frame

    Attributes:
    -----------
        events (List[Event]): events
        formatted_data_manager (FormattedTrackingManager): formatted data manager
    """

    def __init__(self, events: List[Event], formatted_data_manager: FormattedTrackingManager) -> None:
        self.events = events
        self.formatted_data_manager = formatted_data_manager

    def add_is_matched_attr(self, event: Event) -> None:
        """Add is_matched attribute to event

        Args:
            event (Event): event
            dist_ply_ball_seq (np.array): distance seq between ball and player sequence
        """

        # get window limits to focus on
        frame_start_window = max(event.skc_frame - OFFSET, 0)
        frame_end_window = min(event.skc_frame + OFFSET, len(self.formatted_data_manager.ply_formatted_data))

        # get corresponding player_idx in the formatted np array
        ply_idx = self.formatted_data_manager.ply_id_to_idx[event.player_id]

        # get dist sequence between ball and event player
        dist_ply_ball_seq = self.formatted_data_manager.formatted_dist_ply_ball[
            frame_start_window:frame_end_window, ply_idx
        ]

        # to avoid warning
        dist_ply_ball_seq[np.isnan(dist_ply_ball_seq)] = NAN_DIST

        # at least one frame where dist_ball_ply is lower than TH_IS_MATCHED
        event.is_matched = bool(np.sum(dist_ply_ball_seq <= TH_IS_MATCHED) > 0)

    def add_is_player_detected_attr(self, event: Event) -> None:
        """Add is_player_detected attribute to event

        Args:
            event (Event): event
        """

        # get corresponding player_idx in the formatted np array
        ply_idx = self.formatted_data_manager.ply_id_to_idx[event.player_id]
        event.is_player_detected = (
            False
            if event.skc_frame >= len(self.formatted_data_manager.ply_formatted_data)
            else self.formatted_data_manager.ply_formatted_is_detected[event.skc_frame, ply_idx]
        )

    def add_frame_tracking_data_available(self, event: Event) -> None:
        """Add has_video attribute to event

        Args:
            event (Event): event
        """

        event.frame_tracking_data_available = (
            False
            if event.skc_frame >= len(self.formatted_data_manager.formatted_dist_ply_ball)
            else np.any(np.isfinite(self.formatted_data_manager.ply_formatted_data[event.skc_frame, :]))
        )

    def run_is_matched_process(self) -> None:
        """
        Add is_matched info attribute to each event
        """

        for event in self.events:
            if event.player_id not in self.formatted_data_manager.ply_id_to_idx:
                event.is_matched = False
                event.is_player_detected = False
                event.has_provider_player_id = event.provider_player_id is not None
                self.add_frame_tracking_data_available(event)
                continue

            # add attr for event (to understand performance/data quality)
            self.add_is_matched_attr(event)
            self.add_is_player_detected_attr(event)
            event.has_provider_player_id = True
            self.add_frame_tracking_data_available(event)
