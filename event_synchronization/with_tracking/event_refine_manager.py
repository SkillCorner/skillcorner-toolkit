from typing import List, Tuple

import numpy as np

from event_synchronization.constants import IDX_ACC_TO_REFINE
from event_synchronization.events_utils.event import Event
from event_synchronization.with_tracking.formatted_data_manager import FormattedTrackingManager

WINDOW_OFFSET = 10  # in frames
DIST_BALL_TH = 3.0  # in meters
IS_DETECTED_TH = 0.5  # ratio of detected frames in the window
BALL_ACC_TH = 7.0  # in m/s^2
LOCAL_OFFSET_FRAME_PAST = 5  # in frames


class EventRefineManager:
    """
    A manager class responsible for handling the event refinement process
    Using tracking data, especially player_event and ball, the goal is to refine the event frame
    to detect the best frame that correspond to the real event frame

    Attributes:
    -----------
        events (List[Event]): events
        formatted_data_manager (FormattedTrackingManager): formatted data manager
    """

    def __init__(self, events: List[Event], formatted_data_manager: FormattedTrackingManager) -> None:
        self.events = events
        self.formatted_data_manager = formatted_data_manager

    def get_event_frame_before(self, idx_event: int) -> Event:
        """Get the frame before the event

        Args:
            idx_event (int): index of the event

        Returns:
            Event: event before the event
        """

        return self.events[idx_event - 1].provider_frame if idx_event > 0 else 0

    def get_event_frame_after(self, idx_event: int) -> Event:
        """Get the frame after the event

        Args:
            idx_event (int): index of the event

        Returns:
            Event: event after the event
        """

        return (
            self.events[idx_event + 1].provider_frame
            if idx_event < len(self.events) - 1
            else len(self.formatted_data_manager.ply_formatted_data)
        )

    def get_frame_window_start_end(self, event: Event, idx_event: int) -> Tuple[int, int]:
        """Get the window limits to focus on

        Args:
            event (Event): event
            idx_event (int): index of the event

        Returns:
            Tuple[int, int]: frame_start_window, frame_end_window
        """

        event_frame_before = self.get_event_frame_before(idx_event)
        event_frame_after = self.get_event_frame_after(idx_event)

        frame_start_window = max(0, event.provider_frame - event.offset_refine, event_frame_before + 1)
        frame_end_window = min(
            event.provider_frame + event.offset_refine,
            len(self.formatted_data_manager.ply_formatted_data),
            event_frame_after - 1,
        )
        return frame_start_window, frame_end_window

    def check_is_detected_enough_condition(self, frame_start_window: int, frame_end_window: int, ply_idx: int) -> bool:
        """Check if player is detected enough in the window

        Args:
            frame_start_window (int): start frame of the window
            frame_end_window (int): end frame of the window
            ply_idx (int): player index in the formatted np array

        Returns:
            bool: is_detected_window > IS_DETECTED_TH
        """

        is_detected_window = self.formatted_data_manager.ply_formatted_is_detected[
            frame_start_window:frame_end_window, ply_idx
        ]
        ratio_detected_window = np.sum(is_detected_window) / len(is_detected_window)
        return ratio_detected_window > IS_DETECTED_TH

    def get_masked_dist_ply_ball_window(
        self, frame_start_window: int, frame_end_window: int, ply_idx: int
    ) -> Tuple[np.array, np.array]:
        """Get masked distance between player and ball in the window

        Args:
            frame_start_window (int): start frame of the window
            frame_end_window (int): end frame of the window
            ply_idx (int): player index in the formatted np array

        Returns:
            Tuple[np.array, np.array]: dist_ply_ball_window, mask_dist_ply_ball
        """

        dist_ply_ball_window = np.copy(
            self.formatted_data_manager.formatted_dist_ply_ball[frame_start_window:frame_end_window, ply_idx]
        )
        mask_dist_ply_ball = dist_ply_ball_window > DIST_BALL_TH
        if np.all(mask_dist_ply_ball):
            return None, None
        dist_ply_ball_window[mask_dist_ply_ball] = np.nan
        return dist_ply_ball_window, mask_dist_ply_ball

    def get_masked_ball_acc_window(
        self, frame_start_window: int, frame_end_window: int, mask_dist_ply_ball: np.array
    ) -> np.array:
        """Get masked ball acceleration in the window

        Args:
            frame_start_window (int): start frame of the window
            frame_end_window (int): end frame of the window
            mask_dist_ply_ball (np.array): mask of the distance between player and ball

        Returns:
            np.array: ball_acc_window
        """

        # ball acceleration
        ball_acc_window = np.copy(
            self.formatted_data_manager.ball_formatted_data_speed_acc[
                frame_start_window:frame_end_window, 0, IDX_ACC_TO_REFINE
            ]
        )
        # only focus on the ball acceleration when the ball is close to the player
        ball_acc_window[mask_dist_ply_ball] = np.nan
        return None if np.all(np.isnan(ball_acc_window)) else ball_acc_window

    def get_refine_idx_in_window(self, ball_acc_window: np.array) -> int:
        """Get the best frame that correspond to the last touch of the ball (pass/shot frame)

        Args:
            ball_acc_window (np.array): ball acceleration in the window

        Returns:
            int: refine_idx_in_window
        """

        # last idx in window where ball_acc is at least DIST_BALL_TH
        last_idx = np.where(np.isfinite(ball_acc_window))[0][-1]
        mask = np.zeros_like(ball_acc_window) + np.nan
        mask[max(0, last_idx - LOCAL_OFFSET_FRAME_PAST) : last_idx + 1] = 1
        ball_acc_window *= mask
        return None if np.nanmax(ball_acc_window) < BALL_ACC_TH else np.nanargmax(ball_acc_window)

    def event_refinement(self, event: Event, idx_event: int) -> None:
        """Event refinement process

        Args:
            event (Event): event
            idx_event (int): index of the event
        """

        # get window limits to focus on
        frame_start_window, frame_end_window = self.get_frame_window_start_end(event, idx_event)
        if frame_end_window - frame_start_window < 1:
            return

        # get corresponding player_idx in the formatted np array
        ply_idx = self.formatted_data_manager.ply_id_to_idx.get(event.player_id)

        # check if player is detected enough - no refinement if not
        if not self.check_is_detected_enough_condition(frame_start_window, frame_end_window, ply_idx):
            return

        # get distance between player and ball - no refinement if all dist are higher than TH
        dist_ply_ball_window, mask_dist_ply_ball = self.get_masked_dist_ply_ball_window(
            frame_start_window, frame_end_window, ply_idx
        )
        if dist_ply_ball_window is None:
            return

        # ball acceleration
        ball_acc_window = self.get_masked_ball_acc_window(frame_start_window, frame_end_window, mask_dist_ply_ball)
        if ball_acc_window is None:
            return

        # method to find the best frame that correspond to the last touch of the ball (pass/shot frame)
        refine_idx_in_window = self.get_refine_idx_in_window(ball_acc_window)
        if refine_idx_in_window is None:
            return

        event.skc_frame = frame_start_window + refine_idx_in_window

    def apply_events_refinement_process(self, apply_refine: bool, event_provider: str) -> None:
        """Apply the event refinement process

        Args:
            apply_refine (bool): whether to apply the refinement process
            event_provider (str): event provider
        """

        for idx_event, event in enumerate(self.events):
            # no_refine when no mapping with existing player_id
            if event.player_id not in self.formatted_data_manager.ply_id_to_idx:
                continue

            # no_refine rules for impect
            if event_provider == 'impect' and not refine_impect_rule(event, apply_refine):
                continue

            # no_refine rules for other providers
            if event_provider != 'impect' and not event.to_refine:
                continue

            self.event_refinement(event, idx_event)


def refine_impect_rule(event: Event, apply_refine: bool) -> bool:
    """Refine impect rule

    Args:
        event (Event): event
        apply_refine (bool): whether to apply the refinement process

    Returns:
        bool: event.to_refine if apply_refine else event.force_to_refine
    """

    return event.to_refine if apply_refine else event.force_to_refine
