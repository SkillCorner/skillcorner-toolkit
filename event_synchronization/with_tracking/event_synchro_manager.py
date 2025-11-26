from typing import Dict, List, Tuple

from event_synchronization.events_utils.event import Event
from event_synchronization.with_tracking.event_matching_manager import EventsMatchingManager
from event_synchronization.with_tracking.event_output_manager import EventsOutputManager
from event_synchronization.with_tracking.event_refine_manager import EventRefineManager
from event_synchronization.with_tracking.formatted_data_manager import FormattedTrackingManager
from event_synchronization.with_tracking.offset_manager import OffsetSyncManager
from event_synchronization.with_tracking.warning_manager import WarningManager


class EventSynchronizationManager:
    """A manager class responsible for handling the events synchronization process.

    Attributes:
    -----------
        tracking_data (List[Dict]): tracking data
        match_data (Dict): match data
        events (List[Event]): events
    """

    def __init__(self, tracking_data: List[Dict], match_data: Dict, events: List[Event]) -> None:
        self.tracking_data = tracking_data
        self.match_data = match_data
        self.events = events

        # instanciate formatted tracking manager
        self.formatted_tracking_manager = FormattedTrackingManager(self.tracking_data, self.match_data)

        # instanciate serialized events
        self.serialized_events = self.get_serialized_events()

    def get_serialized_events(self) -> List[Event]:
        """Get serialized events from raw wyscout events

        Returns:
            serialized_events (WyscoutEvents): serialized events
        """

        return self.events.standardize_events()

    def get_refined_period_start_and_add_skc_frame(self) -> Dict:
        """Get refined period start and add skc_frame attr to each event

        Returns:
            refined_period_starts_dict (dict): dict of refined period start
        """

        # init offset sync manager
        offset_sync_manager = OffsetSyncManager(self.serialized_events, self.formatted_tracking_manager)

        # to store period starts
        refined_period_starts_dict = {}
        for period in self.formatted_tracking_manager.periods_start_end_dict:
            # get refined period start
            refined_period_start_frame = offset_sync_manager.get_refined_period_start(period)
            refined_period_starts_dict[period] = refined_period_start_frame

            # add skc_frame attr to each event
            offset_sync_manager.add_event_skc_frame_attr(period, refined_period_start_frame)
        return refined_period_starts_dict

    def apply_synchronization_process(self, apply_refine: bool = True) -> Tuple:
        """Executes the full events synchronization process.

        This method performs the following steps:
        1. Retrieves general offset (correspond to refined period start frame if kick_off is the first event of period)
          and adds an `skc_frame` attribute to each event
        2. Optional: Applies refinement to specific event type, such as PASS and SHOT, using EventRefineManager
        3. Adds an `is_matched` flag to each event using the EventsMatchingManager
        4. Generates reports/freeze_frame_format

        Args:
            apply_refine (bool): If True, applies the refinement process to the events. Defaults to True.

        Warning:
            - If the period start estimation is negative, a warning is generated
            - If the `is_matched` ratio is low on the game, a warning is generated

        Returns:
            tuple: A tuple containing:
                - report_by_event (pd.DataFrame): A report of the refined and synchronized events
                - report_by_event_type (pd.DataFrame): A report categorized by event types
                - freeze_frame_format (dict): dict containing info on tracking data for all players at the event frame
        """

        # get refined period start and add skc_frame attr to each event
        refined_period_start_frame = self.get_refined_period_start_and_add_skc_frame()

        # apply period value check and generate warnings if needed
        WarningManager().warning_negative_period_start_estimation(refined_period_start_frame)

        if apply_refine or self.events.event_provider == 'impect':
            # run refinment for some events (pass/shot type)
            EventRefineManager(self.serialized_events, self.formatted_tracking_manager).apply_events_refinement_process(
                apply_refine, self.events.event_provider
            )

        # is_matched flag added to each event
        EventsMatchingManager(self.serialized_events, self.formatted_tracking_manager).run_is_matched_process()

        # get reports and freeze frame format
        report_by_event, report_by_event_type, freeze_frame_format = EventsOutputManager(
            self.serialized_events, self.formatted_tracking_manager
        ).get_reports_and_freeze_frame_format(self.events.event_provider)

        # apply some check and generate warnings if needed
        WarningManager().warning_low_is_matched(report_by_event)
        WarningManager().warning_mapping_player_id(report_by_event, self.events.event_provider)

        return report_by_event, report_by_event_type, freeze_frame_format
