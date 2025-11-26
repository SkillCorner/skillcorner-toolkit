import argparse
import json
import time

from event_synchronization.with_tracking.event_synchro_manager import EventSynchronizationManager
from event_synchronization.events_utils.wyscout import WyscoutEvents
from tools.utils import display_light_table, display_match_info, save_outputs


def load_data(match_data_path, tracking_data_path, raw_wyscout_events_path):
    # load match_data
    with open(match_data_path) as f:
        match_data = json.load(f)

    # load tracking_data
    with open(tracking_data_path) as f:
        tracking_data = [json.loads(line) for line in f.read().splitlines()]

    # load raw_wyscout_events
    with open(raw_wyscout_events_path) as f:
        raw_wyscout_events = json.load(f)
    return match_data, tracking_data, raw_wyscout_events


def main(match_data_path, tracking_data_path, raw_wyscout_events_path, save_outputs_dir):
    # load data
    start = time.time()
    match_data, tracking_data, raw_wyscout_events = load_data(
        match_data_path, tracking_data_path, raw_wyscout_events_path
    )
    display_match_info(match_data, 'wyscout')
    print(f'Data loading took {time.time() - start:.2f} seconds.')

    start = time.time()
    # standardize events
    wyscout_events = WyscoutEvents(raw_wyscout_events, match_data)

    # apply the whole event synchronization process
    report_by_event, report_by_event_type, freeze_frame_format = EventSynchronizationManager(
        tracking_data, match_data, wyscout_events
    ).apply_synchronization_process()

    # display_light_table (of report_by_event)
    display_light_table(report_by_event_type, wyscout_events.event_provider)
    print(f'Event synchronization process took {time.time() - start:.2f} seconds.')

    # save report
    save_outputs(save_outputs_dir, report_by_event, report_by_event_type, freeze_frame_format)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Synchronize event data with tracking data.')

    parser.add_argument('--match_data_path', required=True, help='Path to skc match data file')
    parser.add_argument('--tracking_data_path', required=True, help='Path to skc tracking data file')
    parser.add_argument(
        '--wyscout_events_path', required=True, help='Path to raw Wyscout events file (xxxx-events.json)'
    )
    parser.add_argument('--save_outputs_dir', required=True, help='Directory to save the outputs')

    args = parser.parse_args()

    main(
        args.match_data_path,
        args.tracking_data_path,
        args.wyscout_events_path,
        args.save_outputs_dir,
    )
