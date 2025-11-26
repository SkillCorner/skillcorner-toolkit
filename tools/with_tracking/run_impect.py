import argparse
import json
import time

from event_synchronization.with_tracking.event_synchro_manager import EventSynchronizationManager
from event_synchronization.events_utils.impect import ImpectEvents
from tools.utils import display_light_table, display_match_info, save_outputs


def load_data(match_data_path, tracking_data_path, raw_impect_events_path, impect_match_data_path):
    # load match_data
    with open(match_data_path) as f:
        match_data = json.load(f)

    # load tracking_data
    with open(tracking_data_path) as f:
        tracking_data = [json.loads(line) for line in f.read().splitlines()]

    # load raw_statsbomb_events
    with open(raw_impect_events_path) as f:
        raw_impect_events = json.load(f)

    # load statsbomb_lineup
    with open(impect_match_data_path) as f:
        impect_match_data = json.load(f)
    return match_data, tracking_data, raw_impect_events, impect_match_data


def main(match_data_path, tracking_data_path, raw_impect_events_path, impect_match_data_path, save_outputs_dir):
    # load data
    start = time.time()
    match_data, tracking_data, raw_impect_events, impect_match_data = load_data(
        match_data_path, tracking_data_path, raw_impect_events_path, impect_match_data_path
    )
    display_match_info(match_data, 'impect')
    print(f'Data loading took {time.time() - start:.2f} seconds.')

    # process events
    start = time.time()

    # standardize events
    impect_events = ImpectEvents(raw_impect_events, impect_match_data, match_data)

    # apply the whole event synchronization process
    report_by_event, report_by_event_type, freeze_frame_format = EventSynchronizationManager(
        tracking_data, match_data, impect_events
    ).apply_synchronization_process()

    # display_light_table (of report_by_event)
    display_light_table(report_by_event_type, impect_events.event_provider)
    print(f'Event synchronization process took {time.time() - start:.2f} seconds.')

    # save report
    save_outputs(save_outputs_dir, report_by_event, report_by_event_type, freeze_frame_format)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Synchronize event data with tracking data.')

    parser.add_argument('--match_data_path', required=True, help='Path to skc match data file')
    parser.add_argument('--tracking_data_path', required=True, help='Path to skc tracking data file')
    parser.add_argument(
        '--impect_events_path',
        required=True,
        help='Path to raw Impect events file (xxxx-events.json or xxxx-events.xml)',
    )
    parser.add_argument(
        '--impect_match_data_path',
        required=True,
        help='Path to Impect match data file (xxxx-match.json or xxxx-match.xml)',
    )
    parser.add_argument('--save_outputs_dir', required=True, help='Directory to save the outputs')

    args = parser.parse_args()

    main(
        args.match_data_path,
        args.tracking_data_path,
        args.impect_events_path,
        args.impect_match_data_path,
        args.save_outputs_dir,
    )
