import argparse
import json
import time
import xml.etree.ElementTree as ET

from event_synchronization.with_tracking.event_synchro_manager import EventSynchronizationManager
from event_synchronization.events_utils.opta import OptaEvents
from tools.utils import display_light_table, display_match_info, save_outputs


def load_data(match_data_path, tracking_data_path, raw_opta_events_path, opta_match_data_path, format):
    # load match_data
    with open(match_data_path) as f:
        match_data = json.load(f)

    # load tracking_data
    with open(tracking_data_path) as f:
        tracking_data = [json.loads(line) for line in f.read().splitlines()]

    if format == 'json':
        # load raw_opta_events
        with open(raw_opta_events_path) as f:
            raw_opta_events = json.load(f)

        # load opta_match_data
        with open(opta_match_data_path) as f:
            opta_match_data = json.load(f)
    else:
        raw_opta_events = ET.parse(raw_opta_events_path)
        opta_match_data = ET.parse(opta_match_data_path)
    return match_data, tracking_data, raw_opta_events, opta_match_data


def get_event_format(raw_opta_events_path):
    if raw_opta_events_path.endswith('.json'):
        return 'json'
    if raw_opta_events_path.endswith('.xml'):
        return 'xml'
    raise ValueError('Unknown format for opta_events_path')


def main(match_data_path, tracking_data_path, raw_opta_events_path, opta_match_data_path, save_outputs_dir):
    # get format
    event_format = get_event_format(raw_opta_events_path)

    # load data
    start = time.time()
    match_data, tracking_data, raw_opta_events, opta_match_data = load_data(
        match_data_path, tracking_data_path, raw_opta_events_path, opta_match_data_path, format=event_format
    )
    display_match_info(match_data, 'opta')
    print(f'Data loading took {time.time() - start:.2f} seconds.')

    # process events
    start = time.time()
    # standardize events
    opta_events = OptaEvents(raw_opta_events, opta_match_data, match_data, format=event_format)

    # apply the whole event synchronization process
    report_by_event, report_by_event_type, freeze_frame_format = EventSynchronizationManager(
        tracking_data, match_data, opta_events
    ).apply_synchronization_process()

    # display_light_table (of report_by_event)
    display_light_table(report_by_event_type, opta_events.event_provider)
    print(f'Event synchronization process took {time.time() - start:.2f} seconds.')

    # save report
    save_outputs(save_outputs_dir, report_by_event, report_by_event_type, freeze_frame_format)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Synchronize event data with tracking data.')

    parser.add_argument('--match_data_path', required=True, help='Path to skc match data file')
    parser.add_argument('--tracking_data_path', required=True, help='Path to skc tracking data file')
    parser.add_argument(
        '--opta_events_path', required=True, help='Path to raw Opta events file (xxxx-events.json or xxxx-events.xml)'
    )
    parser.add_argument(
        '--opta_match_data_path', required=True, help='Path to Opta match data file (xxxx-match.json or xxxx-match.xml)'
    )
    parser.add_argument('--save_outputs_dir', required=True, help='Directory to save the outputs')

    args = parser.parse_args()

    main(
        args.match_data_path,
        args.tracking_data_path,
        args.opta_events_path,
        args.opta_match_data_path,
        args.save_outputs_dir,
    )
