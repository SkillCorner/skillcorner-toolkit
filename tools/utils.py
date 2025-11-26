import json
import os

from tabulate import tabulate

KEEP_COLS = ['nb_events', 'is_matched', '%_is_matched']


def get_match_name(match_data):
    date = match_data['date_time'].split('T')[0].replace('-', '')
    match_name = date + '_' + match_data['home_team']['short_name'] + '_' + match_data['away_team']['short_name']
    return match_name


def display_match_info(match_data, event_provider):
    print('=================== MATCH INFO ===================')
    print(f'- match_name: {get_match_name(match_data)}')
    print(f'- match_id: {match_data["id"]}')
    print(f'- event_provider: {event_provider}')
    print('==================================================')


def save_outputs(save_outputs_dir, report_by_event, report_by_event_type, freeze_frame_format):
    if os.path.isdir(save_outputs_dir):
        # freeze_frame_format
        with open(os.path.join(save_outputs_dir, 'freeze_frame_format.json'), 'w') as f:
            json.dump(freeze_frame_format, f)

        # report
        report_by_event.to_csv(os.path.join(save_outputs_dir, 'report_by_event.csv'), index=False)
        report_by_event_type.to_csv(os.path.join(save_outputs_dir, 'report_by_event_type.csv'), index=False)
    else:
        print(f'Warning: {save_outputs_dir} is not a valid directory. Outputs not saved.')


def display_light_table(report_by_event_type, event_provider):
    """Display light table with:
        - event_type
        - nb_events
        - is_matched
        - %_is_matched columns

    Args:
        report_by_event_type (pd.DataFrame): report by event type
        event_provider (str): event provider
    """

    # only display events that makes sense to apply is_matched process
    report_by_event_type = report_by_event_type[report_by_event_type['is_matched_applicable']]

    # display lighter table
    print(
        tabulate(
            report_by_event_type[[f'{event_provider}_event_type', *KEEP_COLS]],
            headers='keys',
            tablefmt='fancy_grid',
            showindex=False,
        )
    )
