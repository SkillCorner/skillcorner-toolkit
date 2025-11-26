from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from event_synchronization.with_dynamic_events.statsbomb import StatsbombSyncDynamicEventsManager
from event_synchronization.events_utils.statsbomb import StatsbombEvents


def load_data(data_dir: Path) -> tuple[dict, pd.DataFrame, dict, dict]:
    """Load the necessary data for StatsBomb/SKC event synchronization."""
    match_data_path = data_dir / 'match_data.json'
    with match_data_path.open() as f:
        match_data = json.load(f)

    skc_events = pd.read_csv(data_dir / 'dynamic_events.csv')

    raw_statsbomb_events_path = data_dir / 'sb_events.json'
    with raw_statsbomb_events_path.open() as f:
        raw_statsbomb_events = json.load(f)

    statsbomb_lineup_path = data_dir / 'sb_lineup.json'
    with statsbomb_lineup_path.open() as f:
        statsbomb_lineup = json.load(f)

    return match_data, skc_events, raw_statsbomb_events, statsbomb_lineup


if __name__ == '__main__':
    data_dir = Path('path/to/your/skc_dynamic_event/and/statsbomb/match/folder')
    output_dir = Path('path/to/load/event/synchronization/results')

    match_data, skc_events, raw_statsbomb_events, statsbomb_lineup = load_data(data_dir)

    statsbomb_home_team_id = None  # or set to an integer team ID if needed
    # standardize events
    if statsbomb_home_team_id is None:  # new version without home team id
        statsbomb_events = StatsbombEvents(raw_statsbomb_events, statsbomb_lineup, match_data)
    else:
        statsbomb_events = StatsbombEvents(raw_statsbomb_events, statsbomb_lineup, match_data, statsbomb_home_team_id)

    skc_events_mapping, statsbomb_events_mapping = StatsbombSyncDynamicEventsManager(
        skc_events, raw_statsbomb_events, statsbomb_events
    ).run()

    skc_events_mapping.to_csv(output_dir / 'skc_events_mapping.csv', index=False)
    statsbomb_events_mapping.to_csv(output_dir / 'statsbomb_events_mapping.csv', index=False)
