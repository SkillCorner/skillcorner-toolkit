from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from event_synchronization.with_dynamic_events.wyscout import WyscoutSyncDynamicEventsManager
from event_synchronization.events_utils.wyscout import WyscoutEvents


def load_data(data_dir: Path) -> tuple[dict, dict, pd.DataFrame]:
    with (data_dir / 'match_data.json').open() as f:
        wyscout_match_data = json.load(f)

    skc_events = pd.read_csv(data_dir / 'dynamic_events.csv')

    raw_wyscout_events_path = data_dir / 'raw_wyscout_events.json'
    with raw_wyscout_events_path.open() as f:
        raw_wyscout_events = json.load(f)

    return wyscout_match_data, raw_wyscout_events, skc_events


if __name__ == '__main__':
    data_dir = Path('path/to/your/skc_dynamic_event/and/wyscout/match/folder')
    output_dir = Path('path/to/load/event/synchronization/results')

    wyscout_match_data, raw_wyscout_events, skc_events = load_data(data_dir)

    wyscout_events_manager = WyscoutEvents(raw_wyscout_events, wyscout_match_data)
    wyscout_manager = WyscoutSyncDynamicEventsManager(skc_events, raw_wyscout_events, wyscout_events_manager)
    skc_events_mapping, wyscout_events_mapping = wyscout_manager.run()

    skc_events_mapping.to_csv(output_dir / 'skc_events_mapping.csv', index=False)
    wyscout_events_mapping.to_csv(output_dir / 'wyscout_events_mapping.csv', index=False)
