import os
import argparse
import pandas as pd
from tqdm import tqdm


def parse_args():
    parser = argparse.ArgumentParser(description='Validate and clean event data.')
    parser.add_argument('subjects_root_path', type=str, help='Directory containing subject sub-directories.')
    return parser.parse_args()


def clean_events(subjects_root_path, subject):
    stays_path = os.path.join(subjects_root_path, subject, 'stays.csv')
    events_path = os.path.join(subjects_root_path, subject, 'events.csv')

    stays_df = pd.read_csv(stays_path, dtype={'HADM_ID': str, "ICUSTAY_ID": str}).rename(str.upper, axis='columns')
    events_df = pd.read_csv(events_path, dtype={'HADM_ID': str, "ICUSTAY_ID": str}).rename(str.upper, axis='columns')

    events_df.dropna(subset=['HADM_ID'], inplace=True)

    events_df = events_df.merge(stays_df[['HADM_ID', 'ICUSTAY_ID']], on='HADM_ID', how='left', suffixes=('', '_R'))
    events_df['ICUSTAY_ID'].fillna(events_df['ICUSTAY_ID_R'], inplace=True)

    events_df.dropna(subset=['ICUSTAY_ID'], inplace=True)

    events_df = events_df[events_df['ICUSTAY_ID'] == events_df['ICUSTAY_ID_R']]

    events_df.to_csv(events_path, index=False)


def main():
    args = parse_args()
    subjects = [subj for subj in os.listdir(args.subjects_root_path) if subj.isdigit()]

    for subject in tqdm(subjects, desc='Cleaning events'):
        clean_events(args.subjects_root_path, subject)


if __name__ == "__main__":
    main()
