import argparse
import yaml
from decompensation.mimic3csv import *
from decompensation.util import dataframe_from_csv
from decompensation.preprocessing import add_hcup_ccs_2015_groups, make_phenotype_label_matrix


def setup_arg_parser():
    parser = argparse.ArgumentParser(description='Extract per-subject data from MIMIC-III CSV files.')
    parser.add_argument('mimic3_path', type=str, help='Directory containing MIMIC-III CSV files.')
    parser.add_argument('output_path', type=str, help='Directory where per-subject data should be written.')
    parser.add_argument('--event_tables', '-e', type=str, nargs='+', help='Tables from which to read events.',
                        default=['CHARTEVENTS', 'LABEVENTS', 'OUTPUTEVENTS'])
    parser.add_argument('--phenotype_definitions', '-p', type=str,
                        default=os.path.join(os.path.dirname(__file__), 'resources/hcup_ccs_2015_definitions.yaml'),
                        help='YAML file with phenotype definitions.')
    parser.add_argument('--itemids_file', '-i', type=str, help='CSV containing list of ITEMIDs to keep.')
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true', help='Verbosity in output')
    parser.add_argument('--quiet', '-q', dest='verbose', action='store_false', help='Suspend printing of details')
    parser.set_defaults(verbose=True)
    parser.add_argument('--test', action='store_true', help='TEST MODE: process only 1000 subjects, 1000000 events.')
    return parser.parse_args()


def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def process_data(args):
    ensure_directory_exists(args.output_path)

    patients, admits, stays = read_basic_tables(args.mimic3_path, args.verbose)
    stays = process_stays(stays, admits, patients, args)
    diagnoses = process_diagnoses(stays, args.mimic3_path, args.output_path, args.verbose)

    phenotypes = add_hcup_ccs_2015_groups(diagnoses, yaml.safe_load(open(args.phenotype_definitions, 'r')))

    make_phenotype_label_matrix(phenotypes, stays).to_csv(os.path.join(args.output_path, 'phenotype_labels.csv'),
                                                          index=False, quoting=csv.QUOTE_NONNUMERIC)

    if args.test:
        patients, stays = apply_test_mode(patients, stays, args)

    subjects = stays.SUBJECT_ID.unique()
    break_up_stays_by_subject(stays, args.output_path, subjects=subjects)
    break_up_diagnoses_by_subject(phenotypes, args.output_path, subjects=subjects)
    process_events(args.mimic3_path, args.event_tables, args.output_path, args.itemids_file, subjects)


def read_basic_tables(mimic3_path, verbose):
    patients = read_patients_table(mimic3_path)
    admits = read_admissions_table(mimic3_path)
    stays = read_icustays_table(mimic3_path)
    return patients, admits, stays


def process_stays(stays, admits, patients, args):
    stays = remove_icustays_with_transfers(stays)


    stays = merge_on_subject_admission(stays, admits)
    stays = merge_on_subject(stays, patients)
    stays = filter_admissions_on_nb_icustays(stays)

    stays = add_age_to_icustays(stays)
    stays = add_inunit_mortality_to_icustays(stays)
    stays = add_inhospital_mortality_to_icustays(stays)
    stays = filter_icustays_on_age(stays)

    stays.to_csv(os.path.join(args.output_path, 'all_stays.csv'), index=False)
    return stays


def process_diagnoses(stays, mimic3_path, output_path, verbose):
    diagnoses = read_icd_diagnoses_table(mimic3_path)
    diagnoses = filter_diagnoses_on_stays(diagnoses, stays)
    diagnoses.to_csv(os.path.join(output_path, 'all_diagnoses.csv'), index=False)
    count_icd_codes(diagnoses, output_path=os.path.join(output_path, 'diagnosis_counts.csv'))
    return diagnoses



def apply_test_mode(patients, stays, args):
    pat_idx = np.random.choice(patients.shape[0], size=1000, replace=False)
    patients = patients.iloc[pat_idx]
    stays = stays.merge(patients[['SUBJECT_ID']], on='SUBJECT_ID')
    args.event_tables = [args.event_tables[0]]
    if args.verbose:
        print(f'Using only {stays.shape[0]} stays and only {args.event_tables[0]} table')
    return patients, stays


def process_events(mimic3_path, event_tables, output_path, itemids_file, subjects):
    items_to_keep = set(
        [int(itemid) for itemid in dataframe_from_csv(itemids_file)['ITEMID'].unique()]) if itemids_file else None
    for table in event_tables:
        read_events_table_and_break_up_by_subject(mimic3_path, table, output_path, items_to_keep=items_to_keep,
                                                  subjects_to_keep=subjects)


if __name__ == '__main__':
    args = setup_arg_parser()
    process_data(args)
