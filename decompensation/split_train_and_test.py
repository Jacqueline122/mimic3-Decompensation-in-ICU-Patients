import os
import shutil
import argparse


def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)


def move_patients(subjects_root_path, patients, partition):
    partition_path = os.path.join(subjects_root_path, partition)
    ensure_directory_exists(partition_path)
    for patient in patients:
        src = os.path.join(subjects_root_path, patient)
        dest = os.path.join(partition_path, patient)
        shutil.move(src, dest)


def get_test_patients(test_set_path):
    test_set = set()
    with open(test_set_path, "r") as file:
        for line in file:
            patient_id, label = line.strip().split(',')
            if label == '1':
                test_set.add(patient_id)
    return test_set


def partition_patients(subjects_root_path, test_set):
    all_patients = [folder for folder in os.listdir(subjects_root_path) if folder.isdigit()]
    train_patients = [patient for patient in all_patients if patient not in test_set]
    test_patients = list(test_set)
    return train_patients, test_patients


def parse_arguments():
    parser = argparse.ArgumentParser(description='Split data into train and test sets.')
    parser.add_argument('subjects_root_path', type=str, help='Directory containing subject sub-directories.')
    return parser.parse_args()


def main():
    args = parse_arguments()
    test_set_path = os.path.join(os.path.dirname(__file__), 'resources/testset.csv')
    test_set = get_test_patients(test_set_path)

    train_patients, test_patients = partition_patients(args.subjects_root_path, test_set)

    move_patients(args.subjects_root_path, train_patients, "train")
    move_patients(args.subjects_root_path, test_patients, "test")


if __name__ == '__main__':
    main()
