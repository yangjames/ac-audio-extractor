#!/usr/bin/env python

import analyze
import argparse
import multiprocessing
from pathlib import Path
import tarfile
import tempfile
import tqdm
import os
from typing import List


def process_audio_file(
    audio_file : str,
    output_file_path : str,
) -> None:
    try:
        analyze.analyze(audio_file, output_file_path, True, True, True, "json", None)
    except Exception as e:
        print(f"Couldn't process file {audio_file}. Reason: {str(e)}. Skipping...")
    finally:
        print(f"Created {output_file_path}")

def process_tarball(tarball_path : str, recompute_existing : bool) -> None:
    parent_dir_name = os.path.dirname(tarball_path).split("/")[-1]
    # Get all filenames in the tarball.
    with tarfile.open(tarball_path, "r") as src_tar:
        # Grab all the filenames in the tarball and their designated output filenames.
        data_samples = src_tar.getnames()
        audio_files = [f for f in data_samples if f.endswith(".wav")]
        output_filenames = [
            os.path.abspath(
                os.path.join(
                    args.output, parent_dir_name, os.path.splitext(af_name)[0] + ".json"
                )
            ) for af_name in audio_files
        ]
        # This part is incredibly stupid, so please fix somehow.
        # Since the analysis code uses essentia to parse sound files, there's no
        # option to feed in file descriptors, only file paths. Here, we
        # essentially untar every audio file, dump its contents into a temporary
        # file, process that temporary file, and then delete the temporary file.
        # Ironically, each file takes between 3s-15s to process, so the I/O is
        # not a bottleneck compared to all the signal processing. This approach
        # allows us to do parallel computations.
        temp_files, pruned_out_files = list(), list()
        for i, (audio_file_name, output_file_path) in enumerate(zip(audio_files, output_filenames)):
            # If the output JSON file already exists, then skip.
            if not recompute_existing and os.path.exists(output_file_path):
                print(f"{output_file_path} already exists. Skipping...")
                continue
            # Create the output file directory if it doesn't already exist.
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            # Extract file and write it directly to the temporary file.
            temp_files.append(tempfile.NamedTemporaryFile(mode="wb", suffix=".wav"))
            audio_tar_member = src_tar.getmember(audio_file_name)
            audio_file = src_tar.extractfile(audio_tar_member)
            temp_files[-1].write(audio_file.read())
            pruned_out_files.append(output_file_path)

        temp_fnames = [temp_file.name for temp_file in temp_files]
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            pool.starmap(process_audio_file, zip(temp_fnames, pruned_out_files))

        for temp_file in temp_files:
            temp_file.close()
    
def get_all_tarball_paths(in_path : str) -> List[str]:
    tarball_fullpaths = list()
    for root, _, files in os.walk(in_path):
        if len(files) == 0:
            continue
        for file in sorted(files):
            tarfile_fullpath = os.path.join(root, file)
            if tarfile_fullpath.endswith(".tar"):
                tarball_fullpaths.append(tarfile_fullpath)
    return tarball_fullpaths

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help="Input directory of datasets.", required=True)
    parser.add_argument('-o', '--output', help="output directory for JSON files.", required=True)
    parser.add_argument('-r', '--recompute-existing', help="By default, we skip existing output files. Enable this flag to force recompute.", action="store_true")

    args = parser.parse_args()

    # Recursively grab the full path of all tarballs.
    in_path = os.path.expanduser(args.input)
    all_tarball_paths = get_all_tarball_paths(in_path)
    for tarball_path in all_tarball_paths:
        # Open up the current tarball
        print(f"Processing tarball {tarball_path}")
        process_tarball(tarball_path, args.recompute_existing)
