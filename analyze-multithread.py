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
    tarball_path : str,
    recompute_existing : bool
) -> None:
    print(f"Processing {audio_file}")
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    if not recompute_existing and os.path.exists(output_file_path):
        # print(f"{output_file_path} already exists. Skipping...")
        return
    try:
        # Extract file and write it directly to the temporary file.
        ff = tempfile.NamedTemporaryFile(mode="wb", suffix=".wav")
        with tarfile.open(tarball_path, "r") as src_tar:
            audio_tar_member = src_tar.getmember(audio_file)
            audio_file = src_tar.extractfile(audio_tar_member)
            ff.write(audio_file.read())
        analyze.analyze(ff.name, output_file_path, True, True, True, "json", None)
    except Exception as e:
        print(f"Couldn't process file {audio_file}. Reason: {str(e)}. Skipping...")
    finally:
        print(f"Created {output_file_path}")
        ff.close()

def process_tarball(tarball_path : str, recompute_existing : bool) -> None:
    parent_dir_name = os.path.dirname(tarball_path).split("/")[-1]
    # Get all filenames in the tarball.
    with tarfile.open(tarball_path, "r") as src_tar:
        data_samples = src_tar.getnames()
        audio_files = [f for f in data_samples if f.endswith(".wav")]
        output_filenames = [
            os.path.abspath(
                os.path.join(
                    args.output, parent_dir_name, os.path.splitext(af_name)[0] + ".json"
                )
            ) for af_name in audio_files
        ]
        for audio_file_name, output_file_path in zip(audio_files, output_filenames):
            if not recompute_existing and os.path.exists(output_file_path):
                print(f"{output_file_path} already exists. Skipping...")
                continue
            try:
                # Extract file and write it directly to the temporary file.
                ff = tempfile.NamedTemporaryFile(mode="wb", suffix=".wav")
                audio_tar_member = src_tar.getmember(audio_file_name)
                audio_file = src_tar.extractfile(audio_tar_member)
                ff.write(audio_file.read())
                analyze.analyze(ff.name, output_file_path, True, True, True, "json", None)
            except Exception as e:
                print(f"Couldn't process file {audio_file_name}. Reason: {str(e)}. Skipping...")
            finally:
                print(f"Created {output_file_path}")
                ff.close()

    # Create a processing job for each filename.
    # jobs = [
    #     (audio_file, output_filename, tarball_path, recompute_existing)
    #     for audio_file, output_filename in zip(audio_files, output_filenames)
    # ]
    # with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
    #     pool.starmap(process_audio_file, jobs)
    
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
