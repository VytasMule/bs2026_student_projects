import urllib.request
import os


def download_dataset(url: str, output_path: str) -> None:
    print(f"Downloading from {url}...")
    urllib.request.urlretrieve(url, output_path)
    print(f"Downloaded successfully to {output_path}")


def get_datasets() -> dict[str, str]:
    return {
        "Zmumu_Run2011A.csv": "https://opendata.cern.ch/record/545/files/Zmumu.csv",
        "Ymumu_Run2011A.csv": "https://opendata.cern.ch/record/545/files/Ymumu.csv",
        "Jpsimumu_Run2011A.csv": "https://opendata.cern.ch/record/545/files/Jpsimumu.csv"
    }


def run_download() -> None:
    os.makedirs('data', exist_ok=True)
    datasets = get_datasets()
    for filename, url in datasets.items():
        output_path = os.path.join('data', filename)
        if not os.path.exists(output_path):
            download_dataset(url, output_path)
        else:
            print(f"File {output_path} already exists. Skipping.")


def download_single_file(url: str, local_filename: str) -> str:
    """Generic function to download a single file to the data directory."""
    os.makedirs('data', exist_ok=True)
    output_path = os.path.join('data', local_filename)
    urllib.request.urlretrieve(url, output_path)
    return output_path


if __name__ == "__main__":
    run_download()
