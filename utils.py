import pandas as pd
import glob

def load_data(folder_path):
    csv_files = glob.glob(folder_path + "/" + "*.csv")
    dfs = []
    for f in csv_files:
        dfs.append(pd.read_csv(f))

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.drop_duplicates(subset="id", keep="first", inplace=True)
    return combined_df


if __name__ == "__main__":
    folder_path = "data3"
    df = load_data(folder_path)
    print(df.info())
