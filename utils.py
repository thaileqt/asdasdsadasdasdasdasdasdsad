import pandas as pd
import glob

def load_data(folder_path):
    csv_files = glob.glob(folder_path + "/" + "*.csv")
    dfs = []
    for f in csv_files:
        dfs.append(pd.read_csv(f))

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.drop_duplicates(subset="id", keep="first", inplace=True)
    combined_df = combined_df.reset_index()
    combined_df.drop('index', axis=1, inplace=True)
    return combined_df

def get_id_list_from_df(df, save_to_file=False):

    id_list = df["id"].tolist()
    # save id_list to file
    if save_to_file:
        with open("id_list4.txt", "w") as f:
            for id in id_list:
                f.write(str(id) + "\n")
    return id_list


def get_id_list_from_file(file_path):
    with open(file_path, "r") as f:
        id_list = f.read().splitlines()
    return id_list


if __name__ == "__main__":
    folder_path = "data4"
    df = load_data(folder_path)
    get_id_list_from_df(df, save_to_file=True)
    print(len(get_id_list_from_file("id_list4.txt")))
