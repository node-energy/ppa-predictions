import os
import pandas as pd

path_legacy = os.path.join(os.sep, "Users", "kaitimofejew", "PycharmProjects", "etl-runner", "Pipelines",
                           "verbrauchsprognosen-und-teileinspeisung", "output", "Fahrpläne_2024-07-03_Verbrauch")
path_current = os.path.join(os.sep, "Users", "kaitimofejew", "PycharmProjects", "ppa-predictions", "gen_pred_files")

malos = {}

for file in os.listdir(path_current):
    filename = os.fsdecode(file)
    malos[filename.split("_")[0]] = os.path.join(path_current, filename)

for file in os.listdir(path_legacy):
    filename = os.fsdecode(file)
    malo = filename.split("_")[0]

    if malo in malos:
        df_legacy = pd.read_csv(os.path.join(path_legacy, filename), sep=";", index_col=False, usecols=[1])
        df_legacy.rename(columns={df_legacy.columns[0]: "Legacy"}, inplace=True)

        df_new = pd.read_csv(malos.get(malo), sep=";")
        df_new.rename(columns={malo: "Current"}, inplace=True)

        joined_df = pd.concat([df_new, df_legacy], axis=1).reindex(df_new.index)
        print(joined_df)
        joined_df.drop(["Timestamp (Europe/Berlin)"], axis=1, inplace=True)  # = joined_df

        plot = joined_df.plot(title=malo)
        fig = plot.get_figure()
        fig.savefig(f"figs{os.sep}{malo}.png")
