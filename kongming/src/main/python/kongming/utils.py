import pandas as pd
import os

# parse files from the input folder
def parse_input_files(path):

    files = os.listdir(path)
    files = [path + file for file in files if
                   file.startswith("part")]

    return files

def load_csv(path, columns):
    files = parse_input_files(path)
    dfList = []
    for file in files:
        df = pd.read_csv(file)
        df.columns = columns
        dfList.append(df)
    return pd.concat(dfList, axis=0)


def s3_copy(src_path, dest_path, quiet=True):
    sync_command = f"aws s3 cp {src_path} {dest_path} --recursive"
    if (quiet):
        sync_command = sync_command + " --quiet"
    os.system(sync_command)
    return sync_command

def s3_move(src_path, dest_path, quiet=True):
    sync_command = f"aws s3 mv --recursive {src_path} {dest_path}"
    if (quiet):
        sync_command = sync_command + " --quiet"
    os.system(sync_command)
    return sync_command

def modify_model_embeddings(model, mapping):
    # get the raw weight and create a new weight based on it
    layer = model.get_layer('AdGroupId_embedding')
    weight_raw = layer.get_weights()[0].copy()
    weight_new = weight_raw.copy()

    # replace weight in new weight
    for i in range(mapping.shape[0]):
        baseInt = int(mapping.iloc[i]['BaseAdGroupIdInt'])
        changeInt = int(mapping.iloc[i]['AdGroupIdInt'])
        baseWeight = weight_raw[baseInt, :].copy()
        weight_new[changeInt, :] = baseWeight

    # set new weight
    layer.set_weights([weight_new])
    return model
