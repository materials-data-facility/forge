#Evan Pike dep78@cornell.edu
#Surface Crystal Energy File Creator
import os
import json
from file_utils import find_files


def create_files(in_dir):

    print("Beginning Creation of txt Files")

    for in_file in find_files(in_dir, ".json"):
        with open(os.path.join(in_file["path"], in_file["filename"]), 'r') as raw_in:
            data = json.load(raw_in)

        for s in data:
            m_id = s["material_id"]
            path = in_dir + m_id
            if not os.path.exists(path):
                os.mkdir(path)
            if in_file["filename"] == "surfaces.json":
                pre = "surfaces_"
            else:
                pre = "details_"
            final_path = os.path.join(path, pre + m_id + ".txt")
            f = open(final_path, 'w')
            json.dump(s, f)
            f.close()


    print("Creation of txt Files Complete")


def create_cifs(in_dir):

    print("Beginning Creation of Cif Files")

    with open(in_dir + "surfaces_vasp_details.json") as raw_in:
        data = json.load(raw_in)

    for s in data:
        m_id = s["material_id"]
        path = in_dir + m_id
        if not os.path.exists(path):
            os.mkdir(path)

        i=1
        for record_id in s["surfaces"]:
            cif = record_id["calculations"]["cif"]
            final_path = os.path.join(path,  m_id + "_cif_record-" + str(i) + ".cif")
            f = open(final_path, 'w')
            f.write(cif)
            f.close()
            i+=1


    print("Creation of Cif Files Complete")



if __name__ == '__main__':
    in_dir = "/".join(os.path.abspath("").split("/")[:-1]) + "/datasets/surface_crystal_energy/"
    create_files(in_dir)
    create_cifs(in_dir)