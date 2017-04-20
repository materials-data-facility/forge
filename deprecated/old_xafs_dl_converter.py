from json import dump
from tqdm import tqdm
import paths
import psycopg2
from bson import ObjectId
from parsers.utils import dc_validate
from urllib import parse


#Converts XAFS Data Library data into feedstock
def xafs_dl_convert(out_file, sack_size=0, sack_file=None, verbose=False):
    query = "select s.id, s.name, s.rating_summary, s.collection_date, s.submission_date, s.d_spacing, s.comments, b.name, f.fullname, s1.name, s1.preparation, s1.material_source, s1.formula, s2.name, ed.name, en.units, p.name from spectrum s, beamline b, facility f, sample s1, sample s2, edge ed, energy_units en, person p where b.id=s.beamline_id and f.id=b.facility_id and s1.id=s.sample_id and s2.id=s.reference_id and ed.id=s.edge_id and en.id=s.energy_units_id and p.id=s.person_id;"
    #Translation vars for convenience and code readability
    s_id = 0
    s_name = 1
    s_rating_summary = 2
    s_collection_date = 3
    s_submission_date = 4
    s_d_spacing = 5
    s_comments = 6
    b_name = 7
    f_fullname = 8
    s1_name = 9
    s1_preparation = 10
    s1_material_source = 11
    s1_formula = 12
    s2_name = 13
    ed_name = 14
    en_units = 15
    p_name = 16
    #Make connection to Postgres
    if verbose:
        print("Processing XAFS data into", out_file)
    with psycopg2.connect("dbname=xafs") as conn:
        with conn.cursor() as curs:
            curs.execute(query) #Run query
            #Open output files
            with open(out_file, 'w') as output:
                if sack_file:
                    count = 0
                    feedsack = open(sack_file, 'w')
                #Process each row
                for record in tqdm(curs, desc="Processing database", disable= not verbose):
                    data = {
                        "original_data_file" : parse.quote("http://cars.uchicago.edu/xaslib/rawfile/" + str(record[s_id]) + "/" + record[s_name] + ".xdi", safe="/:"),
                        "ratings" : record[s_rating_summary],
                        "absorption_edge" : record[ed_name],
                        "sample_name" : record[s1_name],
                        "sample_prep" : record[s1_preparation],
                        "reference_sample" : record[s2_name],
                        "beamline" : record[f_fullname] + " - " + record[b_name],
                        "energy_units" : record[en_units],
                        "d_spacing" : record[s_d_spacing],
                        "date_measured" : str(record[s_collection_date]),
                        "date_uploaded" : str(record[s_submission_date]),
                        "user_comments" : record[s_comments],
#                       "material_source" : record[s1_material_source] #Only one datum
                        }
                    feedstock_data = {
                        "dc.title" : record[s_name],
                        "dc.creator" : "XAFS Data Library",
                        "dc.contributor.author" : [record[p_name]],
                        "dc.identifier" : parse.quote("http://cars.uchicago.edu/xaslib/spectrum/" + str(record[s_id]), safe="/:"),
#                       "dc.subject" : record[s_comments].split(),
                        "dc.description" : record[s_comments],
                        "dc.relatedidentifier" : [data["original_data_file"]],
                        "dc.year" : int(data["date_uploaded"][:4]),
                        "mdf-base.material_composition" : record[s1_formula],
                        "mdf-base.data_acquisition_method" : "XAFS",
                        "mdf_id" : str(ObjectId()),
                        "mdf_source_name" : "xafs_dl",
                        "mdf_source_id" : 18,
                        "mdf_datatype" : "xafs_dl",
                        "acl" : ["public"],
                        "globus_subject" : parse.quote("http://cars.uchicago.edu/xaslib/spectrum/" + str(record[s_id]), safe="/:"),
                        "mdf-publish.publication.collection" : "XAFS Data Library",
                        "data" : data
                        }

                    valid = dc_validate(feedstock_data)
                    if not valid["valid"]:
                        print("Invalid DC metadata:", valid["invalid_fields"])
                        continue

                    dump(feedstock_data, output)
                    output.write("\n")

                    if sack_file and count < sack_size:
                        dump(feedstock_data, feedsack)
                        feedsack.write("\n")
                        count += 1
                if sack_file:
                    feedsack.close()
    if verbose:
        print("Processing complete.")

if __name__ == "__main__":
    xafs_dl_convert(out_file=paths.raw_feed+"xafs_dl_all.json", sack_size=10, sack_file=paths.sack_feed+"xafs_dl_10.json", verbose=True)



