import psycopg2
from tqdm import tqdm
from urllib import parse
from validator import Validator


# This is the converter for the XAFS Spectra Library.
# Arguments:
#   verbose (bool): Should the script print status messages to standard output? Default False.
def convert(verbose=False):
    if verbose:
        print("Begin converting")

    # Collect the metadata
    dataset_metadata = {
        "globus_subject": "http://cars.uchicago.edu/xaslib",
        "acl": ["public"],
        "mdf_source_name": "xafs_sl",
        "mdf-publish.publication.collection": "XAFS Spectra Library",

        "dc.title": "XAFS Spectra Library",
        "dc.creator": "University of Chicago",
        "dc.identifier": "http://cars.uchicago.edu/xaslib",
#        "dc.contributor.author": ,
#        "dc.subject": ,
        "dc.description": "This is a collection of X-ray Absorption Spectra. The data here are intended to be of good quality, and on well-characterized samples, but no guarantees are made about either of these intentions.",
#        "dc.relatedidentifier": ,
#        "dc.year": 
        }


    # Make a Validator to help write the feedstock
    # You must pass the metadata to the constructor
    # Each Validator instance can only be used for a single dataset
    dataset_validator = Validator(dataset_metadata)


    # Get the data
    # Each record also needs its own metadata
    query = "select s.id, s.name, s.rating_summary, s.collection_date, s.submission_date, s.d_spacing, s.comments, b.name, f.fullname, s1.name, s1.preparation, s1.formula, s2.name, ed.name, en.units, p.name from spectrum s, beamline b, facility f, sample s1, sample s2, edge ed, energy_units en, person p where b.id=s.beamline_id and f.id=b.facility_id and s1.id=s.sample_id and s2.id=s.reference_id and ed.id=s.edge_id and en.id=s.energy_units_id and p.id=s.person_id;"
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
    s1_formula = 11
    s2_name = 12
    ed_name = 13
    en_units = 14
    p_name = 15
    #Make connection to Postgres
    with psycopg2.connect("dbname=xafs") as conn:
        with conn.cursor() as cursor:
            cursor.execute(query) #Run query
            #Process each row
            for record in tqdm(cursor, desc="Processing database", disable= not verbose):
                record_metadata = {
                    "globus_subject": parse.quote("http://cars.uchicago.edu/xaslib/spectrum/" + str(record[s_id]), safe="/:"),
                    "acl": ["public"],
                    "mdf-publish.publication.collection": "XAFS Spectra Library",
                    "mdf_data_class": "xafs",
                    "mdf-base.material_composition": record[s1_formula],

                    "dc.title": record[s_name],
                    "dc.creator": record[f_fullname] + " - " + record[b_name],
                    "dc.identifier": parse.quote("http://cars.uchicago.edu/xaslib/spectrum/" + str(record[s_id]), safe="/:"),
                    "dc.contributor.author": [record[p_name]],
#                    "dc.subject": ,
                    "dc.description": record[s_comments],
                    "dc.relatedidentifier": [parse.quote("http://cars.uchicago.edu/xaslib/rawfile/" + str(record[s_id]) + "/" + record[s_name] + ".xdi", safe="/:")],
                    "dc.year": int(str(record[s_submission_date])[:4]),

                    "data": {
#                        "raw": ,
                        "files": {"xdi": parse.quote("http://cars.uchicago.edu/xaslib/rawfile/" + str(record[s_id]) + "/" + record[s_name] + ".xdi", safe="/:")},
#                        "original_data_file" : parse.quote("http://cars.uchicago.edu/xaslib/rawfile/" + str(record[s_id]) + "/" + record[s_name] + ".xdi", safe="/:"),
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
                        "user_comments" : record[s_comments]
                        }
                    }

                # Pass each individual record to the Validator
                result = dataset_validator.write_record(record_metadata)

                # Check if the Validator accepted the record, and print a message if it didn't
                # If the Validator returns "success" == True, the record was written successfully
                if result["success"] is not True:
                    print("Error:", result["message"], ":", result.get("invalid_metadata", ""))

    if verbose:
        print("Finished converting")


# Optionally, you can have a default call here for testing
# The convert function may not be called in this way, so code here is primarily for testing
if __name__ == "__main__":
    import paths
    convert(True)
