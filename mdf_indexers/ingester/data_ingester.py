import sys
import json
import os

from tqdm import tqdm

from . import ingest_client
from ..utils import paths
from ..utils.gmeta_utils import format_gmeta, add_namespace

PATH_FEEDSTOCK = paths.get_path(__file__, "feedstock")
globus_url = "https://search.api.globus.org/"

namespaces = {
    "dc." : "http://datacite.org/schema/kernel-3#",
    "mdf-base." : "http://globus.org/publication-schemas/mdf-base/0.1#",
    "mdf-publish." : "http://globus.org/publish-terms/#"
    }
default_namespace = "http://materialsdatafacility.org/#"


def ingest(mdf_source_names,globus_index="globus_search", batch_size=100, verbose=False):
    ''' Ingests feedstock from file.
        Arguments:
            mdf_source_names (str or list of str): Dataset name(s) to ingest.
            batch_size (int): Max size of a single ingest operation. -1 for unlimited. Default 100.
            verbose (bool): Print status messages? Default False.
        '''
    if verbose:
        print("\nStarting ingest of:\n", mdf_source_names, "\nBatch size:", batch_size, "\n")

    globus_client = ingest_client.IngestClient(globus_url, globus_index)

    if type(mdf_source_names) is str:
        mdf_source_names = [mdf_source_names]

    for source_name in mdf_source_names:
        list_ingestables = []
        count_ingestables = 0
        count_batches = 0
        with open(os.path.join(PATH_FEEDSTOCK, source_name+"_all.json"), 'r') as feedstock:
            for json_record in tqdm(feedstock, desc="Ingesting " + source_name, disable= not verbose):
                record = format_gmeta(json.loads(json_record))
                record["content"] = add_namespace(record["content"])
                list_ingestables.append(record)
                count_ingestables += 1

                if batch_size > 0 and len(list_ingestables) >= batch_size:
                    globus_client.ingest(format_gmeta(list_ingestables))
                    list_ingestables.clear()
                    count_batches += 1

        # Check for partial batch to ingest
        if list_ingestables:
            globus_client.ingest(format_gmeta(list_ingestables))
            list_ingestables.clear()
            count_batches += 1

        if verbose:
            print("Ingested", count_ingestables, "records in", count_batches, "batches, from", source_name, "\n")

'''
    'ab_initio_solute_database',
    'amcs',
    'cip',
    'core_mof',
    'cp_complexes',
    'cxidb',
    'doak_strain_energies',
    'fe_cr_al_oxidation',
    'gw100',
    'gw_soc81',
    'hopv',
    'jcap_benchmarking_db',
    'jcap_xps_spectral_db',
    'khazana_polymer',
    'khazana_vasp',
    'materials_commons',
    'matin',
    'nanomine',
    'nist_atom_weight_iso_comp',
    'nist_heat_transmission',
    'nist_ip',
    'nist_janaf',
    'nist_mml',
    'nist_th_ar_lamp_spectrum',
    'nist_xps_db',
    'nist_xray_tran_en_db',
    'nrel_pv',
    'oqmd',
    'oxygen_interstitials_deformation',
    'pppdb',
    'quinary_alloys',
    'qm_mdt_c',
    'sluschi',
    'strain_effects_oxygen',
    'ti_o_fitting_db',
    'ti_o_meam_model',
    'trinkle_elastic_fe_bcc',
    'uci_steel_annealing', #Metadata only
    'xafs_sl'
'''

