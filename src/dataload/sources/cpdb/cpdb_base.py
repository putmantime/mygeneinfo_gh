import json
import os.path
from utils.dataload import (load_start, load_done)
from dataload import get_data_folder

DATA_FOLDER = get_data_folder('cpdb')

def load_cpdb():
        
    print('DATA_FOLDER: '+ DATA_FOLDER)
    DATA_FILES = []
    DATA_FILES.append(os.path.join(DATA_FOLDER, 'CPDB_pathways_genes_mouse.tab'))
    DATA_FILES.append(os.path.join(DATA_FOLDER, 'CPDB_pathways_genes_yeast.tab'))
    DATA_FILES.append(os.path.join(DATA_FOLDER, 'CPDB_pathways_genes_human.tab'))
    arr = {}
    for DATA_FILE in DATA_FILES:
        load_start(DATA_FILE)
        f= open(DATA_FILE,"r")
        lines = f.readlines()
        for line in lines:
            line = line.rstrip('\n')
            cols = line.split("\t")
            genes = cols[len(cols)-1].split(",")
            for gene in genes:
                if gene != "entrez_gene_ids" and gene in arr.keys():
                    if cols[len(cols)-2] not in arr[gene]['pathway'].keys():
                        arr[gene]['pathway'][cols[len(cols)-2].lower()]={'name':''}
                    arr[gene]['pathway'][cols[len(cols)-2].lower()]['name'] = cols[len(cols)-4]
                    if cols[len(cols)-3] != "None":
                        if cols[len(cols)-2].lower() == "kegg":
                            arr[gene]['pathway'][cols[len(cols)-2].lower()]['id'] = cols[len(cols)-3].replace("path:","")
                        else :
                            arr[gene]['pathway'][cols[len(cols)-2].lower()]['id'] = cols[len(cols)-3]
                else:
                    if cols[len(cols)-3] != "None":
                        arr[gene]= {'pathway':{cols[len(cols)-2].lower():{'name':cols[len(cols)-4], 'id': cols[len(cols)-3].replace("path:","")}}}

        load_done('[%d]' % len(arr))

    return arr




