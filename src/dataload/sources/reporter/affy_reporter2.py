# -*- coding: utf-8 -*-
"""
Created on Fri Jan 23 11:33:12 2015

@author: kevin
"""
import os.path
from utils.dataload import (load_start, load_done, tab2dict,
                            dict_apply)
from config import DATA_ARCHIVE_ROOT
DATA_FOLDER = os.path.join(DATA_ARCHIVE_ROOT,'by_resources/reporters')
AFFY_RELEASE = 'na34'
AFFY_FILE_EXTENSION = '.zip'  # or '.gz'
AFFY_DATA_FOLDER = os.path.join(DATA_FOLDER, 'affy', AFFY_RELEASE)
AFFY_ANNOT_FILES = [#human chips
                    {'name': 'HTA-2_0',
                     'file': 'HTA-2_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    {'name': 'HuGene-1_1',
                     'file': 'HuGene-1_1.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    {'name': 'HuGene-2_1',
                     'file': 'HuGene-2_1.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    {'name': 'HuEx-1_0',
                     'file': 'HuEx-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #mouse chips
                    {'name': 'MTA-1_0',
                     'file': 'MTA-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    {'name': 'MoGene-1_1',
                     'file': 'MoGene-1_1.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    {'name': 'MoGene-2_1',
                     'file': 'MoGene-2_1.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    {'name': 'MoEx-1_0',
                     'file': 'MoEx-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #rat chips
                    {'name': 'RaGene-1_1',
                     'file': 'RaGene-1_1.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    {'name': 'RaGene-2_1',
                     'file': 'RaGene-2_1.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    {'name': 'RaEx-1_0',
                     'file': 'RaEx-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Arabidopsis chips
                    {'name': 'AraGene-1_0',
                     'file': 'AraGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Bovine chips
                    {'name': 'BovGene-1_0',
                     'file': 'BovGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Canine chips
                    {'name': 'CanGene-1_0',
                     'file': 'CanGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Chicken chips
                    {'name': 'ChiGene-1_0',
                     'file': 'ChiGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Cynomolgus chips
                    {'name': 'CynGene-1_0',
                     'file': 'CynGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Cynomolgus+Rhesus chips
                    {'name': 'CyRGene-1_0',
                     'file': 'CyRGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Drosophila chips
                    {'name': 'DroGene-1_0',
                     'file': 'DroGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #C. elegans chips
                    {'name': 'EleGene-1_0',
                     'file': 'EleGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Equine chips
                    {'name': 'EquGene-1_0',
                     'file': 'EquGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Feline chips
                    {'name': 'FelGene-1_0',
                     'file': 'FelGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Guinea Pig chips
                    {'name': 'GuiGene-1_0',
                     'file': 'GuiGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Marmoset chips
                    {'name': 'MarGene-1_0',
                     'file': 'MarGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Porcine chips
                    {'name': 'PorGene-1_0',
                     'file': 'PorGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Rabbit chips
                    {'name': 'RabGene-1_0',
                     'file': 'RabGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Rhesus chips
                    {'name': 'RheGene-1_0',
                     'file': 'RheGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Rice (Cn) chips
                    {'name': 'RCnGene-1_0',
                     'file': 'RCnGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Rice (US) chips
                    {'name': 'RUSGene-1_0',
                     'file': 'RUSGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Rice (Jp) chips
                    {'name': 'RJpGene-1_0',
                     'file': 'RJpGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Soybean chips
                    {'name': 'SoyGene-1_0',
                     'file': 'SoyGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                    #Zebrafish chips
                    {'name': 'ZebGene-1_0',
                     'file': 'ZebGene-1_0.%s.annot.csv' + AFFY_FILE_EXTENSION},
                   ]

platform_li = [af['name'] for af in AFFY_ANNOT_FILES]

def _load_affy(df):
    filename = os.path.split(df)[1]
    rawfile, ext = os.path.splitext(filename)
    if ext.lower() == '.zip':
        df = (df, rawfile)
    dd = tab2dict(df, (0, 7), 1, sep=',', header=1,includefn=lambda ld:len(ld)>7 and ld[7]!='---' and ld[7]!='gene_assignment')
    #fix for keys like "472 /// 4863" for mulitple geneids
    gene2affy = {}
    for k in dd:
        kk = k.split('///')
        if len(kk)>1:
            for kkk in kk:
                k4 = kkk.split('//')
                if k4[len(k4)-1].strip() !='---':
                    dict_apply(gene2affy,k4[len(k4)-1].strip(),dd[k])
        else:
            k4 = k.split('//')
            if len(k4)>1:
                if k4[len(k4)-1].strip() !='---':
                    dict_apply(gene2affy,k4[len(k4)-1].strip(),dd[k])
        
    return gene2affy
    
def loaddata():
    affy_d = {}
    for annot in AFFY_ANNOT_FILES:
        name = annot['name']
        DATAFILE = os.path.join(AFFY_DATA_FOLDER, annot['file'] % AFFY_RELEASE)
        load_start(DATAFILE)
        d = _load_affy(DATAFILE)
        affy_d[name] = d
        load_done('[%d]' % len(d))

    return affy_d