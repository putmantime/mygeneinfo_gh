# Copyright [2010-2011] [Chunlei Wu]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import sys
import os
import os.path
import time
from utils.common import ask, safewfile, LogPrint
from config import DATA_ARCHIVE_ROOT

timestamp = time.strftime('%Y%m%d')
#timestamp = '20111114'
DATA_FOLDER=os.path.join(DATA_ARCHIVE_ROOT, 'by_resources/entrez', timestamp)

FILE_LIST = {
    'gene': {'url': 'ftp://ftp.ncbi.nih.gov/gene/DATA/',
             'files':  ['gene_info.gz',
                        'gene2accession.gz',
                        'gene2refseq.gz',
                        'gene2unigene',
                        'gene2go.gz',
                        'gene_history.gz']},

    'refseq': {'url': 'ftp://ftp.ncbi.nih.gov/refseq/',
               'files': ['H_sapiens/mRNA_Prot/human.rna.gbff.gz',
                         'M_musculus/mRNA_Prot/mouse.rna.gbff.gz',
                         'R_norvegicus/mRNA_Prot/rat.rna.gbff.gz',
                         'D_rerio/mRNA_Prot/zebrafish.rna.gbff.gz',
                         'X_tropicalis/mRNA_Prot/frog.rna.gbff.gz']},

    'Homologene': {'url': 'ftp://ftp.ncbi.nih.gov/pub/HomoloGene/current/',
                   'files': ['homologene.data']}
}

def download(path):
    out = []
    orig_path = os.getcwd()
    for subfolder in FILE_LIST:
        filedata = FILE_LIST[subfolder]
        baseurl = filedata['url']
        data_folder = os.path.join(path, subfolder)
        if not os.path.exists(data_folder):
            os.mkdir(data_folder)

        for f in filedata['files']:
            url = baseurl + f
            os.chdir(data_folder)
            filename = os.path.split(f)[1]
            if os.path.exists(filename):
                if ask('Remove existing file "%s"?' % filename) == 'Y':
                    os.remove(filename)
                else:
                    print "Skipped!"
                    continue
            print 'Downloading "%s"...' % f
            cmdline = 'wget %s' % url
            #cmdline = 'axel -a -n 5 %s' % url   #faster than wget using 5 connections
            return_code = os.system(cmdline)
            if return_code == 0:
                print "Success."
            else:
                print "Failed with return code (%s)." % return_code
                out.append((url, return_code))
            print "="*50

    os.chdir(orig_path)
    return out


def parse_gbff(path):
    import glob
    from parse_refseq_gbff import main
    refseq_folder = os.path.join(DATA_FOLDER, 'refseq')
    gbff_files = glob.glob(os.path.join(refseq_folder, '*.rna.gbff.gz'))
    assert len(gbff_files) == 5, 'Missing "*.gbff.gz" files? Found %d (<5):\n%s' % (len(gbff_files), '\n'.join(gbff_files))
    main(refseq_folder)


def main():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    else:
        if not (len(os.listdir(DATA_FOLDER))==0 or ask('DATA_FOLDER (%s) is not empty. Continue?' % DATA_FOLDER)=='Y'):
            sys.exit()

    log_f, logfile = safewfile(os.path.join(DATA_FOLDER, 'entrez_dump.log'))
    sys.stdout = LogPrint(log_f, timestamp=True)

    try:
        download(DATA_FOLDER)
        parse_gbff(DATA_FOLDER)
    finally:
        sys.stdout.close()

if __name__ == '__main__':
    main()
