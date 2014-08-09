# Copyright [2010-2013] [Chunlei Wu]
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
src_path = os.path.split(os.path.split(os.path.split(os.path.abspath(__file__))[0])[0])[0]
sys.path.append(src_path)
from utils.common import ask, safewfile, LogPrint, timesofar
from utils.mongo import get_src_dump
from config import DATA_ARCHIVE_ROOT

timestamp = time.strftime('%Y%m%d')
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
                         'X_tropicalis/mRNA_Prot/frog.rna.gbff.gz',
                         'B_taurus/mRNA_Prot/cow.rna.gbff.gz',
                         ]},

    'Homologene': {'url': 'ftp://ftp.ncbi.nih.gov/pub/HomoloGene/current/',
                   'files': ['homologene.data']},

    'generif': {'url': 'ftp://ftp.ncbi.nih.gov/gene/GeneRIF/',
                'files': ['generifs_basic.gz']},

}


def _get_ascp_cmdline(url):
    '''
    ~/opt/aspera_connect/bin/ascp -QT -l640M -i  ~/opt/aspera_connect/etc/asperaweb_id_dsa.putty anonftp@ftp.ncbi.nih.gov:/refseq/H_sapiens/mRNA_Prot/human.rna.gbff.gz .
    '''
    cmd = '~/opt/aspera_connect/bin/ascp -QT -l640M -i  ~/opt/aspera_connect/etc/asperaweb_id_dsa.putty anonftp@'
    _url = url[6:]   # remove 'ftp://'
    _url = _url.replace('.gov/', '.gov:/')
    cmd = cmd + _url + ' .'
    return cmd


def download(path, no_confirm=False):
    out = []
    orig_path = os.getcwd()
    try:
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
                    if no_confirm or ask('Remove existing file "%s"?' % filename) == 'Y':
                        os.remove(filename)
                    else:
                        print "Skipped!"
                        continue
                print 'Downloading "%s"...' % f
                #cmdline = 'wget %s' % url
                #cmdline = 'axel -a -n 5 %s' % url   #faster than wget using 5 connections
                cmdline = _get_ascp_cmdline(url)
                return_code = os.system(cmdline)
                #return_code = 0;print cmdline    #for testing
                if return_code == 0:
                    print "Success."
                else:
                    print "Failed with return code (%s)." % return_code
                    out.append((url, return_code))
                print "="*50
    finally:
        os.chdir(orig_path)

    return out


def parse_gbff(path):
    import glob
    from parse_refseq_gbff import main
    refseq_folder = os.path.join(path, 'refseq')
    gbff_files = glob.glob(os.path.join(refseq_folder, '*.rna.gbff.gz'))
    assert len(gbff_files) == 6, 'Missing "*.gbff.gz" files? Found %d (<6):\n%s' % (len(gbff_files), '\n'.join(gbff_files))
    main(refseq_folder)


def main():
    no_confirm = True   #set it to True for running this script automatically without intervention.

    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    else:
        if not (no_confirm or len(os.listdir(DATA_FOLDER))==0 or ask('DATA_FOLDER (%s) is not empty. Continue?' % DATA_FOLDER)=='Y'):
            sys.exit()

    log_f, logfile = safewfile(os.path.join(DATA_FOLDER, 'entrez_dump.log'), prompt=(not no_confirm), default='O')
    sys.stdout = LogPrint(log_f, timestamp=True)
    sys.stderr = sys.stdout

    #mark the download starts
    src_dump = get_src_dump()
    doc = {'_id': 'entrez',
           'timestamp': timestamp,
           'data_folder': DATA_FOLDER,
           'logfile':logfile,
           'status': 'downloading'}
    src_dump.save(doc)
    t0 = time.time()
    try:
        download(DATA_FOLDER, no_confirm=no_confirm)
        t_download = timesofar(t0)
        t1 = time.time()
        #mark parsing starts
        src_dump.update({'_id': 'entrez'}, {'$set': {'status': 'parsing'}})
        parse_gbff(DATA_FOLDER)
        t_parsing = timesofar(t1)
        t_total = timesofar(t0)
    finally:
        sys.stdout.close()

    #mark the download finished successfully
    _updates = {
                'status': 'success',
                'time': {'download': t_download,
                         'parsing': t_parsing,
                         'total': t_total},
                'pending_to_upload': True    # a flag to trigger data uploading
                }

    src_dump.update({'_id': 'entrez'}, {'$set': _updates})

if __name__ == '__main__':
    main()
