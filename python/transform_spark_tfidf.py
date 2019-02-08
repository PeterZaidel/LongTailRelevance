
import sys
#/media/peter/DATA1/Data/InfoSearch/hw5/transfomed_mr_features/QD/part--r-00000
def transform_and_save(filename:str):
    idx = filename.rfind('/')
    fdir = filename[0:idx]
    dir_name = fdir[fdir.rfind('/')+1:]

    transfomed_file = fdir + '/transfomed.txt'

    fin = open(filename, 'r')
    fout = open(transfomed_file, 'w')

    for l in fin:
        args = l.strip().split('\t')
        res = args[0] + '\t' + args[1] + '\t' + 'tf_idf_' + dir_name + ":" + args[2]
        fout.write(res + '\n')

    fin.close()
    fout.close()



def main(args):
    for fn in args:
        transform_and_save(fn)

import glob
if __name__ == '__main__':


    args = glob.iglob('/media/peter/DATA1/Data/InfoSearch/hw5/FULL/spark-res/tf-idf/scores/*/part-00000', recursive=True)

    main(args)