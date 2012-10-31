mkdir lib
pushd lib
wget http://lingpipe-download.s3.amazonaws.com/lingpipe-4.1.0.jar
wget http://nlp.stanford.edu/software/stanford-corenlp-2012-07-09.tgz
tar xvzf stanford-corenlp-2012-07-09.tgz
mv stanford-corenlp-2012-07-09/*jar .
rm -rf stanford-corenlp-2012-07-09
rm stanford-corenlp-2012-07-09-javadoc.jar
rm stanford-corenlp-2012-07-09.tgz
popd
