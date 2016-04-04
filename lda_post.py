import numpy as np
import csv
import linecache
import sys
import argparse



# Input files: doc.dat is list of unique docs, words.dat is list of unique words
# final.gamma is output of topic distributions per doc, final.beta is output of
# distribution of words per topic (originally 20x#words, transform this to end
# up #wordsx20)


def process_topics(doc_file_name, topic_file_name, doc_results_name):

    with open(topic_file_name, 'r') as topic_file:

        # Generate file with correctly formatted topic info per document

        topics_reader = csv.reader(topic_file, delimiter=' ', quotechar='"')

        print "Generating topic distribution per doc..."
        row_count = 1
        doc_results = open(doc_results_name, "w")
        for row in topics_reader:
            topic_distribution = np.asarray(row, dtype=np.float64)
            total_t = sum(topic_distribution)

            if total_t > 0:
                norm_t = map(str, [ti / total_t for ti in topic_distribution])
                norm_t = ' '.join(norm_t)
            else:
                norm_t = ' '.join(['0.0' for ti in topic_distribution])
            doc = linecache.getline(doc_file_name, row_count).split(',')[1].replace('\n', '')

            line = str("%s,%s\n" % (doc, norm_t)).encode('ascii')
            doc_results.write(line)

            if row_count % 1000000 == 0:
                print row_count, "rows processed"
            row_count += 1

    print "Saved topic distribution in " + doc_results_name


    return


def process_words(word_prob_file_name, word_file_name, wresults_name):
    # Generate word file
    words = np.loadtxt(word_prob_file_name, np.float64)
    word_list = np.loadtxt(word_file_name, np.str, delimiter=",")
    print word_list.shape[0]
    print words.shape

    print "Generating probability of word given topic, p(w|z)..."

    # Put words in correct format
    wl_arr = np.empty([word_list.shape[0] + 1, 1], dtype="S20")
    row = 0
    for wl in word_list:
        wl = '_'.join(wl[1:])
        wl_arr[row] = wl
        row += 1
    wl_arr[row] = "0_0_0_0_0"


    # Normalize p(w|z) and flip so each line => word instead of each line => topic
    p_wgz = np.empty([words.shape[1], words.shape[0]], dtype=np.float64)
    col = 0
    for w in words:
        raw_w = [np.exp(wi) for wi in w]
        total_w = sum(raw_w)
        norm_w = [rwi / total_w for rwi in raw_w]
        p_wgz[:, col] = norm_w
        col += 1



    # Combine word list and probability array, write file
    # p_arr = np.append(wl_arr, p_arr, axis=1)
    print "Writing p(w|z) to pword_given_topic.csv"
    # np.savetxt('pword_given_topic.csv', p_arr, delimiter = ",", fmt="%s")

    wresults = open(wresults_name, "w")

    # np.savetxt(f, qties,fmt="%15u")
    # fmt ='%1u'
    fmt = ['%s '] * 19 + ['%s\n']
    format = ''.join(fmt)
    # print format
    for j in xrange(0, len(p_wgz)):
        line1 = str("%s," % (wl_arr[j][0])).encode('ascii')
        wresults.write(line1)
        line = str(format % (tuple(p_wgz[j]))).encode('ascii')
        wresults.write(line)


    return

if __name__ == "__main__":
    # Echo command line...
    sys.stderr.write("{ARGS}\n".format(ARGS=" ".join(sys.argv)))
    DOC_STRING = """Run it after LDA."""

    parser = argparse.ArgumentParser(description=DOC_STRING)

    parser.add_argument('rpath')
    parser.add_argument("-skip_topics", "--skip_topics", action='store_true',
                        help="Skip topic processing.")

    parser.add_argument("--skip_words", "--skip_words", action='store_true', help="Skip word processing.")
    args = parser.parse_args()


    print "+++++++++++++++++++"
    print "modules loaded"
    print "+++++++++++++++++++"

    if (not args.skip_topics):
        process_topics(doc_file_name=args.rpath + 'doc.dat',
                       topic_file_name=args.rpath + 'final.gamma',
                       doc_results_name=args.rpath + 'doc_results.csv')

    if (not args.skip_words):
        process_words(word_prob_file_name=args.rpath + 'final.beta',
                      word_file_name=args.rpath + 'words.dat',
                      wresults_name=args.rpath + 'word_results.csv')