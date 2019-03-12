#!/usr/bin/python
import argparse
import csv
import re
import string
import sys, os, time

from functools import partial
from multiprocessing import Pool

import errno
import layout_scanner

# Zotero CSV Column indices
YEAR_I = 2
AUTHOR_I = 3
TITLE_I = 4
FILE_I = 37

DEFAULT_OUTPUT_CSV_NAME = "titles.csv"
DEFAULT_OUTPUT_DELIMITER = "\t"

used_filenames = []
graph = []


def pdf_to_text_list(file_loc):
    """
     Extracts text (string) of PDF file contents. Images, figures are ignored.

    :param str file_loc: Path to .PDF document on local disk
    :return: The last 10 pages of the PDF document as string text, a list of strings
    :rtype: list
    """
    # Read PDF pages as text
    pages = layout_scanner.get_pages(file_loc, images_folder=None)  # you can try os.path.abspath("output/imgs")
    try:
        page_len = len(pages)
    except TypeError:
        print >> sys.stderr, "[!] Issue parsing PDF file"
        return ""

    print "\t-- Pages:", page_len
    # Take only last 10 pages (We assume references never take more) TODO:HARDCODE
    pages = pages[-10:]

    return pages


def get_pretty_filename(metadata):
    fixed_title = re.sub('[^A-Za-z0-9]+', '', "_".join(metadata["title"].split(" ")[:10]))
    authors = metadata["author"].split(";")
    author_2nd = ""
    if len(authors) > 2:
        author_2nd = "et al."
    elif len(authors) == 2:
        author_2nd = "& " + authors[1].split(",")[0]
    author_1st = authors[0].split(",")[0]
    txt_filename = "%s %s %s" % (author_1st, author_2nd, metadata["year"])
    if txt_filename in used_filenames:
        txt_filename = txt_filename + fixed_title[:-20]
    used_filenames.append(txt_filename)
    return txt_filename

def create_missing_dirs(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def read_titles(zotero_csv):
    titles = {}
    with open(zotero_csv, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(csvfile)  # Skip header
        for r in reader:
            titles[pre_process(r[TITLE_I])] = \
                {'title': r[TITLE_I],
                 'author': r[AUTHOR_I],
                 'file': r[FILE_I],
                 'year': r[YEAR_I]}
    return titles


def process_pdf(metadata, write_to_disk=False):
    """
    Reads text from PDF file specified in the CSV lines' file column, optionally saves it to .txt on disk

    :param dict csv_line: the CSV line to process
    :param bool write_to_disk: whether the text will be written to disk also
    :return: a tuple (bool, text), where bool indicates whether text was extracted successfully,
        text will be either the pdf text contents or the error message
    :rtype: tuple
    """
    print " ".join(metadata['author'].split(";")[:3]), metadata['year'], metadata['title'][:32], metadata['file']
    if len(metadata['file']) < 1:
        return False, "Missing Zotero file"

    pages = pdf_to_text_list(metadata['file'])

    if write_to_disk:  # Kind of deprecated, this was used by the R script of A.R. Siders
        output_filename = get_pretty_filename(metadata)
        paper_txt_filename = args.txts_dir + os.sep + output_filename + '.txt'
        create_missing_dirs(paper_txt_filename)
        with open(paper_txt_filename, 'w') as outfile:
            for p in pages:
                print >> outfile, p
            outfile.close()

    all_pages = "\n".join(pages)

    return len(all_pages) > 0, all_pages


def find_citations(paper_text, all_titles, metadata):
    cited_ids = []
    # Check which titles this paper cited:
    fixed_paper_title = pre_process(metadata["title"])
    fixed_text = pre_process(paper_text)

    # remove whitespace
    for title in all_titles:
        if (title != fixed_paper_title) and \
                (title.replace(' ', '') in fixed_text.replace(' ', '')):  # Stripping whitespace!
            print "\t\t-- citation found:", title
            cited_ids.append(title)
            # graph.append([fixed_paper_title, title])
    return cited_ids


def article_worker((title, metadata), all_titles):
    t0 = time.time()

    pdf_result, text = process_pdf(metadata)
    t1 = time.time()
    print "\t-- processed pdf in ", (t1 - t0), "seconds"

    cited_papers = []
    if pdf_result:
        cited_papers = find_citations(text, all_titles, metadata)
        t2 = time.time()
        print "\t-- processed text cites in ", (t2 - t1), "seconds"

    return title, pdf_result, text, cited_papers


def pre_process(text):
    # to lowercase
    text = text.lower()
    # remove punctuation
    text = text.translate(string.maketrans('', ''), string.punctuation)
    # remove linebreaks
    # text = re.sub(r"(?<=[a-z])\r?\n", " ", text)
    text = text.replace('\r', '').replace('\n', '')
    # remove numbers
    text = re.sub(r'\d+', '', text)
    # remove whitespace
    text = " ".join(re.findall(r'[a-z]+', text))

    return text


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
                                     'Extract text from PDF files whose locations are given by a Zotero CSV file')
    parser.add_argument('zotero_csv', type=str, help='the Zotero exported CSV file of papers')
    parser.add_argument('--gephi_dir', default="gephi", type=str,
                        help='Output dir for gephi Edges and Nodes files (default: "gephi")')
    parser.add_argument('--txts_dir', default="papers", type=str,
                        help='Output dir for article txt files (default: "papers")')
    parser.add_argument('--out_csv', default=DEFAULT_OUTPUT_CSV_NAME, type=str,
                        help='Output csv filename (default: ' + DEFAULT_OUTPUT_CSV_NAME + ')')
    parser.add_argument('--delimiter', default=DEFAULT_OUTPUT_DELIMITER, type=str,
                        help='Output csv delimiter  (default: ' + DEFAULT_OUTPUT_CSV_NAME + ')')

    args = parser.parse_args()
    OUTPUT_CSV_NAME = args.out_csv
    OUTPUT_GEPHI_DIR = args.gephi_dir
    OUTPUT_DELIMITER = args.delimiter

    error_documents = []

    # First, just get the titles in the csv
    titles_dict = read_titles(args.zotero_csv)
    title_ids = titles_dict.keys()

    # Now process the PDFs
    pool_start_time = time.time()

    pool = Pool(processes=4)  # start 3 worker processes

    list_worker = partial(article_worker, all_titles=title_ids)
    result = pool.map(list_worker, titles_dict.items(), chunksize=1)

    for title, pdf_result, text, cited_papers in result:
        if pdf_result:
            for paper in cited_papers:
                graph.append([title, paper])

        else:
            error_documents.append([title, text])
    total_time = time.time() - pool_start_time

    # Print finish report, show failed documents
    print "\n---- Finished -----\n" \
          "Processed ", len(title_ids), " papers in  ", total_time, "seconds"
    print "%s documents were not extracted due to errors:" % len(error_documents)
    for i, (doc_id, reason) in enumerate(error_documents):
        doc = titles_dict[doc_id]
        print ("%s." % i), doc["author"], doc["year"], doc["title"], doc["file"]
        print "\t--", reason

    # Write Graph Edges to csv
    with open(OUTPUT_GEPHI_DIR + os.sep + "Edges_" + OUTPUT_CSV_NAME, "a") as graph_csv:
        # Header
        graph_csv.write(OUTPUT_DELIMITER.join(["Source", "Target", "Weight"]) + "\n")
        for (src, target) in graph:
            graph_csv.write(OUTPUT_DELIMITER.join([src, target, "1"]) + "\n")

    # Write Graph Nodes with Labels to csv
    with open(OUTPUT_GEPHI_DIR + os.sep + "Nodes_" + OUTPUT_CSV_NAME, "a") as nodes_csv:
        # Header
        nodes_csv.write(OUTPUT_DELIMITER.join(["Id", "Label", "Author", "PrettyName"]) + "\n")
        for title in title_ids:
            metadata = titles_dict[title]
            nodes_csv.write(OUTPUT_DELIMITER.join(
                [title, metadata["title"], metadata["author"], get_pretty_filename(metadata)]) + "\n")
