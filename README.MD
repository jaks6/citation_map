# Create a Citation Graph based on Simplistic Text Analysis

_Inspired by A.R. Siders' R Script from this [ResearchGate question]( https://www.researchgate.net/post/Is_there_any_recommended_software_to_visualise_articles_papers_references_when_conducting_a_systematic_review_or_meta-analysis )_

_Based on dpapathanasiou's [example script for pdfminer](https://github.com/dpapathanasiou/pdfminer-layout-scanner)_

## Takes Zotero .CSV Article collections and creates Gephi-compatible files for Graph Edges and Nodes based on citations


![screenshot]( https://github.com/jaks6/citation_map/blob/master/screenshot_map.png )

### Principle:
* Let A be a set of known articles
* For any a in A, let _title_a_ be its title, and _text_a_ be its text content
* For some x in A and y in A, x!=y:
    * cites(x,y) is true if _title_y_ appears in _text_x_

For the above to work, we do some text normalization (removing punctuation, whitespace, special characters) and assume that
the title_y would only appear in text_x if it appears in the references section...

### Usage:
1. Export list of articles as .csv from Zotero, (articles should have File attachments)
2. Run `analyze_papers.py zotero_file.csv`
3. Script should produce two files: Edges_titles.csv and Nodes_titles.csv in folder "gephi"
4. Load them into [Gephi](https://gephi.org) with "Load Spreadsheet"


## Notes
* Tested with Python3
* Uses the library [pdfminer](https://pypi.org/project/pdfminer/)
* You can specify number of processes the script uses to parse the PDFs with parameter --processes (default value is 4)



