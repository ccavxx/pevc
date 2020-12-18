# PEVC

This is a python project to scrape and show the co-investment networks of PE/VC investors in China from 2010 to 2019.


## Structure

The directory `/scripts` includes scripts for data scraping and processing. The results are stored in `/data`.

The directory `/gephi` contains Gephi files for data visualization. [Gephi](https://gephi.org/) is a powerful software to visualize network-structured data. An interactive network HTML page is output to `gephi/interactive_networks`.

## Steps

This project has three steps: data scraping, data processing and data visualization.

### Data Scraping

The records of investment events are scraped from `cyzone.cn` (创业邦), a Chinese VC news platform and database. Similar sites include `36kr.com` (36氪), `itjuzi.com` (IT桔子), etc. The script `scripts/scraping.py` acquires the following information
- investee name and id
- investor name(s) and id(s)
- amount of financing
- series of financing
- industry
- date

It scrapes from 2010 to 2019, obtains 79325 investment records, and outputs them to `data/events.csv`.

There are two issues during scraping
- CAPTCHA
  - This site uses slider CAPTCHA. This is solved by the `CaptchaSolver` class in `scraping.py` using packages `selenium`, `open-cv` and `pillow`.
- Font-face Coding
  - Some websites use self-defined fonts to hide important information in the HTML content, making it harder to parse, such as the amount of financing and the series of financing in our case. To tackle this, we use packages `fontTools`, `io.BytesIO`, and `base64` to decode the special fonts, which are wrapped in the `Decoder` class.

Finally, to improve the speed of scraping, we use `multiprocessing` to exploit all cores in the CPU. This turns out to be 10x faster.

### Data Processing

In the co-investment networks, nodes are investors and edges are co-investment events. We then obtain the nodes list `investors_unique.csv` and the edges list `investor_paris.csv` from the investment events records `events.csv` by running the script `data_processing.py`. This step mainly uses `pandas`.

We have roughly 10,000 investors in the database during the past 10 years. But most of them only invested once. For visualization purpose, we select the active investors who made at least 21 investments in the past 10 years. This results in a filtered nodes list of about 500 investors.

Some Chineses information (names, amount, etc) are translated to English using `dictionary.csv`.

Eventually, we change the column names according to the requirements by Gephi, and output `gephi/investors_gephi.csv` and `gephi/investor_pairs_gephi.csv`.


### Data Visualization

We import the nodes list and the edges list to Gephi, run algorithms for appropriate visualization, and output an HTML file of [interactive networks](https://dennissxz.github.io/pevc/gephi/interactive_networks/).

<div  align="center">    
<img src="gephi/nw_pic.png" width = "80%" alt="network_static" align=center />
</div>
