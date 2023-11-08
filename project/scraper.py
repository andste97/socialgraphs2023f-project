import urllib.request
import urllib.parse
import json
import re
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from tqdm.asyncio import tqdm
import aiohttp  # requires cchardet package


## Util functions ##

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def flatten(l):
    return [item for sublist in l for item in sublist]

## Data download ##

# Category pages

def get_category_pages_query(title, namespace_id=0):
    baseurl = "https://en.wikipedia.org/w/api.php?"
    action = "action=query"
    content = "list=categorymembers"
    dataformat ="format=json&cmlimit=500"
    safe_title = "cmtitle=" + urllib.parse.quote_plus(title)
    cmnamespace = "cmnamespace=" + str(namespace_id)

    query = "{}{}&{}&{}&{}&{}".format(baseurl, action, content, safe_title, cmnamespace, dataformat)

    return query

def handle_category_pages_return(wikitext_json):

    categories_list = wikitext_json["query"]["categorymembers"]

    if "continue" in wikitext_json:
        contin = wikitext_json["continue"]["cmcontinue"]
    else:
        contin = None

    return contin, categories_list

# Title search

def get_wiki_pages_with_prefix_query(prefix, namespace_id=0):
    baseurl = "https://en.wikipedia.org/w/api.php?"
    action = "action=query"
    content = "list=allpages"
    dataformat ="format=json&aplimit=500"
    apprefix = "apprefix=" + urllib.parse.quote_plus(prefix)
    apnamespace = "apnamespace=" + str(namespace_id)

    query = "{}{}&{}&{}&{}&{}".format(baseurl, action, content, apprefix, apnamespace, dataformat)

    return query

def handle_wiki_pages_with_prefix_return(wikitext_json):

    title_list_raw = wikitext_json["query"]["allpages"]

    if len(title_list_raw) > 0:
        title_list = [entry["title"] for entry in title_list_raw]
    else:
        title_list = []

    return None, title_list

# Wiki pages

def get_wiki_data_query(titles):
    # Can handle multiple titles
    if isinstance(titles, list):
        titlestring = "|".join(titles)
    else:
        titlestring = titles

    baseurl = "https://en.wikipedia.org/w/api.php?"
    action = "action=query"
    content = "prop=revisions&rvprop=content&rvslots=*"
    dataformat ="format=json"
    safe_title = "titles=" + urllib.parse.quote_plus(titlestring)
    query = "{}{}&{}&{}&{}".format(baseurl, action, content, safe_title, dataformat)

    return query

def handle_wiki_data_return(wikitext_json):

    pages = wikitext_json["query"]["pages"]

    return None, pages

# HTTP Request handling

def send_urlib_request_sync(query):
    wikiresponse = urllib.request.urlopen(query)
    wikidata = wikiresponse.read()
    wikitext = wikidata.decode('utf-8')
    wikitext_json = json.loads(wikitext)

    return wikitext_json

async def send_urlib_request_async(query, response_handler=None, query_continue_param=None):
    # response_handler callback functions should return a continue indicator as first argument, which will trigger another query if it is not None, and the actual return as a second argument.
    # Will return a list of results if query_continue_param is provided, otherwise just the response.

    results = []
    contin = "initial_run"

    while contin is not None:
        if contin != "initial_run" and query_continue_param is not None:
            curr_query = query + query_continue_param + contin
        else:
            curr_query = query

        async with aiohttp.ClientSession() as session:
            async with session.get(curr_query) as response:
                html = await response.text()
                wikitext_json = json.loads(html)

                if response_handler is None:
                    results = wikitext_json
                    contin = None
                else:
                    contin, curr_results = response_handler(wikitext_json)
                    if query_continue_param is not None:
                        results += curr_results
                    else:
                        results = curr_results
                        contin = None # prevent loops

    return results

async def handle_queries(queries, response_handler=None, query_continue_param=None, tqdm_desc=None):
    coroutines = [send_urlib_request_async(query, response_handler, query_continue_param) for query in queries]
    wikitexts = await tqdm.gather(*coroutines, desc=tqdm_desc)

    return wikitexts

# Parser

def parse_talk_page(page):

    # Does page exist?
    if "revisions" in page:
        content = page["revisions"][0]["slots"]["main"]["*"]  # * from rvslots
        title = page["title"]

        # Normalize whitespace
        content = re.sub(r'[\n\t\ ]+', ' ', content)

        # Retreive links to User: pages
        links = re.findall('\[(User:[^/\]\[\|]+)[\]\|]', content) # Previous: '\[([^\]\[\|:]+)[\]\|]'
        filtered_links = np.unique(links)

        origin_title_list = re.findall('([^/]+).*', title)
        if len(origin_title_list) > 0:
            origin_title = origin_title_list[0]
        else:
            origin_title = title
        
        # Article word count
        #word_count = len(re.findall('\w+', content.lower()))
        #graph.nodes(data=True)[title]["word_count"] = word_count

        # Obsolete Heuristic for Archive pages
        #archive_number = re.findall('\|\s*counter\s*=\s*([^\|\}\s]+)', content)

        return {"origin_title": origin_title, "user_links": filtered_links}
    else:
        return None

def parse_wiki_page(page):

    # Does page exist?
    if "revisions" in page:
        content = page["revisions"][0]["slots"]["main"]["*"]  # * from rvslots
        title = page["title"]

        # Normalize whitespace
        content = re.sub(r'[\n\t\ ]+', ' ', content)

        return {"origin_title": title}
    else:
        return None

# Scraper

async def scrape_wiki(category_titles, verbose=True):
    # Constants
    wiki_api_page_request_limit = 50
    namespace_id_talk = 1


    ## Talk: pages
    # Get pages in category
    category_queries = [get_category_pages_query(category_title, namespace_id_talk) for category_title in category_titles]
    # Send requests
    pages = await handle_queries(category_queries, response_handler=handle_category_pages_return, query_continue_param="&cmcontinue=", tqdm_desc="Fetching " + str(len(category_titles)) + " categories")
    # Handle results
    talk_titles = [r["title"] for page in pages for r in page]
    
    ## Talk: Archive pages
    # Find archive pages
    archive_queries = [get_wiki_pages_with_prefix_query(title.replace("Talk:", "") + "/Archive", namespace_id_talk) for title in talk_titles]
    # Send requests
    archive_titles = await handle_queries(archive_queries, response_handler=handle_wiki_pages_with_prefix_return, tqdm_desc="Fetching " + str(len(talk_titles)) + " page archive titles")
    archive_titles = flatten(archive_titles)

    ## Fetch and parse Talk:
    # List of all pages to gather
    all_titles = talk_titles + archive_titles
    # Split list because of API limits
    split_talk_titles_list = list(chunks(all_titles, wiki_api_page_request_limit))

    # Get wiki Talk: pages
    wiki_page_queries = [get_wiki_data_query(titles) for titles in split_talk_titles_list]
    # Send requests
    talk_pages = await handle_queries(wiki_page_queries, response_handler=handle_wiki_data_return, tqdm_desc="Fetching " + str(len(all_titles)) + " pages")

    # Parse Talk: pages
    talk_data = []
    for sublist in tqdm(talk_pages, desc="Parsing page batches"): # TODO parallelize if possible
        parse_results = [parse_talk_page(page_content) for key, page_content in sublist.items()]
        talk_data += parse_results

    ## Main pages
    wiki_page_titles = [title.replace("Talk:", "") for title in talk_titles]
    # Split list because of API limits
    split_wiki_titles_list = list(chunks(wiki_page_titles, wiki_api_page_request_limit))
    # Get wiki Talk: pages
    wiki_page_queries = [get_wiki_data_query(titles) for titles in split_wiki_titles_list]
    # Send requests
    wiki_pages = await handle_queries(wiki_page_queries, response_handler=handle_wiki_data_return, tqdm_desc="Fetching " + str(len(wiki_page_titles)) + " wiki pages")

    # Parse Talk: pages
    wiki_data = []
    for sublist in tqdm(wiki_pages, desc="Parsing page batches"): # TODO parallelize if possible
        parse_results = [parse_wiki_page(page_content) for key, page_content in sublist.items()]
        wiki_data += parse_results

    # Graph
    page_graph = nx.DiGraph()

    for talk_page_title, wiki_page_title in zip(talk_titles, wiki_page_titles):
        page_graph.add_node(talk_page_title, page_class="talk")
        page_graph.add_node(wiki_page_title, page_class="page")
        page_graph.add_edge(talk_page_title, wiki_page_title)


    count = 0
    # Add User: links to graph
    for page_data in tqdm(talk_data, desc="Creating graph"):
        if page_data is not None:
            if page_data["origin_title"] in page_graph:
                for link in page_data["user_links"]:
                    if link not in page_graph:
                        page_graph.add_node(link, page_class="user")
                    page_graph.add_edge(link, page_data["origin_title"])
                    count += 1
            #else:
            #    print(origin_title) # Talk:HIV for some reason

    print("Total edges: " + str(count))

    infos = {"titles": talk_titles, "archive_titles": archive_titles }

    return page_graph, infos