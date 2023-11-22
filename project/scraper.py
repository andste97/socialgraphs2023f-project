import urllib.request
import urllib.parse
import json
import re
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from tqdm.asyncio import tqdm
import aiohttp  # requires cchardet package
import asyncio
import os
import errno
from collections import ChainMap, Counter


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
    dataformat = "format=json&cmlimit=500"
    safe_title = "cmtitle=" + urllib.parse.quote_plus(title)
    cmnamespace = "cmnamespace=" + str(namespace_id)

    query = "{}{}&{}&{}&{}&{}".format(baseurl, action, content, safe_title, cmnamespace, dataformat)

    query_info = {"query": query, "name": None}

    return query_info

def handle_category_pages_return(wikitext_json):

    categories_list = wikitext_json["query"]["categorymembers"]

    if "continue" in wikitext_json:
        contin = "&cmcontinue=" +  wikitext_json["continue"]["cmcontinue"]
    else:
        contin = None

    return categories_list, contin

# Title search

def get_wiki_pages_with_prefix_query(prefix, namespace_id=0):
    baseurl = "https://en.wikipedia.org/w/api.php?"
    action = "action=query"
    content = "list=allpages"
    dataformat = "format=json&aplimit=500"
    apprefix = "apprefix=" + urllib.parse.quote_plus(prefix)
    apnamespace = "apnamespace=" + str(namespace_id)

    query = "{}{}&{}&{}&{}&{}".format(baseurl, action, content, apprefix, apnamespace, dataformat)

    query_info = {"query": query, "name": None}

    return query_info

def handle_wiki_pages_with_prefix_return(wikitext_json):

    title_list_raw = wikitext_json["query"]["allpages"]

    if len(title_list_raw) > 0:
        title_list = [entry["title"] for entry in title_list_raw]
    else:
        title_list = []

    return title_list, None

# Wiki pages

def get_wiki_data_query(titles):
    # Can handle up to 50 titles
    if isinstance(titles, list):
        titlestring = "|".join(titles)
    else:
        titlestring = titles

    baseurl = "https://en.wikipedia.org/w/api.php?"
    action = "action=query"
    content = "prop=revisions&rvprop=content&rvslots=*"
    dataformat = "format=json"
    safe_title = "titles=" + urllib.parse.quote_plus(titlestring)
    query = "{}{}&{}&{}&{}".format(baseurl, action, content, safe_title, dataformat)

    query_info = {"query": query, "name": None}

    return query_info

def handle_wiki_data_return(wikitext_json):

    pages = wikitext_json["query"]["pages"]

    return pages, None

# Wiki page revisions

def get_wiki_page_revisions_query(title):

    baseurl = "https://en.wikipedia.org/w/api.php?"
    action = "action=query"
    content = "prop=revisions&rvprop=timestamp|user|comment|size&rvslots=*&rvlimit=500"
    dataformat = "format=json"
    safe_title = "titles=" + urllib.parse.quote_plus(title)
    query = "{}{}&{}&{}&{}".format(baseurl, action, content, safe_title, dataformat)

    query_info = {"query": query, "name": title}

    return query_info

def handle_wiki_page_revisions_return(wikitext_json):

    revisions = wikitext_json["query"]["pages"][next(iter(wikitext_json["query"]["pages"]))]["revisions"]

    if "continue" in wikitext_json:
        contin = "&rvcontinue=" + wikitext_json["continue"]["rvcontinue"]
    else:
        contin = None

    return revisions, contin

# User edits

def get_user_edits_query(users):
    # Can handle up to 50 users
    if isinstance(users, list):
        userstring = "|".join([str(user) for user in users])
    else:
        userstring = users

    baseurl = "https://en.wikipedia.org/w/api.php?"
    action = "action=query"
    content = "list=users&usprop=editcount"
    dataformat = "format=json"
    safe_user = "ususers=" + urllib.parse.quote_plus(str(userstring))
    query = "{}{}&{}&{}&{}".format(baseurl, action, content, safe_user, dataformat)

    query_info = {"query": query, "name": None}

    return query_info

def handle_user_edits_return(wikitext_json):

    users = wikitext_json["query"]["users"]

    return users, None

# HTTP Request handling

def send_urlib_request_sync(query):
    wikiresponse = urllib.request.urlopen(query)
    wikidata = wikiresponse.read()
    wikitext = wikidata.decode('utf-8')
    wikitext_json = json.loads(wikitext)

    return wikitext_json

async def send_urlib_request_async(query_info, response_handler):
    # response_handler callback functions should return a continue indicator as first argument, which will trigger another query if it is not None, and the actual return as a second argument.
    # Will return a list of results if query_continue_param is provided, otherwise just the response.

    results = []
    request_count = 0
    contin = None

    query = query_info["query"]

    while contin is not None or request_count == 0:
        request_count += 1

        if contin is not None:
            curr_query = query + contin
        else:
            curr_query = query

        async with aiohttp.ClientSession(timeout=aiohttp. ClientTimeout(total=30)) as session:
            async with session.get(curr_query) as response:
                try:
                    html = await response.text()

                    wikitext_json = json.loads(html)

                    curr_results, contin = response_handler(wikitext_json)

                    if request_count == 1:
                        results = curr_results
                    else:
                        if type(curr_results) is list:
                            results.extend(curr_results)
                        elif type(curr_results) is dict:
                            results |= (curr_results)
                        else:
                            print("Can't handle type " + str(type(curr_results)))
                except:
                    error = 0 # stub

    # Option to turn unnamed list into dict in case of undistinguishable information, configure in query generator
    if query_info["name"] is not None:
        results = {query_info["name"]: results}

    return results

async def urlib_request_worker(queue, session, results_p, pbar, response_handler):
    # Worker for processing multiple queries
    while True:
        query_info = await queue.get()
        results_p.append(await send_urlib_request_async(query_info, response_handler))
        pbar.update(1)
        queue.task_done()

async def handle_queries(queries, response_handler, tqdm_desc=None):

    N_MAX_WORKERS = 200

    if len(queries) <= N_MAX_WORKERS:
        coroutines = [send_urlib_request_async(query_info, response_handler) for query_info in queries]
        wikitexts = await tqdm.gather(*coroutines, desc=tqdm_desc)

        return wikitexts
    else:
        N_WORKERS = 50
        queue = asyncio.Queue()
        results_p = []

        async def async_query_generator():
            for i in queries:
                yield i

        async with aiohttp.ClientSession() as session:

            pbar = tqdm(total=len(queries), desc=tqdm_desc, mininterval=0.2)

            workers = [asyncio.create_task(urlib_request_worker(queue, session, results_p, pbar, response_handler)) for _ in range(N_WORKERS)]
            
            async for query_info in async_query_generator():
                await queue.put(query_info)
            
            # Wait for tasks to finish
            await queue.join()

        # Finished
        for worker in workers:
            worker.cancel()

        return results_p

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

# This is mostly stub
def parse_article_page(page):

    # Does page exist?
    if "revisions" in page:
        content = page["revisions"][0]["slots"]["main"]["*"]  # * from rvslots
        title = page["title"]

        # Normalize whitespace
        content = re.sub(r'[\n\t\ ]+', ' ', content)

        return {"origin_title": title}
    else:
        return None


def parse_page_revisions(revision_list):

    #reverts = {k: [edit for edit in v if "comment" in edit and "revert" in edit["comment"].lower()] for k, v in infos["revision_dict"].items()}

    # count user edits
    #dict(Counter([revision["user"] for revision in revision_list if "user" in revision]))

    return {"edit_war_score": 0}

# Save pages to disk

def save_page(page):
    if "revisions" in page:
        content = page["revisions"][0]["slots"]["main"]["*"]  # * from rvslots
        title = page["title"]

        filename = "./page_contents/" + title +".txt"

         # create directories if they do not exist
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        with open(filename, "+w") as file:
            file.write(content)

# Getting plaintext wiki article pages

def get_plaintext_wiki_data_query(title):
    baseurl = "https://en.wikipedia.org/w/api.php?"
    action = "action=query"
    title = "titles=" + urllib.parse.quote_plus(title)
    content = "prop=extracts"
    exlimit = "exlimit=1"
    explaintext = "explaintext=1"
    dataformat ="format=json"

    query = "{}{}&{}&{}&{}&{}&{}".format(baseurl, action, content, exlimit, explaintext, dataformat, title)

    query_info = {"query": query, "name": None}

    return query_info

# Save pages to disk
def write_file_to_folder(filepath, content):
    # filepath = filepath.replace("/", '-')
    # create directories if they do not exist
    if not os.path.exists(os.path.dirname(filepath)):
        try:
            os.makedirs(os.path.dirname(filepath))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(filepath, "+w") as file:
        file.write(content)

def save_talk_page(page):
    if "revisions" in page:
        content = page["revisions"][0]["slots"]["main"]["*"]  # * from rvslots
        title = page["title"]

        filepath = "./page_contents/" + title +".txt"
        write_file_to_folder(filepath, content)

def save_article_plaintext(page):
    title = page["title"]
    content = page["extract"]

    filepath = "./article_pages_plaintext/" + title +".txt"

    write_file_to_folder(filepath, content)

# Scraper

async def scrape_wiki(category_titles, verbose=True):
    # Constants
    wiki_api_page_request_limit = 50
    namespace_id_talk = 1

    ## Talk: pages
    # Get pages in category
    category_queries = [get_category_pages_query(category_title, namespace_id_talk) for category_title in category_titles]
    # Send requests
    pages = await handle_queries(category_queries, response_handler=handle_category_pages_return, tqdm_desc="Fetching " + str(len(category_titles)) + " categories")
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
    talk_page_queries = [get_wiki_data_query(titles) for titles in split_talk_titles_list]
    # Send requests
    talk_pages = await handle_queries(talk_page_queries, response_handler=handle_wiki_data_return, tqdm_desc="Fetching " + str(len(all_titles)) + " talk pages")
    # Parse Talk: pages
    talk_data = []
    for sublist in tqdm(talk_pages, desc="Parsing talk page batches", mininterval=0.5):
        parse_results = [parse_talk_page(page_content) for key, page_content in sublist.items() if type(sublist) == dict]
        talk_data += parse_results

    # Save talk page Data
    for sublist in tqdm(talk_pages, desc="Writing talk page batches to disk", mininterval=0.5):
        [save_talk_page(page_content) for _, page_content in sublist.items()]

    ## Article pages
    article_page_titles = [title.replace("Talk:", "") for title in talk_titles]
    # Split list because of API limits
    split_article_titles_list = list(chunks(article_page_titles, wiki_api_page_request_limit))
    # Get wiki Talk: pages
    article_page_queries = [get_wiki_data_query(titles) for titles in split_article_titles_list]
    # Send requests
    article_pages = await handle_queries(article_page_queries, response_handler=handle_wiki_data_return, tqdm_desc="Fetching " + str(len(article_page_titles)) + " article pages")
    # Parse wiki pages
    article_data = []
    for sublist in tqdm(article_pages, desc="Parsing article page batches", mininterval=0.5):
        parse_results = [parse_article_page(page_content) for key, page_content in sublist.items() if type(sublist) == dict]
        article_data += parse_results

    #### Retrieve and store plaintext wiki pages
    # Retrieve plaintext wiki pages for sentiment analysis
    wiki_plaintext_queries = [get_plaintext_wiki_data_query(title) for title in article_page_titles]
    # Send requests
    wiki_plaintext_pages = await handle_queries(wiki_plaintext_queries, 
                                      response_handler=handle_wiki_data_return, 
                                      tqdm_desc="Fetching " + str(len(article_page_titles)) + " plaintext wiki pages")
    
    # Parse and save plaintext wiki pages
    for sublist in tqdm(wiki_plaintext_pages, desc="Parsing and saving plaintext wiki page batches", mininterval=0.5):
        [save_article_plaintext(page_content) for _,page_content in sublist.items()]

    ## Revisions
    print("getting revisions")
    # Get revisions
    revision_queries = [get_wiki_page_revisions_query(title) for title in article_page_titles]
    # Send requests
    revisions = await handle_queries(revision_queries, response_handler=handle_wiki_page_revisions_return, tqdm_desc="Fetching " + str(len(article_page_titles)) + " revisions")
    # Merge list of dicts into one dict
    revision_dict = dict(ChainMap(*revisions))
    # Extract users
    user_list = [revision["user"] for page_title, page_revisions in revision_dict.items() for revision in page_revisions if "user" in revision]
    user_list.extend([link for page_data in talk_data for link in page_data["user_links"]])
    users_unique = np.unique(user_list)

    ## Users
    user_edit_counts = []
    # split_user_list = list(chunks(users_unique, wiki_api_page_request_limit))
    # # Get revisions
    # user_edit_queries = [get_user_edits_query(users) for users in split_user_list]
    # # Send requests
    # users = await handle_queries(user_edit_queries, response_handler=handle_user_edits_return, tqdm_desc="Fetching " + str(len(users_unique)) + " user edits")
    # # Count edits for users
    # user_edit_counts = {user["editcount"] if "editcount" in user else 0 for user in users}

    # Graph
    print("creating graph")
    page_graph = nx.DiGraph()

    for talk_page_title, wiki_page_title in zip(talk_titles, article_page_titles):
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

    infos = {"titles": talk_titles, "archive_titles": archive_titles, "user_edit_counts": user_edit_counts, "revision_dict": revision_dict}

    return page_graph, infos