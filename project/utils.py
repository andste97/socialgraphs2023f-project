"""This file contains utility functions that would otherwise clutter
up the python notebook"""

import nltk
from nltk.tokenize import word_tokenize
import wikichatter as wc
import re
import string
wnl = nltk.WordNetLemmatizer()

def flatten(l):
    return [item for sublist in l for item in sublist]

def parse_comments_from_pages(list_file_names):
    """Iterate over list_file_names and extract all the commens from 
    each page using wc. Returns a list of parsed pages."""
    parsed_comments = []
    # extract comments from talk page files
    for filename in list_file_names:
        #print(filename)
        with open(filename, 'r') as file:
            text = file.read()
            try:
                parsed = wc.parse(text)
                parsed_comments.append((str(filename), parsed))
            except:
                print("failed to parse: " + str(filename))
    return parsed_comments

def chunk_list(list, chunk_size: int):
    """Split a list into a list of chunk_sized lists"""
    start = 0
    end = len(list) 
    result = []
    for i in range(start, end, chunk_size): 
        x = i 
        result.append(list[x:x+chunk_size])
    return result

def parse_comment(comment):
    """
    Parse a single comment and return a tuple in this format: (author, comment words)
    """
    author = comment.get("author")
    result = ''
    # result = []
    for text in comment["text_blocks"]:
        # result += tokenize_custom(text)
        result += ' ' +text
    
    return (author, result)

def parse_comment_subcomment(comment):
        """Parse a single comment, and then recursively parse all answers to that comment.
        Yields a generator with tuples in this format: (author, comment words)"""
        yield parse_comment(comment)
        if comment.get("comments"):
            for subcomment in comment.get("comments"):
                yield from parse_comment_subcomment(subcomment)

re_tok = re.compile(f'([{string.punctuation}“”¨«»®´·º½¾¿¡§£₤‘’])')
def tokenize_custom(s):
    if(not s.startswith('=')):
        wikilink_regex = r'\[\[.*?\]\]|\(\)|\{\{.*?\{\}|<.*?>|[0-9]{1,2} [A-Z][a-z]+ [0-9]{4}|\(UTC\)'
        s = re.sub(wikilink_regex, ' ', s)

        s = word_tokenize(s)
        s = ' '.join([wnl.lemmatize(word.lower()) for word in s if word.isalnum()])
        return re_tok.sub(r' \1 ', s).split() # having this line at the end vastly improves classifier results. Not sure why. 
                                            # Just by having this line with the above tokenizer improve score
                                            # from 0.9 to 0.97.
    return []