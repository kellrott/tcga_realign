
try:
    import wikipedia
except ImportError:
    wikipedia = None

def wikidownload(params):
    with open("text", "w") as handle:
        txt = wikipedia.summary(params['article']).encode('ascii', 'ignore')
        handle.write(txt)
    yield (params['id']+":summary", "text")

STEPS=[wikidownload]
RESUME=True
STORE=False
IMAGE="wikireader"
CLUSTER_MAX=2
